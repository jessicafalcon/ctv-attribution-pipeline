"""The loader↔rollup dirty-set contract (Phase 18a), offline.

Every test here exists because the review gate found the phase shipped with none:
the incremental refresh could be broken (`!=` → `>`, a dropped key filter, a skipped
stamp) and `make test`, `make test-int` and `make test-int-long-delay` all stayed
green. The live half — that the dirty set equals the keys a reconcile pass actually
changed — is `tests/integration/test_rollup_dirty.py`.

The contract, in one line: a key is dirty when the version the loader recorded for it
differs from the version the rollup was last computed against, and the refresh stamps
exactly the versions it computed against, AFTER it writes the rollup rows.
"""

import re

from lake import load_serving
from reconcile import rollup


class _Client:
    """Records statements and replays scripted query results in order."""

    def __init__(self, dirty: list[tuple] | None = None):
        self.commands: list[tuple[str, dict]] = []
        self.inserts: list[tuple] = []
        self._dirty = dirty if dirty is not None else []
        self.dirty_reads = 0

    def command(self, sql: str, parameters: dict | None = None) -> None:
        self.commands.append((" ".join(sql.split()), parameters or {}))

    def query(self, sql: str, parameters: dict | None = None):
        class _R:
            pass

        r = _R()
        if "rollup_dirty" in sql:
            self.dirty_reads += 1
            r.result_rows = list(self._dirty)
        else:
            r.result_rows = []
        return r


# (campaign, hour epoch-seconds, version epoch-millis)
_KEY_A = ("camp-00", 1_760_000_000, 1_760_000_005_000)
_KEY_B = ("camp-01", 1_760_003_600, 1_760_003_605_000)


def test_a_key_is_dirty_when_its_own_version_differs_not_when_it_beats_a_global_max():
    """The bug this phase shipped and the gate caught: the first cut compared every
    key's version to ONE scalar watermark (the max over the whole dirty table), so a
    key whose own timestamps lagged the highest key's sat permanently below it and was
    never refreshed again — 321 of 340 keys on long_delay, reproduced as a stale
    served rollup. The comparison must be per key, against that key's own last
    refreshed version."""
    sql = " ".join(rollup._DIRTY_KEYS.split())
    assert "left join rollup_refreshed" in sql
    assert "on d.campaign_id = r.campaign_id and d.hour = r.hour" in sql
    assert "where d.version != r.version" in sql
    # No global aggregate anywhere in the selection: max()/min() over the dirty table
    # is exactly the shape that produced the bug.
    assert not re.search(r"\b(max|min)\s*\(", sql), sql


def test_a_version_that_moves_DOWN_still_marks_the_key_dirty() -> None:
    # `!=`, not `>`: re-seeding a profile after `make lake-reset` can leave the same
    # key with an EARLIER max ingest_time, and "the rollup was computed against a
    # different version of this key" is the honest condition. A `>` comparison would
    # serve the previous seed's rollup forever.
    assert "!=" in rollup._DIRTY_KEYS
    assert ">" not in rollup._DIRTY_KEYS.replace("!=", "")


def test_keys_cross_the_process_boundary_as_integers_never_as_datetimes() -> None:
    # The driver renders a DateTime in the CALLER's local timezone, so re-binding one
    # compares a shifted instant — measured live during the gate: 5 keys of 19 matched
    # on a MDT laptop. Hours travel as epoch seconds computed by ClickHouse.
    assert "toUnixTimestamp(d.hour) as hour_s" in rollup._DIRTY_KEYS
    assert "toUnixTimestamp64Milli(d.version) as version_ms" in rollup._DIRTY_KEYS
    assert "{keys:Array(Tuple(String, UInt32))}" in rollup._DIRTY_FILTER
    assert "toDateTime(t.2, 'UTC')" in rollup._STAMP_REFRESHED
    assert "fromUnixTimestamp64Milli(t.3, 'UTC')" in rollup._STAMP_REFRESHED


def test_refresh_selects_only_dirty_keys_and_binds_them() -> None:
    client = _Client(dirty=[_KEY_A, _KEY_B])
    assert rollup.refresh_campaign_hourly(client, offset_ms=1000) == 2
    refresh, stamp = client.commands
    assert refresh[0].startswith("insert into campaign_hourly")
    assert refresh[1]["keys"] == [
        (_KEY_A[0], _KEY_A[1]),
        (_KEY_B[0], _KEY_B[1]),
    ]
    assert refresh[1]["offset_ms"] == 1000
    # …and the stamp carries the VERSIONS, not just the keys.
    assert stamp[0].startswith("insert into rollup_refreshed")
    assert stamp[1]["keys"] == [_KEY_A, _KEY_B]


def test_the_key_filter_is_applied_to_both_exposure_reads() -> None:
    # The rollup reads exposures_landed twice (the spend/exposure side and the credit
    # join's exposure side). Dropping either substitution leaves the refresh reading
    # every key on that side and writing wrong aggregates for the dirty ones — a
    # mutation that survived `make test` before this pin existed.
    sql = rollup.refresh_sql(full=False)
    assert "/*dirty_exposures*/" not in sql
    assert sql.count(rollup._DIRTY_FILTER) == 2
    assert rollup.refresh_sql(full=True).count("{keys:") == 0


def test_nothing_dirty_writes_nothing() -> None:
    client = _Client(dirty=[])
    assert rollup.refresh_campaign_hourly(client, offset_ms=0) == 0
    assert client.commands == []


def test_crash_before_the_stamp_re_refreshes_the_same_keys() -> None:
    """Done-when 1's idempotence claim. The stamp is written AFTER the rollup rows, so
    a crash in between leaves the keys dirty and the next pass recomputes them —
    never skips them, which is the one failure the full-refresh oracle cannot see."""
    client = _Client(dirty=[_KEY_A])
    refresh_index = [i for i, (sql, _) in enumerate(client.commands)]
    assert refresh_index == []

    class _CrashAfterRefresh(_Client):
        def command(self, sql: str, parameters: dict | None = None) -> None:
            if sql.strip().startswith("insert into rollup_refreshed"):
                raise RuntimeError("crash before the stamp")
            super().command(sql, parameters)

    crashing = _CrashAfterRefresh(dirty=[_KEY_A])
    try:
        rollup.refresh_campaign_hourly(crashing, offset_ms=0)
    except RuntimeError:
        pass
    assert [sql.split()[2] for sql, _ in crashing.commands] == ["campaign_hourly"]
    # The dirty set is untouched by a refresh (no deletes, no mutations), so the next
    # pass sees the same key and recomputes it.
    retry = _Client(dirty=[_KEY_A])
    assert rollup.refresh_campaign_hourly(retry, offset_ms=0) == 1


def test_full_is_the_oracle_not_a_bypass() -> None:
    # `full=True` must recompute EVERY key and still stamp them, so a full rebuild
    # converges to "nothing dirty" exactly like an incremental one. It must never be
    # a way to skip the stamp (which would leave every key dirty forever) nor to skip
    # the equality the bench asserts.
    client = _Client(dirty=[_KEY_A, _KEY_B])
    rollup.refresh_campaign_hourly(client, offset_ms=0, full=True)
    statements = [sql for sql, _ in client.commands]
    assert statements[0].startswith("insert into campaign_hourly")
    assert "{keys:" not in statements[0]
    assert statements[1].startswith("insert into rollup_refreshed")
    assert "from rollup_dirty final" in statements[1]


def test_the_dirty_set_is_never_deleted_or_mutated() -> None:
    # A correction that silently skips a key is invisible to the full-refresh oracle,
    # so the bookkeeping is append-only: the refresh may only INSERT.
    for sql in (rollup._STAMP_REFRESHED, rollup._STAMP_ALL_REFRESHED):
        assert sql.strip().startswith("insert into")
    module_sql = [
        v for k, v in vars(rollup).items() if isinstance(v, str) and k.isupper()
    ]
    for sql in module_sql:
        lowered = sql.lower()
        assert "delete" not in lowered and "truncate" not in lowered
        assert "alter table" not in lowered


def test_the_loader_records_both_sides_of_a_days_keys() -> None:
    # The rollup buckets by the CREDITED EXPOSURE's hour, so a day's dirty keys come
    # from the day's exposures AND from the exposures its conversions credit — which
    # can sit up to the long window earlier than the conversion's own day.
    exposures = " ".join(load_serving._DIRTY_FROM_EXPOSURES.split())
    attributed = " ".join(load_serving._DIRTY_FROM_ATTRIBUTED.split())
    assert (
        "insert into rollup_dirty" in exposures
        and "insert into rollup_dirty" in attributed
    )
    assert "max(ingest_time) as version" in exposures
    assert "max(a.processed_at) as version" in attributed
    # the CONVERSION side keys on the exposure's hour, not the conversion's
    assert "toStartOfHour(e.event_time) as hour" in attributed
    assert "where a.attributed = 1" in attributed


def test_a_days_load_records_credits_in_both_directions() -> None:
    """Order independence. Loading a CONVERSION day whose exposure day is not in yet
    records nothing on the conversion side (the join finds no exposure), so the
    exposure day's load must pick those credits up — otherwise `rollup_dirty` FINAL
    depends on the order Dagster materialized the partitions in, which nothing pins.
    Pinned live by `tests/integration/test_rollup_dirty.py`."""
    credits = " ".join(load_serving._DIRTY_FROM_EXPOSURE_CREDITS.split())
    assert "insert into rollup_dirty" in credits
    assert "max(a.processed_at) as version" in credits
    # the DAY filter is on the EXPOSURE side here (the mirror of _DIRTY_FROM_ATTRIBUTED)
    day_filter = "from exposures_landed final where toDate(event_time) = {day:Date}"
    assert day_filter in credits


def test_the_loaders_versions_are_data_derived_not_wall_clock() -> None:
    for sql in (
        load_serving._DIRTY_FROM_EXPOSURES,
        load_serving._DIRTY_FROM_ATTRIBUTED,
        load_serving._DIRTY_FROM_EXPOSURE_CREDITS,
    ):
        assert not re.search(r"\bnow\(|\bnow64\(|\btoday\(", sql)
        assert "{day:Date}" in sql


def test_a_reload_of_the_same_day_records_the_same_versions() -> None:
    # Both statements aggregate over the rows now IN ClickHouse for that day, not over
    # whatever this process happened to insert — so a re-load records identical keys
    # and identical versions, and the key drops out of the dirty set instead of
    # bouncing back in.
    for sql in (
        load_serving._DIRTY_FROM_EXPOSURES,
        load_serving._DIRTY_FROM_ATTRIBUTED,
    ):
        assert "final" in sql
        assert "group by" in sql
