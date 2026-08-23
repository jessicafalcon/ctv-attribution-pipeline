"""LIVE half of the `report_snapshots` version column (Phase 18a Done-when 4).

Runs under any `make test-int*` target: it never reads the profile's numbers, only
the table's SCHEMA behaviour, and it works on probe tables of its own so no pinned
snapshot row is touched.

Stack fact this exists for (ARCHITECTURE §8): ClickHouse has no `alter table ...
modify engine`, so adding a ReplacingMergeTree version to an existing table is a
create → backfill → `exchange tables` → drop rebuild. `report_snapshots` is the
one serving table `make replay-serving` does NOT rebuild from the lake
(`orchestration/replay.py` SERVING_TABLES), so the migration has to preserve every
row rather than recreate the table.
"""

from types import SimpleNamespace

import pytest
from clickhouse_connect.driver.client import Client

from clickhouse.apply import MigrationError, migrate_report_snapshots
from clickhouse.client import connect
from reconcile import rollup

PROBE = "_report_snapshots_probe"

# Two rows on ONE sort key (reported_at, campaign_id, period), differing in
# snapshot_version and in a metric.
_EARLIER = "2026-01-01 00:00:00.000"
_LATER = "2026-01-02 00:00:00.000"


@pytest.fixture
def client() -> Client:
    return connect()


@pytest.fixture(autouse=True)
def _drop_probes(client: Client):
    for table in (PROBE, f"{PROBE}_v2", "report_snapshots_v2"):
        client.command(f"drop table if exists {table}")
    yield
    for table in (PROBE, f"{PROBE}_v2"):
        client.command(f"drop table if exists {table}")


def _probe_like_the_real_table(client: Client) -> None:
    """Structure AND engine copied from the live `report_snapshots` — the probe is
    the real schema, so what it proves the real table does too."""
    client.command(f"create table {PROBE} as report_snapshots")


def _insert(client: Client, version: str, revenue: float) -> None:
    client.command(
        f"insert into {PROBE} (reported_at, campaign_id, period, spend, revenue, "
        "conversions, purchases, exposures, snapshot_version) values "
        f"('{_EARLIER}', 'camp-probe', 'all', 1.0, {revenue}, 1, 1, 1, '{version}')"
    )


def test_forced_optimize_keeps_the_later_snapshot_version(client: Client) -> None:
    """The rule the version column declares: on an identical sort key, the higher
    `snapshot_version` wins a merge — not whichever part the merge happened to
    visit last.

    NOT reachable through the pipeline: a re-run is byte-identical (the
    determinism pin), so a real twin pair has EQUAL versions and equal content.
    This shapes the case the pin exists to exclude, so the rule is proven rather
    than assumed. Do not read it as a live failure mode.
    """
    _probe_like_the_real_table(client)
    _insert(client, _EARLIER, 10.0)
    _insert(client, _LATER, 99.0)
    client.command(f"optimize table {PROBE} final")

    rows = client.query(f"select snapshot_version, revenue from {PROBE}").result_rows
    assert len(rows) == 1, f"the twins did not collapse: {rows}"
    assert rows[0][1] == 99.0, f"the merge kept the older version: {rows}"


def test_the_pre_and_post_pair_is_never_collapsed(client: Client) -> None:
    # The other half of the sort-key decision: different reported_at = different
    # key, so `make restate` still sees both passes through FINAL.
    _probe_like_the_real_table(client)
    _insert(client, _EARLIER, 10.0)
    client.command(
        f"insert into {PROBE} (reported_at, campaign_id, period, spend, revenue, "
        "conversions, purchases, exposures, snapshot_version) values "
        f"('{_LATER}', 'camp-probe', 'all', 1.0, 99.0, 1, 1, 1, '{_LATER}')"
    )
    client.command(f"optimize table {PROBE} final")
    assert client.query(f"select count() from {PROBE} final").result_rows[0][0] == 2


def _unversioned_probe_with_rows(client: Client) -> None:
    client.command(
        f"create table {PROBE} "
        "(reported_at DateTime64(3, 'UTC'), campaign_id String, period String, "
        "spend Float64, revenue Float64) "
        "engine = ReplacingMergeTree order by (reported_at, campaign_id, period)"
    )
    client.command(
        f"insert into {PROBE} values "
        f"('{_EARLIER}', 'camp-a', 'all', 1.0, 10.0), "
        f"('{_LATER}', 'camp-b', 'all', 2.0, 20.0)"
    )


def test_migration_preserves_every_row_and_is_a_no_op_on_the_second_run(
    client: Client,
) -> None:
    _unversioned_probe_with_rows(client)
    before = client.query(
        f"select reported_at, campaign_id, period, spend, revenue from {PROBE} "
        "order by reported_at"
    ).result_rows

    assert migrate_report_snapshots(client, PROBE) is True
    engine = client.query(
        "select engine_full from system.tables "
        f"where database = currentDatabase() and name = '{PROBE}'"
    ).result_rows[0][0]
    assert engine.startswith("ReplacingMergeTree(snapshot_version)")
    assert "ORDER BY (reported_at, campaign_id, period)" in engine

    after = client.query(
        f"select reported_at, campaign_id, period, spend, revenue from {PROBE} "
        "order by reported_at"
    ).result_rows
    assert after == before
    # Backfill: legacy rows carry reported_at as their version (almost inert —
    # reported_at is in the sort key, so a legacy row only competes with a twin
    # of its own pass).
    assert (
        client.query(
            f"select countIf(snapshot_version != reported_at) from {PROBE}"
        ).result_rows[0][0]
        == 0
    )

    assert migrate_report_snapshots(client, PROBE) is False
    assert client.query(f"select count() from {PROBE}").result_rows[0][0] == len(before)


def test_a_crash_before_the_exchange_leaves_the_rows_intact(client: Client) -> None:
    _unversioned_probe_with_rows(client)
    before = client.query(f"select count() from {PROBE}").result_rows[0][0]

    class _CrashOnExchange:
        """The real client until the EXCHANGE, then a hard failure — the window
        where the rows exist in the original only."""

        def __init__(self, inner: Client):
            self._inner = inner

        def command(self, sql: str, parameters: dict | None = None):
            if sql.strip().lower().startswith("exchange"):
                raise RuntimeError("crash before exchange")
            return self._inner.command(sql, parameters=parameters)

        def query(self, sql: str, parameters: dict | None = None):
            return self._inner.query(sql, parameters=parameters)

    with pytest.raises(RuntimeError, match="crash before exchange"):
        migrate_report_snapshots(_CrashOnExchange(client), PROBE)

    # The original is untouched — still unversioned, still holding every row.
    engine = client.query(
        "select engine_full from system.tables "
        f"where database = currentDatabase() and name = '{PROBE}'"
    ).result_rows[0][0]
    assert "ReplacingMergeTree(" not in engine
    assert client.query(f"select count() from {PROBE}").result_rows[0][0] == before

    # …and the retry (which drops the leftover scratch table first) completes.
    assert migrate_report_snapshots(client, PROBE) is True
    assert client.query(f"select count() from {PROBE}").result_rows[0][0] == before


def test_a_short_copy_is_refused_rather_than_exchanged(client: Client) -> None:
    _unversioned_probe_with_rows(client)

    class _LosesARow:
        """Reports a copy one row short — the guard, not a real failure mode."""

        def __init__(self, inner: Client):
            self._inner = inner
            self._counts = 0

        def command(self, sql: str, parameters: dict | None = None):
            return self._inner.command(sql, parameters=parameters)

        def query(self, sql: str, parameters: dict | None = None):
            result = self._inner.query(sql, parameters=parameters)
            if "count()" in sql:
                self._counts += 1
                if self._counts == 2:  # the scratch table's count
                    return SimpleNamespace(
                        result_rows=[(result.result_rows[0][0] - 1,)]
                    )
            return result

    with pytest.raises(MigrationError, match="refusing to exchange"):
        migrate_report_snapshots(_LosesARow(client), PROBE)
    engine = client.query(
        "select engine_full from system.tables "
        f"where database = currentDatabase() and name = '{PROBE}'"
    ).result_rows[0][0]
    assert "ReplacingMergeTree(" not in engine


def test_a_pass_that_credits_nothing_never_versions_below_a_prior_pass(
    client: Client,
) -> None:
    """The no-credit fallback, against real data. Forced by an impossible path
    filter (`path = 'no_such_path'`), so the CREDITED set is empty while every
    other input is the live one — the shape a profile with zero attributions
    would produce, without mutating a table.

    The fallback must be max(processed_at) over ALL current rows, not
    `reported_at`: reported_at is max(ingest_time) + offset, while a reconciled
    row's processed_at is max(ingest_time) + RECONCILE_DELTA_MS, so the pre pass's
    reported_at sits BELOW a version already written (measured, DECISIONS 18a).
    """
    _probe_like_the_real_table(client)
    sql = rollup._WRITE_REPORT_SNAPSHOT.replace(
        "/*path_filter*/", "and a.path = 'no_such_path'"
    )
    sql = sql.replace("insert into report_snapshots", f"insert into {PROBE}")
    client.command(sql, parameters={"offset_ms": 0, "period": rollup.PERIOD})

    all_rows_max = client.query(
        "select max(processed_at) from attributed_conversions final"
    ).result_rows[0][0]
    versions = client.query(
        f"select distinct snapshot_version, reported_at from {PROBE}"
    ).result_rows
    assert versions, "the no-credit pass wrote no rows"
    for version, reported_at in versions:
        assert version == all_rows_max
        # The point of the ruling: the fallback would have gone BACKWARDS here.
        assert version >= reported_at

    prior = client.query(
        "select max(snapshot_version) from report_snapshots"
    ).result_rows[0][0]
    assert all_rows_max >= prior, "a no-credit pass versioned below a prior pass"
