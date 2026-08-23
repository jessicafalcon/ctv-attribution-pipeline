"""The ClickHouse storage scrape (Phase 18a), offline.

The live halves — that `metrics_ro` cannot read a pipeline row, and that a real
capture is reproducible — are `tests/integration/test_metrics_ro.py` and the
two-capture diff in the phase gate.
"""

from prometheus_client.exposition import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from observability import ch_scrape


class _Client:
    """Replays a scripted sequence of (counts, backlog) samples."""

    def __init__(self, samples: list[tuple[dict, float]]):
        self._samples = list(samples)
        self.queries = 0

    def _current(self):
        return self._samples[0] if len(self._samples) == 1 else self._samples.pop(0)

    def query(self, sql: str, parameters: dict | None = None):
        self.queries += 1

        class _R:
            pass

        r = _R()
        if "system.merges" in sql:
            r.result_rows = [(self._pending_backlog,)]
        else:
            counts, self._pending_backlog = self._current()
            r.result_rows = [(t, p, u) for t, (p, u) in counts.items()]
        return r


def test_every_metric_carries_the_stage_prefix() -> None:
    # CLAUDE.md convention: each stage's metrics are prefixed. This stage is
    # `clickhouse_` (the storage view), not `lake_` (the load) — a different thing
    # is being measured, from a different principal.
    names = {
        m._name
        for m in (
            ch_scrape.ACTIVE_PARTS,
            ch_scrape.UNMERGED_PARTS,
            ch_scrape.MERGE_BACKLOG,
        )
    }
    assert all(n.startswith("clickhouse_") for n in names), names


def test_merge_backlog_help_states_the_caveat() -> None:
    # The gauge reads a wall-clock elapsed of merges IN FLIGHT, so a settled capture
    # reads 0. If that caveat ever leaves the HELP text, someone will read a 0 as
    # "no merge work pending" — which is what clickhouse_unmerged_parts answers.
    help_text = ch_scrape.MERGE_BACKLOG._documentation
    assert "0 when none is running" in help_text
    assert "clickhouse_unmerged_parts" in help_text


def test_settle_returns_once_two_samples_agree_with_no_merge_running() -> None:
    moving = {"a": (5, 4)}
    settled = {"a": (3, 2)}
    client = _Client([(moving, 0.0), (settled, 0.0), (settled, 0.0)])
    counts, backlog, waited = ch_scrape.settle(client, "default")
    assert counts == settled and backlog == 0.0
    assert waited >= 0


def test_settle_does_not_accept_a_sample_taken_mid_merge() -> None:
    # Equal counts are not enough: a merge in flight is about to change them.
    stable = {"a": (3, 2)}
    client = _Client([(stable, 1.5), (stable, 1.5), (stable, 0.0), (stable, 0.0)])
    counts, backlog, _ = ch_scrape.settle(client, "default")
    assert (counts, backlog) == (stable, 0.0)


def test_settle_gives_up_rather_than_hanging_a_pipeline_run(monkeypatch) -> None:
    # A scrape is the LAST thing `make run` does; it must never be able to hold the
    # run open. On timeout it reports the last sample — an unsettled capture shows up
    # as a difference from the next one, which is what the gate's two-capture diff
    # and the committed fixtures catch.
    monkeypatch.setattr(ch_scrape, "_SETTLE_TIMEOUT_S", 0.2)
    monkeypatch.setattr(ch_scrape, "_SETTLE_INTERVAL_S", 0.01)
    never = [({"a": (n, n)}, 0.0) for n in range(1, 500)]
    counts, _, waited = ch_scrape.settle(_Client(never), "default")
    assert counts and waited < 5


def test_scrape_reports_a_zero_for_a_table_that_has_no_parts() -> None:
    # A missing series and a zero series are different claims: an alert that never
    # sees the series cannot fire on it. Every named table is always exported.
    client = _Client([({"campaign_hourly": (2, 1)}, 0.0)] * 3)
    values = ch_scrape.scrape(client)
    for table in ch_scrape.TABLES:
        assert f"parts:{table}" in values
    assert values["parts:campaign_hourly"] == 2
    assert values["parts:attributed_conversions"] == 0


def test_the_written_registry_carries_no_wall_clock() -> None:
    # How long settling took is printed, never exported: the promtool fixtures are
    # baked from these files, and a timing value would make them irreproducible.
    client = _Client([({"campaign_hourly": (2, 1)}, 0.0)] * 3)
    values = ch_scrape.scrape(client)
    assert "settle_seconds" in values  # printed by main()
    exported = generate_latest(ch_scrape._registry()).decode()
    names = {
        sample.name
        for family in text_string_to_metric_families(exported)
        for sample in family.samples
    }
    assert names == {
        "clickhouse_active_parts",
        "clickhouse_unmerged_parts",
        "clickhouse_merge_backlog_seconds",
    }


def test_the_scraped_tables_are_named_not_discovered() -> None:
    # A stray table (a bench probe, a migration scratch) must not be able to appear
    # in a captured fixture and move a threshold.
    assert "rollup_refresh_marker" not in ch_scrape.TABLES
    assert "system.parts" in ch_scrape._PARTS
    assert "{tables:Array(String)}" in ch_scrape._PARTS
