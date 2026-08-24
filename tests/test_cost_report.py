"""Offline pins for the query-cost report (Phase 18b, Done-when 2). The measured
cost itself is LIVE (tests/integration/test_cost_report.py); these pin the pure
derivations (the mutation sentinels) and the quarantine (Invariant 3)."""

from pathlib import Path

import pytest

from queries import cost_report
from queries.cost_report import (
    _TAG_PREFIX,
    QUERIES,
    cpu_seconds,
    to_dollars,
    usd_per_cpu_second,
)


def test_cpu_seconds_is_the_profileevents_value() -> None:
    assert cpu_seconds(3_157) == pytest.approx(0.003157)
    assert cpu_seconds(0) == 0.0
    assert cpu_seconds(2_500_000) == pytest.approx(2.5)


def test_dollars_come_from_the_config_rate_not_a_hardcoded_sql_literal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert to_dollars(2.0, rate=1.5) == pytest.approx(3.0)  # explicit rate
    monkeypatch.setenv("COST_USD_PER_CPU_SECOND", "0.01")
    assert usd_per_cpu_second() == 0.01
    assert to_dollars(3.0) == pytest.approx(0.03)  # from config, no rate arg
    # The rate never lives in SQL — usd is a Python-computed column, then inserted.
    assert "usd" not in cost_report._READ_COST.lower()
    assert "usd" in cost_report._COST_COLS


def test_every_measured_query_is_tagged() -> None:
    tags = {_TAG_PREFIX + t for t in QUERIES}
    assert len(tags) == len(QUERIES)  # one distinct tag per query (Invariant 4)
    assert all(t.startswith(_TAG_PREFIX) for t in tags)
    for path in QUERIES.values():
        assert path.exists()


def test_no_pipeline_path_reads_query_cost_daily() -> None:
    """Invariant 3: only cost_report writes query_cost_daily, and NO serving /
    report / reconcile / lake path reads it — the quarantine that keeps its
    non-determinism out of every pipeline answer."""
    root = Path(__file__).parent.parent
    hits = []
    for pkg in ("queries", "reconcile", "orchestration", "lake"):
        for py in (root / pkg).rglob("*.py"):
            if py.name == "cost_report.py":
                continue
            if "query_cost_daily" in py.read_text():
                hits.append(str(py.relative_to(root)))
    assert hits == [], f"query_cost_daily referenced outside cost_report.py: {hits}"
