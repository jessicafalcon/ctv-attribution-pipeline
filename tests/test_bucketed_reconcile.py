"""Phase-17 D2 gate (offline): the bucket-aligned lake reconcile == the single
in-memory pass, byte for byte — on long_delay (state-miss channel) and
shared_ip_spike (ambiguous channel, the one most-recent-exposure tiebreak).

Offline: generate → resolve → dedup → hot engine (run_attribution) → land the hot
rows + exposures in a tmp lake → `recover_day` per candidate day (explode →
bucket-local reads → leaf → cross-bucket pick_household) vs `reconcile(
expand_candidates(...))` over the full in-memory exposure dict. Also pins that a
bucket read touches only its bucket's files (the partition-local join claim).
"""

from collections import defaultdict
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from lake.iceberg_catalog import bucket_of, ensure_exposures, metadata_path
from lake.land_attributed import land_attributed
from lake.land_exposures import land
from producer.config import load_profile
from producer.generate import generate
from producer.models import Exposure
from reconcile.reconcile import (
    LONG_WINDOW,
    candidate_days,
    expand_candidates,
    lake_candidates,
    reconcile,
    reconciled_at_for,
    recover_day,
)
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream
from streaming.attribute import ALLOWED_LATENESS, dedup_streams
from streaming.dataflow import run_attribution


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))


def _hot_run(name: str):
    p = load_profile(name)
    s = generate(p, p.seed)
    graph = GraphIndex.from_households(s.graph.households)
    exps, res, _ = dedup_streams(s.exposures, resolve_stream(s.conversions, graph))
    return exps, run_attribution(exps, res, ALLOWED_LATENESS)


@pytest.mark.parametrize("name", ["long_delay", "shared_ip_spike"])
def test_bucketed_lake_pass_equals_the_single_in_memory_pass(name: str) -> None:
    exps, hot = _hot_run(name)
    land(exps)
    land_attributed(hot)
    at = reconciled_at_for(max(e.ingest_time for e in exps))

    # the oracle: Phase-16's single pass over everything in memory
    by_hh: dict[str, list[Exposure]] = defaultdict(list)
    for e in exps:
        by_hh[e.household_id].append(e)
    candidates = [r for r in hot if not r.attributed]
    # the lake reads back naive UTC; the in-memory rows are aware UTC — compare
    # on the canonical JSON, which renders both as the same instant
    single = reconcile(expand_candidates(candidates), by_hh, LONG_WINDOW, at)

    assert sorted(lake_candidates(), key=lambda r: r.conversion_id) == sorted(
        (r.model_copy(update=_naive(r)) for r in candidates),
        key=lambda r: r.conversion_id,
    )
    bucketed = [r for d in candidate_days() for r in recover_day(d, at)]
    assert bucketed, f"{name} must recover something"
    assert len(bucketed) == len(single)
    assert sorted(_canon(r) for r in bucketed) == sorted(_canon(r) for r in single)


def _canon(r) -> str:
    return r.model_copy(update=_naive(r)).model_dump_json()


def test_accumulated_lake_reconciles_identically_to_a_fresh_one(
    tmp_path, monkeypatch
) -> None:
    # Done-when 6 as its own property (not a side effect of test ordering): the
    # same hot run landed 3× vs once → the bucketed pass recovers identical rows.
    exps, hot = _hot_run("long_delay")
    at = reconciled_at_for(max(e.ingest_time for e in exps))
    outputs = []
    for name, appends in (("fresh", 1), ("accumulated", 3)):
        monkeypatch.setenv("LAKE_ROOT", str(tmp_path / name))
        for _ in range(appends):
            land(exps)
            land_attributed(hot)
        outputs.append(
            [r.model_dump_json() for d in candidate_days() for r in recover_day(d, at)]
        )
    assert outputs[0] and outputs[0] == outputs[1]


def test_recover_day_issues_one_bucket_homogeneous_read_per_bucket(monkeypatch) -> None:
    # The product path is bucket-GROUPED, not one big read: every exposure read
    # covers households of exactly one bucket, one read per bucket touched. A
    # refactor to a single scan would keep every other test green — this one not.
    import reconcile.reconcile as rc

    exps, hot = _hot_run("shared_ip_spike")
    land(exps)
    land_attributed(hot)
    at = reconciled_at_for(max(e.ingest_time for e in exps))
    real = rc.read_exposures_by_household
    calls: list[set[str]] = []

    def spy(household_ids, **kw):
        calls.append(set(household_ids))
        return real(household_ids, **kw)

    monkeypatch.setattr(rc, "read_exposures_by_household", spy)
    for day in candidate_days():
        calls.clear()
        recovered = recover_day(day, at)
        buckets = [{bucket_of(h) for h in hhs} for hhs in calls]
        assert all(len(b) == 1 for b in buckets), buckets  # bucket-homogeneous
        assert len(calls) == len({next(iter(b)) for b in buckets})  # one per bucket
        expected = {
            bucket_of(h)
            for c in lake_candidates(day)
            for h in (c.candidate_households or [c.household_id])
        }
        assert {next(iter(b)) for b in buckets} == expected
    assert recovered  # the pass ran; its content is pinned by the gate above


def _naive(r):
    """The lake round-trip shape: naive UTC to the ms (DECISIONS Phase 12)."""
    return {
        k: getattr(r, k).astimezone(UTC).replace(tzinfo=None)
        for k in ("event_time", "ingest_time", "processed_at")
        if getattr(r, k).tzinfo is not None
    }


def test_a_bucket_read_touches_only_its_buckets_files() -> None:
    # 32 households spread over the 8 buckets on one day → 8 files; a read for
    # the households of ONE bucket must scan exactly one of them.
    t = datetime(2026, 8, 1, 12, tzinfo=UTC)
    exps = [
        Exposure(
            exposure_id=f"e-{i}",
            event_time=t,
            ingest_time=t + timedelta(minutes=1),
            campaign_id="c",
            household_id=f"h-{i:04d}",
            ip="1",
            app_id="a",
            program_genre="g",
            spend=1.0,
        )
        for i in range(32)
    ]
    land(exps)
    table = ensure_exposures()
    files = table.inspect.files().to_pylist()
    assert len({f["partition"]["household_bucket"] for f in files}) == 8
    target = 3
    hhs = sorted(e.household_id for e in exps if bucket_of(e.household_id) == target)
    assert hhs
    con = duckdb.connect()
    con.execute("load iceberg")
    plan = con.execute(
        "explain analyze select count(*) from iceberg_scan(?) "
        "where household_id = any(?) and event_time >= ? and event_time < ?",
        [metadata_path(table), hhs, t - timedelta(days=90), t + timedelta(days=1)],
    ).fetchall()[0][1]
    (line,) = [ln for ln in plan.splitlines() if "Total Files Read" in ln]
    assert "Total Files Read: 1" in line, line
