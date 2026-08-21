"""Phase-17 LIVE lake-of-record proof on a CLEAN long_delay-only stack
(`make test-int-lakehouse`: make down && lake-reset && up && seed long_delay &&
run-hot long_delay). NOT part of the shared `make test-int` (tiny-only) — same
shared-conversion_id isolation as the other isolated live proofs (DECISIONS
Phase 5).

The stack's ClickHouse was populated by `make run-hot` = engine → lake → Dagster
load. This module re-runs the engine IN MEMORY (the direct-write oracle: the rows
the pre-Phase-17 sink would have inserted) and lands it into a TMP lake of its
own (module fixture — it never touches the developer's `data/lake/<profile>`, so
it is safe to run standalone), then proves the spec's Done-when:
- #1/#2 arrow flipped: lake-loaded serving rows == oracle rows (row content,
  sorted, 6dp);
- #6 accumulated lake: landing the same run 3 more times into the lake and
  reloading leaves the serving rows byte-identical (dedup-on-read + RMT);
- #3 the bucket-aligned lake pass == the same candidates matched against
  exposures read from ClickHouse (`exposures_landed FINAL` — the Phase-12
  source-equivalence proof, kept, now with the lake pass as the product path);
- Phase 12 #3 (kept): the Dagster-orchestrated pass writes the same reconciled
  rows a single pass would — now via lake append + reload.
"""

import os
from collections import defaultdict
from datetime import datetime

import pytest

from clickhouse.client import connect
from lake.load_serving import ATTRIBUTED_COLS, EXPOSURE_COLS
from orchestration.load import materialize_load
from orchestration.run import main as dagster_main
from producer.models import Exposure
from reconcile.reconcile import (
    LONG_WINDOW,
    _max_ingest,
    candidate_days,
    expand_candidates,
    lake_candidates,
    reconcile,
    reconciled_at_for,
    recover_day,
)
from streaming.dataflow import EngineRun, land_run, run_engine
from tests.oracle import (
    _ATTRIBUTED_COLS,
    _EXPOSURE_COLS,
    attributed_values,
    exposure_values,
)

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
_EXP_ORDER = "campaign_id, event_time, exposure_id"


def _norm(v):
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if isinstance(v, list | tuple):
        return [_norm(x) for x in v]
    return v


def _serving(client, table: str, cols: list[str], order: str) -> list[list]:
    rows = client.query(
        f"select {', '.join(cols)} from {table} final order by {order}"
    ).result_rows
    return [[_norm(v) for v in r] for r in rows]


def _att(client) -> list[list]:
    return _serving(client, "attributed_conversions", ATTRIBUTED_COLS, "conversion_id")


def _exp(client) -> list[list]:
    return _serving(client, "exposures_landed", EXPOSURE_COLS, _EXP_ORDER)


def _oracle(run: EngineRun) -> tuple[list[list], list[list]]:
    att = sorted([_norm(v) for v in attributed_values(r)] for r in run.rows)
    exp = sorted(
        ([_norm(v) for v in exposure_values(e)] for e in run.exposures),
        key=lambda r: (r[3], r[1], r[0]),
    )
    return att, exp


@pytest.fixture(scope="module")
def oracle_run(tmp_path_factory) -> EngineRun:
    """The in-memory engine run, landed ONCE into a tmp lake that this module
    owns (LAKE_ROOT overridden for the whole module). Writes nothing to the
    developer's lake; ClickHouse is the stack's, loaded by `make run-hot`."""
    mp = pytest.MonkeyPatch()
    mp.setenv("LAKE_ROOT", str(tmp_path_factory.mktemp("lake")))
    run = run_engine(BROKER)
    land_run(run)
    yield run
    mp.undo()


def test_lake_loaded_serving_rows_equal_the_direct_write_oracle(oracle_run) -> None:
    assert _ATTRIBUTED_COLS == ATTRIBUTED_COLS and _EXPOSURE_COLS == EXPOSURE_COLS
    client = connect()
    att, exp = _oracle(oracle_run)
    assert att, "long_delay must attribute rows"
    assert (
        _serving(client, "attributed_conversions", ATTRIBUTED_COLS, "conversion_id")
        == att
    )
    assert _serving(client, "exposures_landed", EXPOSURE_COLS, _EXP_ORDER) == exp


def test_accumulated_lake_reloads_byte_identically(oracle_run) -> None:
    # Land the SAME run three more times (an append-only log accumulates), reload
    # every touched day, and the serving rows must not move (Done-when #6).
    client = connect()
    before_att = _serving(
        client, "attributed_conversions", ATTRIBUTED_COLS, "conversion_id"
    )
    before_exp = _serving(client, "exposures_landed", EXPOSURE_COLS, _EXP_ORDER)
    days: set[str] = set()
    for _ in range(3):
        days |= land_run(oracle_run)
    materialize_load(days)
    after_att = _serving(
        client, "attributed_conversions", ATTRIBUTED_COLS, "conversion_id"
    )
    after_exp = _serving(client, "exposures_landed", EXPOSURE_COLS, _EXP_ORDER)
    assert after_att == before_att
    assert after_exp == before_exp


def _clickhouse_exposures(client) -> dict[str, list[Exposure]]:
    """Test-only: every household's exposures from `exposures_landed FINAL` — the
    Phase-6 read, kept here as the equivalence oracle for the lake pass."""
    rows = client.query(
        f"select {', '.join(EXPOSURE_COLS)} from exposures_landed final "
        "order by exposure_id"
    ).result_rows
    by_hh: dict[str, list[Exposure]] = defaultdict(list)
    for r in rows:
        e = Exposure(**dict(zip(EXPOSURE_COLS, r, strict=True)))
        by_hh[e.household_id].append(e)
    return by_hh


def _recovered_via_clickhouse(client, reconciled_at):
    expanded = expand_candidates(lake_candidates())
    return reconcile(
        expanded, _clickhouse_exposures(client), LONG_WINDOW, reconciled_at
    )


def test_reconcile_output_is_byte_identical_across_sources(oracle_run) -> None:
    client = connect()
    reconciled_at = reconciled_at_for(_max_ingest(client))
    assert lake_candidates(), "long_delay must produce hot-miss candidates"

    ch = _recovered_via_clickhouse(client, reconciled_at)
    assert ch, "long_delay must recover at least one conversion"
    # the Phase-17 bucket-aligned lake pass (the product path) == the same
    # candidates matched against ClickHouse-read exposures: same recovered set,
    # same order, same processed_at, same last-touch exposure_id + assists (+
    # candidate_households).
    bucketed = sorted(
        (r for d in candidate_days() for r in recover_day(d, reconciled_at)),
        key=lambda r: r.conversion_id,
    )
    assert [r.model_dump() for r in bucketed] == [r.model_dump() for r in ch]


def test_dagster_pass_writes_the_same_reconciled_rows(oracle_run) -> None:
    client = connect()
    reconciled_at = reconciled_at_for(_max_ingest(client))
    expected = {
        r.conversion_id for r in _recovered_via_clickhouse(client, reconciled_at)
    }

    # Orchestrated, Iceberg-sourced, day-partitioned recovery → lake append →
    # reload of the touched days → finalize (headless).
    dagster_main(["--profile", "long_delay"])

    got = {
        r[0]
        for r in client.query(
            "select conversion_id from attributed_conversions final "
            "where path = 'reconciled' order by conversion_id"
        ).result_rows
    }
    assert got == expected
    assert got, "the orchestrated pass must recover conversions"
    # a second pass finds none of them as hot-unattributed candidates any more
    assert {c.conversion_id for c in lake_candidates()}.isdisjoint(got)
