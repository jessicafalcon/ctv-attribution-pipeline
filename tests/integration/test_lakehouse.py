"""Phase-17 LIVE lake-of-record proof on a CLEAN long_delay-only stack
(`make test-int-lakehouse`: make down && up && seed long_delay && run-hot
long_delay). NOT part of the shared `make test-int` (tiny-only) — same
shared-conversion_id isolation as the other isolated live proofs (DECISIONS
Phase 5).

The stack's ClickHouse was populated by `make run-hot` = engine → lake → Dagster
load. This module re-runs the engine IN MEMORY (the direct-write oracle: the rows
the pre-Phase-17 sink would have inserted) and proves the spec's Done-when:
- #1/#2 arrow flipped: lake-loaded serving rows == oracle rows (row content,
  sorted, 6dp);
- #6 accumulated lake: landing the same run 3 more times into the lake and
  reloading leaves the serving rows byte-identical (dedup-on-read + RMT);
- Phase 12 #2 (kept): recovered rows identical whether the matcher's exposures
  come from ClickHouse or from Iceberg-via-DuckDB;
- Phase 12 #3 (kept): the Dagster-orchestrated pass writes the same reconciled
  rows a single pass would — now via lake append + reload.
"""

import os
from datetime import datetime

import pytest

from clickhouse.client import connect
from lake.load_serving import ATTRIBUTED_COLS, EXPOSURE_COLS
from orchestration.load import materialize_load
from orchestration.run import main as dagster_main
from reconcile.reconcile import (
    LONG_WINDOW,
    _max_ingest,
    _read_candidates,
    candidate_days,
    expand_candidates,
    reconcile,
    reconciled_at_for,
    recover_day,
)
from reconcile.sources import ClickHouseExposureSource, IcebergExposureSource
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
def oracle_run() -> EngineRun:
    return run_engine(BROKER)  # in memory; writes nothing


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


def _recovered_via(source, candidates, reconciled_at):
    expanded = expand_candidates(candidates)
    exposures = source.read_for(expanded, LONG_WINDOW)
    return reconcile(expanded, exposures, LONG_WINDOW, reconciled_at)


def test_reconcile_output_is_byte_identical_across_sources() -> None:
    client = connect()
    reconciled_at = reconciled_at_for(_max_ingest(client))
    candidates = _read_candidates(client)
    assert candidates, "long_delay must produce hot-miss candidates"

    ch = _recovered_via(ClickHouseExposureSource(client), candidates, reconciled_at)
    ice = _recovered_via(IcebergExposureSource(), candidates, reconciled_at)

    assert ch, "long_delay must recover at least one conversion"
    # row-content identical: same recovered set, same order, same processed_at
    # version, same last-touch exposure_id + assists (+ candidate_households).
    assert [r.model_dump() for r in ch] == [r.model_dump() for r in ice]
    # … and the Phase-17 bucket-aligned lake pass (the product path) equals both.
    bucketed = sorted(
        (r for d in candidate_days() for r in recover_day(d, reconciled_at)),
        key=lambda r: r.conversion_id,
    )
    assert [r.model_dump() for r in bucketed] == [r.model_dump() for r in ch]


def test_dagster_pass_writes_the_same_reconciled_rows() -> None:
    client = connect()
    reconciled_at = reconciled_at_for(_max_ingest(client))
    candidates = _read_candidates(client)
    expected = {
        r.conversion_id
        for r in _recovered_via(
            ClickHouseExposureSource(client), candidates, reconciled_at
        )
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
    assert {c.conversion_id for c in _read_candidates(client)}.isdisjoint(got)
