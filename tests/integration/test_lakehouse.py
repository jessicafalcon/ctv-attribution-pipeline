"""Phase-12 LIVE lakehouse proof on a CLEAN long_delay-only stack
(`make test-int-lakehouse`: make down && up && seed long_delay && lake-land
long_delay). NOT part of the shared `make test-int` (tiny-only) — same
shared-conversion_id isolation as the other isolated live proofs (DECISIONS
Phase 5).

`make lake-land` runs resolve → engine and dual-writes exposures to BOTH the
ClickHouse exposures_landed table AND the Iceberg lake, leaving attributed_conversions
with only hot rows. Proves the Done-when:
- #2 (byte-identical source swap): the recovered rows are identical whether the
  matcher's exposures come from ClickHouse or from Iceberg-via-DuckDB.
- #3 (orchestration): the Dagster day-partitioned pass writes exactly the same
  reconciled rows a single ClickHouse pass would.
"""

import os

from clickhouse.client import connect
from orchestration.run import main as dagster_main
from reconcile.reconcile import (
    LONG_WINDOW,
    _max_ingest,
    _read_candidates,
    expand_candidates,
    reconcile,
    reconciled_at_for,
)
from reconcile.sources import ClickHouseExposureSource, IcebergExposureSource
from resolve.stage import load_graph_index

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")


def _recovered_via(source, candidates, reconciled_at):
    expanded = expand_candidates(candidates, load_graph_index(BROKER))
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
    # row-content identical (Done-when #2): same recovered set, same order, same
    # processed_at version, same last-touch exposure_id + assists.
    assert [r.model_dump() for r in ch] == [r.model_dump() for r in ice]


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

    # Orchestrated, Iceberg-sourced, day-partitioned recovery + finalize (headless).
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
