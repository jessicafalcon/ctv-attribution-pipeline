"""Dagster software-defined assets for orchestrated reconciliation (Phase 12).

A Dagster software-defined asset = a data product Dagster tracks with lineage and
partitions, so "the exposure lake" and "reconcile day D" become named,
independently re-runnable, backfillable units instead of one opaque `make run`
subprocess. Determinism is preserved by construction: the assets call the SAME
pure `reconcile.recover` / `reconcile.finalize` used by `make run`; only the
exposure source is the Iceberg lake. Dagster run ids / wall-clock are
non-deterministic and are NOT asserted on (DECISIONS Phase 12).
"""

from datetime import date, timedelta

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from lake.iceberg_catalog import ensure_exposures
from reconcile.reconcile import (
    LONG_WINDOW,
    _max_ingest,
    _read_candidates,
    finalize,
    reconciled_at_for,
    recover,
)
from reconcile.sources import IcebergExposureSource


def _day_keys(start: date, end: date) -> list[str]:
    """YYYY-MM-DD keys for [start, end)."""
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days)]


# Day partitions as a STATIC set, not DailyPartitionsDefinition: the latter
# validates keys against the real wall clock (rejecting any day not yet elapsed),
# but the deterministic producer emits conversion days in the wall-clock future —
# so a DailyPartitionsDefinition run would accept a different set of partitions
# depending on when it ran, breaking the determinism policy ("same input → same
# answer on a re-run"). A fixed static set spanning the sim calendar is
# reproducible and still day-granular + backfillable (DECISIONS Phase 12). All
# profiles seed 2026-08-01; a one-year span covers every tail. The runner
# materializes only the days that hold candidates.
DAY_PARTITIONS = StaticPartitionsDefinition(
    _day_keys(date(2026, 8, 1), date(2027, 8, 1))
)


@asset
def exposures_iceberg() -> MaterializeResult:
    """The Iceberg `raw.exposures` lake table (landed out-of-band by
    make lake-land). This asset observes it — ensures it exists and reports its
    row count — it does not re-land; landing rides the engine so the lake and
    ClickHouse share one input set (DECISIONS Phase 12)."""
    table = ensure_exposures()
    rows = table.scan().to_arrow().num_rows
    return MaterializeResult(metadata={"rows": MetadataValue.int(rows)})


@asset(deps=[exposures_iceberg], partitions_def=DAY_PARTITIONS)
def reconciled_conversions(context: AssetExecutionContext) -> MaterializeResult:
    """Recover the hot-misses whose conversion event_time falls on this partition
    day, sourcing their households' exposures from the Iceberg lake via DuckDB.
    `reconciled_at` is the global, data-derived version (max ingest_time + 1s,
    stable across days), so a per-day backfill stamps exactly what a single full
    ClickHouse pass would — the byte-identical guarantee (Done-when #2)."""
    day = context.partition_key
    client = connect()
    apply_ddl()
    reconciled_at = reconciled_at_for(_max_ingest(client))
    candidates = [
        c for c in _read_candidates(client) if c.event_time.date().isoformat() == day
    ]
    # Candidate households come from the rows themselves (Phase 17) — no broker.
    recovered = recover(
        client, candidates, IcebergExposureSource(), LONG_WINDOW, reconciled_at
    )
    return MaterializeResult(
        metadata={
            "candidates": MetadataValue.int(len(candidates)),
            "recovered": MetadataValue.int(len(recovered)),
        }
    )


@asset(deps=[reconciled_conversions])
def reconciled_report() -> MaterializeResult:
    """Global finalize, once all days are recovered: pre/post report snapshots +
    rollup refresh (reconcile.finalize). Non-partitioned — it summarizes the whole
    reconciled state, not one day."""
    client = connect()
    finalize(client)
    return MaterializeResult()
