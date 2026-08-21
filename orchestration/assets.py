"""Dagster software-defined assets — the lake of record → ClickHouse load and the
orchestrated reconciliation (Phase 12; Phase 17 flips the arrow).

A Dagster software-defined asset = a data product Dagster tracks with lineage and
partitions, so "load day D into ClickHouse" and "reconcile day D" become named,
independently re-runnable, backfillable units instead of one opaque `make run`
subprocess. Determinism is preserved by construction: the assets call the SAME
pure functions `make run` calls (`lake.load_serving`, `reconcile.recover`,
`reconcile.finalize`). Dagster run ids / wall-clock are non-deterministic and are
NOT asserted on (DECISIONS Phase 12).

Graph:
  exposures_iceberg ─┐                      ┌─ clickhouse_exposures_landed[day]
  attributed_iceberg ┴ (observe the lake) ──┴─ clickhouse_attributed_conversions[day]
                                                │
  reconciled_conversions[day]  (candidates + exposures from the lake, bucket-aligned,
                                corrections APPENDED to raw.attributed_conversions)
                                                │ → reload the touched days
  reconciled_report  (global finalize: snapshots + rollup)
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
from lake.iceberg_catalog import ensure_attributed, ensure_exposures
from lake.land_attributed import land_attributed
from lake.load_serving import load_attributed_day, load_exposures_day
from reconcile.reconcile import (
    _max_ingest,
    finalize,
    lake_candidates,
    reconciled_at_for,
    recover_day,
)


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
# profiles seed 2026-08-01; a one-year span covers every tail. Which days get
# materialized is decided by the DATA — the days a landing touched (spec D6), or
# the days holding reconcile candidates — never by today's date.
DAY_PARTITIONS = StaticPartitionsDefinition(
    _day_keys(date(2026, 8, 1), date(2027, 8, 1))
)


@asset
def exposures_iceberg() -> MaterializeResult:
    """The Iceberg `raw.exposures` table. Observes it (ensures it exists, reports
    the physical row count — an append-only log, so ≥ the distinct count)."""
    rows = ensure_exposures().scan().to_arrow().num_rows
    return MaterializeResult(metadata={"rows": MetadataValue.int(rows)})


@asset
def attributed_iceberg() -> MaterializeResult:
    """The Iceberg `raw.attributed_conversions` table (observe only)."""
    rows = ensure_attributed().scan().to_arrow().num_rows
    return MaterializeResult(metadata={"rows": MetadataValue.int(rows)})


@asset(deps=[exposures_iceberg], partitions_def=DAY_PARTITIONS)
def clickhouse_exposures_landed(context: AssetExecutionContext) -> MaterializeResult:
    """Load this day's distinct exposures from the lake into `exposures_landed`.
    Re-materializing is idempotent (ReplacingMergeTree collapses the re-insert)."""
    apply_ddl()
    n = load_exposures_day(connect(), context.partition_key)
    return MaterializeResult(metadata={"rows": MetadataValue.int(n)})


@asset(deps=[attributed_iceberg], partitions_def=DAY_PARTITIONS)
def clickhouse_attributed_conversions(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Load this day's CURRENT attributed row per conversion_id (argMax
    processed_at over the lake's append-only log) into `attributed_conversions`.
    Idempotent: keyed conversion_id, version processed_at."""
    apply_ddl()
    n = load_attributed_day(connect(), context.partition_key)
    return MaterializeResult(metadata={"rows": MetadataValue.int(n)})


@asset(
    deps=[exposures_iceberg, attributed_iceberg, clickhouse_exposures_landed],
    partitions_def=DAY_PARTITIONS,
)
def reconciled_conversions(context: AssetExecutionContext) -> MaterializeResult:
    """The bucket-aligned reconcile for this partition day (spec D2): the day's
    current hot-unattributed rows from raw.attributed_conversions, exploded over
    their candidate households, joined bucket-locally against raw.exposures in
    [day − 90d, day], reduced across buckets by `pick_household` — and the
    corrections APPENDED to raw.attributed_conversions (path=reconciled).
    `reconciled_at` is the global, data-derived version (max ingest_time over
    the loaded serving state + 1s, stable across days), so a per-day backfill
    stamps exactly what a single full pass would — the byte-identical
    guarantee. The touched days are reported so the runner reloads them into
    ClickHouse before the finalize. (Day-partitioned with the bucket loop
    inside, not day × bucket: the cross-bucket reduce needs every bucket's
    scores in one place, and an IO manager to carry them between runs is
    machinery the laptop tier does not need — DECISIONS Phase 17.)"""
    day = context.partition_key
    client = connect()
    apply_ddl()
    reconciled_at = reconciled_at_for(_max_ingest(client))
    candidates = lake_candidates(day)
    recovered = recover_day(day, reconciled_at)
    touched = land_attributed(recovered)
    return MaterializeResult(
        metadata={
            "candidates": MetadataValue.int(len(candidates)),
            "recovered": MetadataValue.int(len(recovered)),
            "touched_days": MetadataValue.json(sorted(touched)),
        }
    )


@asset(deps=[reconciled_conversions, clickhouse_attributed_conversions])
def reconciled_report() -> MaterializeResult:
    """Global finalize, once all days are recovered AND reloaded: pre/post report
    snapshots + rollup refresh (reconcile.finalize). Non-partitioned — it
    summarizes the whole reconciled state, not one day."""
    client = connect()
    finalize(client)
    return MaterializeResult()
