"""Dagster asset graph shape (offline, no services) — Phase 12, flipped in Phase 17.

Asserts the lineage and partitioning that make "load day D" and "reconcile day D"
independent, backfillable units — without touching ClickHouse or the lake. Run
behavior lives in the live test_lakehouse integration test.
"""

from dagster import StaticPartitionsDefinition

from orchestration.assets import (
    DAY_PARTITIONS,
    attributed_iceberg,
    clickhouse_attributed_conversions,
    clickhouse_exposures_landed,
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)
from orchestration.definitions import defs


def test_definitions_expose_the_six_assets() -> None:
    keys = {a.key.to_user_string() for a in defs.assets}
    assert keys == {
        "exposures_iceberg",
        "attributed_iceberg",
        "clickhouse_exposures_landed",
        "clickhouse_attributed_conversions",
        "reconciled_conversions",
        "reconciled_report",
    }


def test_day_partitions_are_static_calendar_days() -> None:
    # Static day partitions (wall-clock-independent — determinism policy), each key
    # a calendar day; which ones run is decided by the data (touched days).
    assert isinstance(DAY_PARTITIONS, StaticPartitionsDefinition)
    keys = DAY_PARTITIONS.get_partition_keys()
    assert keys[0] == "2026-08-01"
    assert all(len(k) == 10 and k.count("-") == 2 for k in keys)  # YYYY-MM-DD


def test_load_assets_are_day_partitioned_downstream_of_the_lake() -> None:
    # The arrow: lake → ClickHouse. Each load asset is per-day and depends on its
    # raw table's observe asset (lineage, not a data-passing input).
    for load, raw in (
        (clickhouse_exposures_landed, exposures_iceberg),
        (clickhouse_attributed_conversions, attributed_iceberg),
    ):
        assert load.partitions_def is DAY_PARTITIONS
        assert raw.key in load.asset_deps[load.key]
        assert raw.partitions_def is None


def test_reconcile_reads_candidates_and_exposures_from_the_lake() -> None:
    # Day-partitioned (bucket loop inside — DECISIONS Phase 17); both raw tables
    # are lineage inputs, plus the loaded serving state it stamps its version from.
    assert reconciled_conversions.partitions_def is DAY_PARTITIONS
    deps = reconciled_conversions.asset_deps[reconciled_conversions.key]
    assert attributed_iceberg.key in deps  # candidates (lake, argMax current)
    assert exposures_iceberg.key in deps  # exposures (lake via DuckDB)
    # reconciled_at = max(ingest_time) over BOTH loaded tables → both are lineage
    assert clickhouse_exposures_landed.key in deps
    assert clickhouse_attributed_conversions.key in deps


def test_report_finalize_follows_recovery_and_reload() -> None:
    # non-partitioned global finalize, downstream of the per-day recovery AND the
    # reload of the corrected days (it reads ClickHouse)
    assert reconciled_report.partitions_def is None
    deps = reconciled_report.asset_deps[reconciled_report.key]
    assert reconciled_conversions.key in deps
    assert clickhouse_attributed_conversions.key in deps
