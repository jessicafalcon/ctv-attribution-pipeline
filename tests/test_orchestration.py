"""Phase-12 Dagster asset unit test (offline, no services).

Asserts the asset graph's shape — the lineage and partitioning that make
"reconcile day D" an independent, backfillable unit — without touching ClickHouse
or the lake. Run behavior lives in the live test_lakehouse integration test.
"""

from dagster import StaticPartitionsDefinition

from orchestration.assets import (
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)
from orchestration.definitions import defs


def test_definitions_expose_the_three_assets() -> None:
    keys = {a.key.to_user_string() for a in defs.assets}
    assert keys == {"exposures_iceberg", "reconciled_conversions", "reconciled_report"}


def test_reconciled_conversions_is_day_partitioned_on_the_lake() -> None:
    # Static day partitions (wall-clock-independent — determinism policy), each key
    # a calendar day, feeding an independently backfillable per-day recovery.
    pdef = reconciled_conversions.partitions_def
    assert isinstance(pdef, StaticPartitionsDefinition)
    keys = pdef.get_partition_keys()
    assert keys[0] == "2026-08-01"
    assert all(len(k) == 10 and k.count("-") == 2 for k in keys)  # YYYY-MM-DD
    # depends on the Iceberg lake source (lineage, not a data-passing input)
    deps = reconciled_conversions.asset_deps[reconciled_conversions.key]
    assert exposures_iceberg.key in deps


def test_report_finalize_depends_on_the_recovered_days() -> None:
    # non-partitioned global finalize, downstream of the per-day recovery
    assert reconciled_report.partitions_def is None
    deps = reconciled_report.asset_deps[reconciled_report.key]
    assert reconciled_conversions.key in deps


def test_exposures_iceberg_is_not_partitioned() -> None:
    assert exposures_iceberg.partitions_def is None
