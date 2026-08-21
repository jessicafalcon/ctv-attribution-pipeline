"""Local Iceberg catalog for the exposure lake (Phase 12).

An Iceberg table = a catalog + metadata files that track which Parquet files
form the current table snapshot, so many engines (DuckDB here; Spark/Trino at
scale) read one table with ACID appends and time-partitioning. This catalog is a
local SqlCatalog on SQLite and the warehouse is a plain `file://` directory under
data/lake/ (gitignored) — no object store, no REST catalog (those are the
SCALING port, not built).

Determinism carve-out: Iceberg stamps a fresh snapshot_id + commit timestamp per
append, so table *metadata* is not byte-identical across runs even though the
*rows* are. That is carved out of the pipeline's byte-identical guarantee exactly
like the agent (DECISIONS Phase 12) — every asserted check reads row content
back, never snapshot ids or commit times, and the tiny golden gate keeps reading
the deterministic ClickHouse copy.
"""

import os
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

# Lake root defaults to data/lake/ (gitignored); LAKE_ROOT overrides it so tests
# point at an isolated tmp warehouse. Resolved per call, not at import, so the
# override takes effect for a process that sets it after import.
_DEFAULT_ROOT = "data/lake"
NAMESPACE = "raw"
TABLE = "raw.exposures"


def _lake_root() -> Path:
    return Path(os.environ.get("LAKE_ROOT", _DEFAULT_ROOT))


# event_time / ingest_time land as TIMESTAMPTZ (a UTC instant), never a naive
# timestamp: exposures_landed is DateTime64(3,'UTC') and clickhouse-connect hands
# back tz-aware UTC, so a tz-less lake column (or a local-tz render on read) would
# reintroduce the ARCHITECTURE §8 timezone gotcha that already bit reconcile.
# Field ids mirror the Exposure field order (producer/models.py).
EXPOSURE_SCHEMA = Schema(
    NestedField(1, "exposure_id", StringType(), required=True),
    NestedField(2, "event_time", TimestamptzType(), required=True),
    NestedField(3, "ingest_time", TimestamptzType(), required=True),
    NestedField(4, "campaign_id", StringType(), required=True),
    NestedField(5, "household_id", StringType(), required=True),
    NestedField(6, "ip", StringType(), required=True),
    NestedField(7, "app_id", StringType(), required=True),
    NestedField(8, "program_genre", StringType(), required=True),
    NestedField(9, "spend", DoubleType(), required=True),
)

# Day-partition on event_time: a day transform buckets rows into per-day files, so
# a time-bounded read prunes whole days — the lake counterpart to the ClickHouse
# leading (campaign_id, event_time) key's range prune (reconcile.read).
_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=2, field_id=1000, transform=DayTransform(), name="event_time_day"
    )
)


def connect_catalog() -> Catalog:
    """Open (creating on first use) the local SqlCatalog under the lake root."""
    root = _lake_root()
    warehouse = root / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "lake",
        uri=f"sqlite:///{root / 'catalog.db'}",
        warehouse=f"file://{warehouse.resolve()}",
    )


def ensure_table(catalog: Catalog | None = None) -> Table:
    """Create-if-not-exists the day-partitioned `raw.exposures` table and return
    a handle to its current snapshot."""
    catalog = catalog or connect_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog.create_table_if_not_exists(
        identifier=TABLE, schema=EXPOSURE_SCHEMA, partition_spec=_PARTITION_SPEC
    )
