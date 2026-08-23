"""Local Iceberg catalog for the lake of record (Phase 12; Phase 17 = record).

An Iceberg table = a catalog + metadata files that track which Parquet files
form the current table snapshot, so many engines (DuckDB here; Spark/Trino at
scale) read one table with ACID appends and partitioning. This catalog is a
local SqlCatalog on SQLite and the warehouse is a plain `file://` directory under
data/lake/ (gitignored) — no object store, no REST catalog (those are the
SCALING port, not built).

Two raw tables, both partitioned `day(event_time)` × `bucket(N, household_id)`:
`raw.exposures` and `raw.attributed_conversions` (the ClickHouse table's 19
columns, same order — spec D3). Bucketing by household is what makes the 90-day
reconcile join partition-local: a candidate's exposures live in the same bucket
number of every day partition, so the join reads N-th-of-the-lake, never the
lake. N is recorded as a table property, identical on both tables, set once per
deployment and never changed on a populated lake (spec D7; SCALING.md lever).

Determinism carve-out: Iceberg stamps a fresh snapshot_id + commit timestamp per
append, so table *metadata* is not byte-identical across runs even though the
*rows* are. That is carved out of the pipeline's byte-identical guarantee exactly
like the agent (DECISIONS Phase 12) — every asserted check reads row content
back, never snapshot ids or commit times.
"""

import os
import re
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform, DayTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    ListType,
    NestedField,
    StringType,
    TimestamptzType,
)

# The lake root is data/lake/<profile> (gitignored), derived from the PROFILE the
# entry point was given (`configure`). There is NO default root: an unset default
# is what once produced a profile-mixed lake at data/lake/ that `make lake-reset`
# refuses to touch (review gate). LAKE_ROOT is TEST-ONLY — pytest fixtures point
# the code at a tmp warehouse; an entry point refuses it outside pytest. Resolved
# per call, not at import, so a fixture set after import takes effect.
_LAKES = Path("data/lake")
_PROFILE_RE = re.compile(r"^[a-z0-9_]+$")
_profile: str | None = None
NAMESPACE = "raw"
EXPOSURES_TABLE = "raw.exposures"
ATTRIBUTED_TABLE = "raw.attributed_conversions"

# The bucket count (spec D7). Laptop tier: 8. A SCALING.md lever chosen once per
# deployment like a Kafka partition count — `ensure_*` refuses a populated lake
# whose recorded count differs (make lake-reset, never a silent re-spec).
BUCKET_COUNT = 8
BUCKET_PROPERTY = "ctv.bucket_count"


class LakeRootUnset(RuntimeError):
    """No lake root: the entry point did not `configure(profile)` and no test
    fixture set LAKE_ROOT. Never silently fall back to a shared directory."""


def configure(profile: str) -> Path:
    """Bind this process's lake to data/lake/<profile> (entry points call this
    from `--profile`). Refuses a malformed profile (the same `[a-z0-9_]+` shape
    `make lake-reset` enforces) and refuses a LAKE_ROOT override outside pytest —
    LAKE_ROOT is for tmp-lake fixtures only."""
    global _profile
    _profile = validate_profile(profile)
    return _lake_root()


def validate_profile(profile: str) -> str:
    """The profile-name rule, in one place: `[a-z0-9_]+`. Returns it unchanged or
    raises. Public because entry points that take `--profile` but bind NO lake
    (`make rollup-bench` reads ClickHouse only) must refuse an empty, path-escaping
    or metacharacter value by exactly the same rule the lake paths use."""
    if not _PROFILE_RE.match(profile):
        raise LakeRootUnset(f"profile {profile!r} is not [a-z0-9_]+")
    return profile


def profile() -> str | None:
    return _profile


def _lake_root() -> Path:
    env = os.environ.get("LAKE_ROOT")
    if env:
        # Test-only: honoured under pytest, refused everywhere else — here, in
        # the one resolver, so no importer (an entry point, the Dagster code
        # location, a notebook) can bypass the rule (review gate, round 3).
        if not os.environ.get("PYTEST_CURRENT_TEST"):
            raise LakeRootUnset(
                "LAKE_ROOT is test-only (tmp fixtures); entry points bind the lake "
                "with --profile — unset LAKE_ROOT"
            )
        return Path(env)
    if _profile:
        return _LAKES / _profile
    raise LakeRootUnset(
        "no lake root: entry points pass --profile (lake.iceberg_catalog.configure); "
        "tests set LAKE_ROOT to a tmp dir"
    )


def catalog_exists() -> bool:
    """Whether this root holds a catalog already — a read that must not CREATE
    an empty lake as a side effect (replay's refusal) checks this first."""
    return (_lake_root() / "catalog.db").exists()


# event_time / ingest_time land as TIMESTAMPTZ (a UTC instant), never a naive
# timestamp: the ClickHouse copies are DateTime64(3,'UTC') and clickhouse-connect
# hands back tz-aware UTC, so a tz-less lake column (or a local-tz render on read)
# would reintroduce the ARCHITECTURE §8 timezone gotcha that already bit reconcile.
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

# The attributed row: AttributedConversion's 19 fields in model order (pinned by
# tests/test_column_contract.py against the model, the sink, the DDL and the
# reconcile read-back). Nullable exactly where the model is Optional.
ATTRIBUTED_SCHEMA = Schema(
    NestedField(1, "conversion_id", StringType(), required=True),
    NestedField(2, "event_time", TimestamptzType(), required=True),
    NestedField(3, "ingest_time", TimestamptzType(), required=True),
    NestedField(4, "device_id", StringType(), required=True),
    NestedField(5, "ip", StringType(), required=True),
    NestedField(6, "conversion_type", StringType(), required=True),
    NestedField(7, "revenue", DoubleType(), required=True),
    NestedField(8, "order_id", StringType(), required=False),
    NestedField(9, "household_id", StringType(), required=True),
    NestedField(10, "resolution", StringType(), required=True),
    NestedField(11, "ambiguous", BooleanType(), required=True),
    NestedField(12, "candidate_count", IntegerType(), required=True),
    NestedField(13, "exposure_id", StringType(), required=False),
    NestedField(
        14, "assists", ListType(100, StringType(), element_required=True), required=True
    ),
    NestedField(15, "attributed", BooleanType(), required=True),
    NestedField(16, "path", StringType(), required=True),
    NestedField(17, "processed_at", TimestamptzType(), required=True),
    NestedField(18, "reason", StringType(), required=False),
    NestedField(
        19,
        "candidate_households",
        ListType(101, StringType(), element_required=True),
        required=True,
    ),
)


def _spec(event_time_id: int, household_id_id: int) -> PartitionSpec:
    """`day(event_time)` then `bucket(N, household_id)`: a time-bounded read prunes
    whole days, a household-keyed read prunes 1/N of each day (verified on the
    pinned DuckDB — ARCHITECTURE §8, Phase-17 stack check)."""
    return PartitionSpec(
        PartitionField(
            source_id=event_time_id,
            field_id=1000,
            transform=DayTransform(),
            name="event_time_day",
        ),
        PartitionField(
            source_id=household_id_id,
            field_id=1001,
            transform=BucketTransform(BUCKET_COUNT),
            name="household_bucket",
        ),
    )


_EXPOSURE_SPEC = _spec(event_time_id=2, household_id_id=5)
_ATTRIBUTED_SPEC = _spec(event_time_id=2, household_id_id=9)


class LakeLayoutError(RuntimeError):
    """An existing lake table's partition layout or bucket count is not this
    code's. Never re-spec a populated lake: `make lake-reset` (destructive,
    confirmed), then re-populate."""


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


def _ensure(
    catalog: Catalog | None, identifier: str, schema: Schema, spec: PartitionSpec
) -> Table:
    catalog = catalog or connect_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    table = catalog.create_table_if_not_exists(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
        properties={BUCKET_PROPERTY: str(BUCKET_COUNT)},
    )
    _check_layout(table, identifier, spec)
    return table


def _check_layout(table: Table, identifier: str, spec: PartitionSpec) -> None:
    """Fail loud on a lake created under another layout (e.g. the Phase-12
    day-only spec, or a different bucket count) instead of silently appending
    into it — the partition-local join would then be wrong, not slow."""
    got_fields = [(f.source_id, str(f.transform), f.name) for f in table.spec().fields]
    want_fields = [(f.source_id, str(f.transform), f.name) for f in spec.fields]
    recorded = table.properties.get(BUCKET_PROPERTY)
    if got_fields != want_fields or recorded != str(BUCKET_COUNT):
        raise LakeLayoutError(
            f"{identifier}: lake layout is {got_fields} with {BUCKET_PROPERTY}="
            f"{recorded!r}, this code expects {want_fields} with "
            f"{BUCKET_COUNT} — the lake predates this layout; run make lake-reset "
            "(destructive) and re-populate, never re-spec a populated lake"
        )


def ensure_exposures(catalog: Catalog | None = None) -> Table:
    """Create-if-not-exists `raw.exposures` and return a current handle."""
    return _ensure(catalog, EXPOSURES_TABLE, EXPOSURE_SCHEMA, _EXPOSURE_SPEC)


def ensure_attributed(catalog: Catalog | None = None) -> Table:
    """Create-if-not-exists `raw.attributed_conversions` and return a handle."""
    return _ensure(catalog, ATTRIBUTED_TABLE, ATTRIBUTED_SCHEMA, _ATTRIBUTED_SPEC)


def bucket_count_of(table: Table) -> int:
    return int(table.properties[BUCKET_PROPERTY])


_BUCKET = BucketTransform(BUCKET_COUNT).transform(StringType())


def bucket_of(household_id: str) -> int:
    """The partition bucket a household's rows live in — pyiceberg's own
    `bucket[N]` hash (murmur3 of the UTF-8 string mod N), so Python routes an
    exploded candidate row to exactly the bucket the writer put its exposures in."""
    return _BUCKET(household_id)


def metadata_path(table: Table) -> str:
    """Local filesystem path to the table's current metadata JSON (strip the
    file:// scheme pyiceberg stores) — what DuckDB's iceberg_scan takes."""
    loc = table.metadata_location
    return loc[len("file://") :] if loc.startswith("file://") else loc
