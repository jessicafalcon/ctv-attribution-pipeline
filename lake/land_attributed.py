"""Append attributed rows to the Iceberg `raw.attributed_conversions` table
(Phase 17 — the lake is the record, ClickHouse is loaded from it).

Append-only log (spec D4): the hot row AND, later, the reconciled row for the
same conversion_id both live here, distinguished by `path` and a strictly later
`processed_at`. "Current row per conversion_id" is a READ-side question
(lake.read_attributed, argMax-style) — never assume one row per key. The
ClickHouse ReplacingMergeTree collapses to the same current row on load, which is
what makes the loader idempotent.

Timestamps land TIMESTAMPTZ, ms-truncated, exactly as exposures do (DECISIONS
Phase 12), so a row read back is bit-for-bit the DateTime64(3) ClickHouse holds.
"""

import pyarrow as pa

from lake.iceberg_catalog import ensure_attributed
from lake.land_exposures import _to_utc_ms, touched_days
from producer.models import AttributedConversion

# Mirrors ATTRIBUTED_SCHEMA (lake/iceberg_catalog.py): nullable exactly where the
# Iceberg field is optional; list elements non-null like the schema's
# element_required.
_ARROW_SCHEMA = pa.schema(
    [
        pa.field("conversion_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ingest_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("device_id", pa.string(), nullable=False),
        pa.field("ip", pa.string(), nullable=False),
        pa.field("conversion_type", pa.string(), nullable=False),
        pa.field("revenue", pa.float64(), nullable=False),
        pa.field("order_id", pa.string(), nullable=True),
        pa.field("household_id", pa.string(), nullable=False),
        pa.field("resolution", pa.string(), nullable=False),
        pa.field("ambiguous", pa.bool_(), nullable=False),
        pa.field("candidate_count", pa.int32(), nullable=False),
        pa.field("exposure_id", pa.string(), nullable=True),
        pa.field(
            "assists",
            pa.list_(pa.field("element", pa.string(), nullable=False)),
            nullable=False,
        ),
        pa.field("attributed", pa.bool_(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        pa.field("processed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("reason", pa.string(), nullable=True),
        pa.field(
            "candidate_households",
            pa.list_(pa.field("element", pa.string(), nullable=False)),
            nullable=False,
        ),
    ]
)


def _to_arrow(rows: list[AttributedConversion]) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                **r.model_dump(),
                "event_time": _to_utc_ms(r.event_time),
                "ingest_time": _to_utc_ms(r.ingest_time),
                "processed_at": _to_utc_ms(r.processed_at),
            }
            for r in rows
        ],
        schema=_ARROW_SCHEMA,
    )


def land_attributed(rows: list[AttributedConversion]) -> set[str]:
    """Append `rows` to raw.attributed_conversions; return the event_time days
    touched (spec D6 — the days the loader must (re)materialize)."""
    if not rows:
        return set()
    ensure_attributed().append(_to_arrow(rows))
    return touched_days(r.event_time for r in rows)
