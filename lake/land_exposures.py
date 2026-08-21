"""Append exposures to the local Iceberg `raw.exposures` table (Phase 12).

Idempotency is on READ, not on write. An Iceberg append only ACCUMULATES rows —
unlike the ClickHouse ReplacingMergeTree, which collapses re-sends on its sort key
at FINAL time — so re-landing the same exposures duplicates them in the lake. The
reconcile read (lake.read_exposures) collapses on exposure_id to reproduce the
ClickHouse FINAL set exactly. That is safe because of a stated invariant
(DECISIONS Phase 12): exposure_id is unique per exposure — the deterministic
producer maps one exposure_id to exactly one (campaign_id, event_time, …) row —
so a distinct-on-exposure_id read equals ClickHouse's collapse on the full
(campaign_id, event_time, exposure_id) sort key. Do NOT rely on append for dedup.
"""

from datetime import UTC, datetime

import pyarrow as pa

from lake.iceberg_catalog import ensure_table
from producer.models import Exposure

# Arrow schema mirrors EXPOSURE_SCHEMA: timestamptz(UTC) at microsecond precision
# (Iceberg's granularity). We store ms-truncated instants (see _to_utc_ms) so the
# stored value is bit-for-bit the DateTime64(3) ClickHouse holds — no sub-ms drift
# between the two copies.
# nullable=False on every field: the Iceberg schema marks all columns `required`,
# and pyiceberg rejects a nullable arrow field against a required one.
_ARROW_SCHEMA = pa.schema(
    [
        pa.field("exposure_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ingest_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("campaign_id", pa.string(), nullable=False),
        pa.field("household_id", pa.string(), nullable=False),
        pa.field("ip", pa.string(), nullable=False),
        pa.field("app_id", pa.string(), nullable=False),
        pa.field("program_genre", pa.string(), nullable=False),
        pa.field("spend", pa.float64(), nullable=False),
    ]
)


def _to_utc_ms(dt: datetime) -> datetime:
    """UTC-normalize and truncate to millisecond — the exposures_landed
    DateTime64(3,'UTC') granularity. Producer times are already tz-aware UTC and
    ms-granular (AwareDatetime sim_start + ms-rounded deltas), so this is a no-op
    on real data; it is the defensive guard that stops a stray sub-ms or naive
    datetime from diverging the lake copy from the ClickHouse copy."""
    dt = dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def _to_arrow(exposures: list[Exposure]) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "exposure_id": e.exposure_id,
                "event_time": _to_utc_ms(e.event_time),
                "ingest_time": _to_utc_ms(e.ingest_time),
                "campaign_id": e.campaign_id,
                "household_id": e.household_id,
                "ip": e.ip,
                "app_id": e.app_id,
                "program_genre": e.program_genre,
                "spend": e.spend,
            }
            for e in exposures
        ],
        schema=_ARROW_SCHEMA,
    )


def land(exposures: list[Exposure]) -> int:
    """Append `exposures` to raw.exposures; return the count appended. Consumes
    the SAME deduped list the ClickHouse sink lands (streaming.dataflow run_engine
    under --lake-land), so the two copies share one input set by construction
    (DECISIONS Phase 12)."""
    if not exposures:
        return 0
    table = ensure_table()
    table.append(_to_arrow(exposures))
    return len(exposures)
