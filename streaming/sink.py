"""ClickHouse row inserts for the engine and the reconciliation job — synchronous;
async inserts are a SCALING lever, not built (SCALING.md 50k/500k tiers).

`insert_attributed` and `insert_exposures` map a pydantic model to the DDL
column order. Inserts are idempotent by table design: attributed_conversions
replaces on conversion_id/processed_at, exposures_landed collapses on
(campaign_id, event_time, exposure_id) (DECISIONS Phase 3)."""

from collections.abc import Sequence

from clickhouse_connect.driver.client import Client

from producer.models import AttributedConversion, Exposure

_ATTRIBUTED_COLS = [
    "conversion_id",
    "event_time",
    "ingest_time",
    "device_id",
    "ip",
    "conversion_type",
    "revenue",
    "order_id",
    "household_id",
    "resolution",
    "ambiguous",
    "candidate_count",
    "exposure_id",
    "assists",
    "attributed",
    "path",
    "processed_at",
    "reason",
    "candidate_households",
]
_EXPOSURE_COLS = [
    "exposure_id",
    "event_time",
    "ingest_time",
    "campaign_id",
    "household_id",
    "ip",
    "app_id",
    "program_genre",
    "spend",
]


def insert_attributed(client: Client, rows: Sequence[AttributedConversion]) -> None:
    data = [
        [
            r.conversion_id,
            r.event_time,
            r.ingest_time,
            r.device_id,
            r.ip,
            r.conversion_type,
            r.revenue,
            r.order_id,
            r.household_id,
            r.resolution,
            int(r.ambiguous),
            r.candidate_count,
            r.exposure_id,
            r.assists,
            int(r.attributed),
            r.path,
            r.processed_at,
            r.reason,
            r.candidate_households,
        ]
        for r in rows
    ]
    client.insert("attributed_conversions", data, column_names=_ATTRIBUTED_COLS)


def insert_exposures(client: Client, rows: Sequence[Exposure]) -> None:
    data = [
        [
            r.exposure_id,
            r.event_time,
            r.ingest_time,
            r.campaign_id,
            r.household_id,
            r.ip,
            r.app_id,
            r.program_genre,
            r.spend,
        ]
        for r in rows
    ]
    client.insert("exposures_landed", data, column_names=_EXPOSURE_COLS)
