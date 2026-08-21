"""The direct-write ORACLE (Phase 17) — formerly `streaming/sink.py`, the engine's
ClickHouse sink through Phase 16. Since Phase 17 no product path writes the
serving tables from the engine: rows go engine → Iceberg lake → Dagster load →
ClickHouse (`lake/load_serving.py` is the one serving-table writer). This module
keeps the old model→column mapping so the integration parity proof
(tests/integration/test_lakehouse.py) can assert lake-loaded serving rows ==
exactly what the direct writer would have inserted. Test-only; never imported
by product code (tests/test_column_contract.py pins its column order to the
model, the DDL and the loader)."""

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


def attributed_values(r: AttributedConversion) -> list:
    return [
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


def insert_attributed(client: Client, rows: Sequence[AttributedConversion]) -> None:
    data = [attributed_values(r) for r in rows]
    client.insert("attributed_conversions", data, column_names=_ATTRIBUTED_COLS)


def exposure_values(r: Exposure) -> list:
    return [
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


def insert_exposures(client: Client, rows: Sequence[Exposure]) -> None:
    data = [exposure_values(r) for r in rows]
    client.insert("exposures_landed", data, column_names=_EXPOSURE_COLS)
