"""The direct-write ORACLE (Phase 17) — formerly `streaming/sink.py`, the engine's
ClickHouse sink through Phase 16. Since Phase 17 no product path writes the
serving tables from the engine: rows go engine → Iceberg lake → Dagster load →
ClickHouse (`lake/load_serving.py` is the one serving-table writer). This module
keeps the old model→column VALUE mapping so the integration parity proof
(tests/integration/test_lakehouse.py) can assert lake-loaded serving rows ==
exactly what the direct writer would have inserted. Test-only; never imported
by product code (tests/test_column_contract.py pins its column order to the
model, the DDL and the loader).

INDEPENDENCE: this mapping is the Phase-3→16 direct writer, independent of the
loader BY HISTORY — it is deliberately not derived from `lake/load_serving.py`,
or the parity test would be a tautology. It shares exactly one thing with the
loader, `_utc` (so the oracle cannot reproduce the §8 naive-write shift); every
other line is maintained by hand and pinned to the model by
tests/test_column_contract.py / test_sink.py. The insert functions were deleted at
the review gate — nothing may write the serving tables but the loader."""

from lake.load_serving import _utc
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
        _utc(r.event_time),
        _utc(r.ingest_time),
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
        _utc(r.processed_at),
        r.reason,
        r.candidate_households,
    ]


def exposure_values(r: Exposure) -> list:
    return [
        r.exposure_id,
        _utc(r.event_time),
        _utc(r.ingest_time),
        r.campaign_id,
        r.household_id,
        r.ip,
        r.app_id,
        r.program_genre,
        r.spend,
    ]
