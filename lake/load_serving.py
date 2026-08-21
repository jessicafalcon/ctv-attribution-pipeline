"""Load the ClickHouse serving tables FROM the lake (Phase 17 — the lake is the
record, ClickHouse is a derived projection).

One day partition at a time: read that day's distinct exposures and its CURRENT
attributed row per conversion_id (argMax(processed_at) — lake.read_attributed)
and insert them. This is the ONE writer of `exposures_landed` and
`attributed_conversions`; the engine and the reconcile job land to the lake and
never touch ClickHouse rows directly (the old direct sink lives on only as the
test oracle, tests/oracle.py).

Idempotent by table design (DECISIONS Phase 3/17): re-materializing a day
re-inserts the same rows, which ReplacingMergeTree collapses on its sort key —
attributed_conversions on (conversion_id, version processed_at),
exposures_landed on (campaign_id, event_time, exposure_id). Synchronous chunked
inserts; async inserts are a SCALING lever, not built.
"""

from collections.abc import Sequence
from itertools import batched

from clickhouse_connect.driver.client import Client

from lake.read_attributed import read_current
from lake.read_exposures import read_exposures_for_days
from producer.models import AttributedConversion, Exposure

_BATCH = 256  # rows per ClickHouse insert → fewer, larger parts

# DDL column order (clickhouse/ddl.sql); pinned to the models by
# tests/test_column_contract.py.
ATTRIBUTED_COLS = list(AttributedConversion.model_fields)
EXPOSURE_COLS = list(Exposure.model_fields)


def _attributed_values(r: AttributedConversion) -> list:
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


def _exposure_values(r: Exposure) -> list:
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


def insert_attributed(client: Client, rows: Sequence[AttributedConversion]) -> None:
    for chunk in batched(rows, _BATCH):
        client.insert(
            "attributed_conversions",
            [_attributed_values(r) for r in chunk],
            column_names=ATTRIBUTED_COLS,
        )


def insert_exposures(client: Client, rows: Sequence[Exposure]) -> None:
    for chunk in batched(rows, _BATCH):
        client.insert(
            "exposures_landed",
            [_exposure_values(r) for r in chunk],
            column_names=EXPOSURE_COLS,
        )


def load_exposures_day(client: Client, day: str) -> int:
    """Insert the lake's distinct exposures for `day` (YYYY-MM-DD); return count."""
    rows = read_exposures_for_days([day])
    insert_exposures(client, rows)
    return len(rows)


def load_attributed_day(client: Client, day: str) -> int:
    """Insert the lake's current attributed row per conversion_id for `day`."""
    rows = read_current(days=[day])
    insert_attributed(client, rows)
    return len(rows)
