"""ClickHouse sink for the Bytewax engine: a DynamicSink whose partitions
insert batches synchronously (async inserts → Phase 7). One ClickHouse client
per worker partition, built from env via clickhouse.client.connect.

`insert_attributed` and `insert_exposures` are the row builders — they map a
pydantic model to the DDL column order. Inserts are idempotent by table design:
attributed_conversions replaces on conversion_id/processed_at, exposures_landed
collapses on (campaign_id, event_time, exposure_id) (DECISIONS Phase 3)."""

from collections.abc import Callable, Sequence

from bytewax.outputs import DynamicSink, StatelessSinkPartition
from clickhouse_connect.driver.client import Client

from clickhouse.client import connect
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


class _Partition(StatelessSinkPartition):
    def __init__(self, insert: Callable[[Client, Sequence], None]):
        self._client = connect()
        self._insert = insert

    def write_batch(self, items: list) -> None:
        if items:
            self._insert(self._client, items)

    def close(self) -> None:
        self._client.close()


class ClickHouseSink(DynamicSink):
    """Insert each item batch via `insert`. Stateless: idempotency lives in the
    table engine, so retries/replays converge (idempotency contract)."""

    def __init__(self, insert: Callable[[Client, Sequence], None]):
        self._insert = insert

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition:
        return _Partition(self._insert)
