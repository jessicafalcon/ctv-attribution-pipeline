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
exposures_landed on (campaign_id, event_time, exposure_id). Chunked inserts; the
async-insert lever (`async_insert=1, wait_for_async_insert=1`) is built on this
writer (Phase 18b), OFF unless `LAKE_ASYNC_INSERT=1` — `make run` opts in, the
golden / oracle / capture paths leave it off (see `_async_insert_enabled`).

The loader also owns the ROLLUP DIRTY SET (Phase 18a). It is the only writer of
the serving tables and it knows exactly which day it just loaded, so it is the
only place that cannot disagree with what ClickHouse holds: after each day's
insert it records that day's (campaign_id, hour) rollup keys in `rollup_dirty`,
stamped with a data-derived version. `reconcile.rollup.refresh_campaign_hourly`
then recomputes only the keys whose recorded version differs from the version the
rollup was last computed against (`rollup_refreshed`, one row per key).
"""

import os
from collections.abc import Sequence
from datetime import UTC, datetime
from itertools import batched

from clickhouse_connect.driver.client import Client

from lake import metrics
from lake.read_attributed import read_current
from lake.read_exposures import read_exposures_for_days
from producer.models import AttributedConversion, Exposure

_BATCH = 256  # rows per ClickHouse insert → fewer, larger parts


def _async_insert_enabled() -> bool:
    """Async inserts are OFF unless `LAKE_ASYNC_INSERT=1` (set by `make run`). The
    golden / oracle / capture paths (`make run-hot`, `make metrics-capture`, the
    offline suite) leave it unset, so their rows stay on the known synchronous path
    and no frozen pin or captured metric moves for a server-side-batching reason.
    `make run` opts in; the LIVE parity check proves the rows are byte-identical
    either way (Invariant 1)."""
    return os.environ.get("LAKE_ASYNC_INSERT", "0") == "1"


def _async_settings(async_insert: bool | None = None) -> dict:
    """ClickHouse insert settings for the async lever. Enabled → buffer rows
    server-side into fewer, larger parts (`async_insert=1`) AND block until the
    buffer is flushed (`wait_for_async_insert=1`), so a read right after a load sees
    every row — no eventual-visibility race, the property that keeps serving rows
    byte-identical (Invariants 1, 2). `async_insert` overrides the env for callers
    that pin the flag directly (the parity test)."""
    enabled = _async_insert_enabled() if async_insert is None else async_insert
    return {"async_insert": 1, "wait_for_async_insert": 1} if enabled else {}


# DDL column order (clickhouse/ddl.sql); pinned to the models by
# tests/test_column_contract.py.
ATTRIBUTED_COLS = list(AttributedConversion.model_fields)
EXPOSURE_COLS = list(Exposure.model_fields)


def _utc(dt: datetime) -> datetime:
    """clickhouse-connect is asymmetric (ARCHITECTURE §8, Phase 17): it reads a
    DateTime64(3,'UTC') column back as a NAIVE UTC wall-clock, but writes a naive
    datetime as LOCAL wall-clock (→ a +6h shift on a MDT laptop, none in CI). The
    lake readers hand back naive UTC by contract (DECISIONS Phase 12), so every
    datetime is made tz-aware UTC here before it is inserted — the loader must
    move rows, never change them."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _attributed_values(r: AttributedConversion) -> list:
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


def _exposure_values(r: Exposure) -> list:
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


def insert_attributed(
    client: Client,
    rows: Sequence[AttributedConversion],
    *,
    async_insert: bool | None = None,
) -> None:
    settings = _async_settings(async_insert) or None
    for chunk in batched(rows, _BATCH):
        client.insert(
            "attributed_conversions",
            [_attributed_values(r) for r in chunk],
            column_names=ATTRIBUTED_COLS,
            settings=settings,
        )
    metrics.ROWS_LOADED.labels(table="attributed_conversions").inc(len(rows))


def insert_exposures(
    client: Client, rows: Sequence[Exposure], *, async_insert: bool | None = None
) -> None:
    settings = _async_settings(async_insert) or None
    for chunk in batched(rows, _BATCH):
        client.insert(
            "exposures_landed",
            [_exposure_values(r) for r in chunk],
            column_names=EXPOSURE_COLS,
            settings=settings,
        )
    metrics.ROWS_LOADED.labels(table="exposures_landed").inc(len(rows))


# The rollup buckets by the CREDITED EXPOSURE's event-time hour (reconcile/rollup.py),
# so a day's dirty keys come from two sides: the exposures landed for that day, and
# the exposures that the day's current attributed rows are credited to — which can sit
# up to the long window earlier than the conversion's own day. Both are computed FROM
# the rows now in ClickHouse (not from what this process happened to insert), so a
# re-load of the same day records the same keys and the same versions: idempotent, no
# wall clock. `final` on both reads for the same reason every other rollup read takes
# it (DECISIONS Phase 4).
_DIRTY_FROM_EXPOSURES = """
insert into rollup_dirty
select
    campaign_id,
    toStartOfHour(event_time) as hour,
    max(ingest_time) as version
from exposures_landed final
where toDate(event_time) = {day:Date}
group by
    campaign_id,
    hour
"""

_DIRTY_FROM_ATTRIBUTED = """
insert into rollup_dirty
select
    e.campaign_id as campaign_id,
    toStartOfHour(e.event_time) as hour,
    max(a.processed_at) as version
from attributed_conversions as a final
inner join
(
    select
        exposure_id,
        campaign_id,
        event_time
    from exposures_landed final
) as e
    on a.exposure_id = e.exposure_id
where a.attributed = 1
    and toDate(a.event_time) = {day:Date}
group by
    campaign_id,
    hour
"""


_DIRTY_FROM_EXPOSURE_CREDITS = """
insert into rollup_dirty
select
    e.campaign_id as campaign_id,
    toStartOfHour(e.event_time) as hour,
    max(a.processed_at) as version
from attributed_conversions as a final
inner join
(
    select
        exposure_id,
        campaign_id,
        event_time
    from exposures_landed final
    where toDate(event_time) = {day:Date}
) as e
    on a.exposure_id = e.exposure_id
where a.attributed = 1
group by
    campaign_id,
    hour
"""


def record_dirty_exposure_keys(client: Client, day: str) -> None:
    """Mark the rollup keys this day's exposures land in (spend/exposure counts), at
    the max of BOTH sides' stamps — the day's exposures and any conversions already
    credited to them. The second half is what makes the recording independent of LOAD
    ORDER: if a conversion's day was loaded first, its exposures were not there yet and
    the conversion side recorded nothing, so this pass picks it up. Either order leaves
    the same `rollup_dirty` FINAL (review gate; the scenario is pinned live by
    `tests/integration/test_rollup_dirty.py::test_reverse_order_day_loads_leave_the_same_rollup_dirty`)."""
    client.command(_DIRTY_FROM_EXPOSURES, parameters={"day": day})
    client.command(_DIRTY_FROM_EXPOSURE_CREDITS, parameters={"day": day})


def record_dirty_attributed_keys(client: Client, day: str) -> None:
    """Mark the rollup keys this day's credited conversions land in — the hours of
    the EXPOSURES they are credited to, not the conversion's own hour."""
    client.command(_DIRTY_FROM_ATTRIBUTED, parameters={"day": day})


def load_exposures_day(client: Client, day: str) -> int:
    """Insert the lake's distinct exposures for `day` (YYYY-MM-DD); return count.
    Records this day's rollup keys (Phase 18a) after the insert, never before —
    the recording reads the loaded rows back."""
    rows = read_exposures_for_days([day])
    insert_exposures(client, rows)
    record_dirty_exposure_keys(client, day)
    return len(rows)


def load_attributed_day(client: Client, day: str) -> int:
    """Insert the lake's current attributed row per conversion_id for `day`.
    Records the rollup keys those rows credit (Phase 18a) after the insert."""
    rows = read_current(days=[day])
    insert_attributed(client, rows)
    record_dirty_attributed_keys(client, day)
    return len(rows)
