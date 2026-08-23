"""LIVE (Phase 18b, Done-when 1): the async-insert lever changes cost, not rows.

The same rows inserted with `async_insert=1, wait_for_async_insert=1` and without
land byte-identical (Invariant 1); and because `wait_for_async_insert=1` blocks
until the server-side buffer is flushed, a read issued right after the load sees
every row — no eventual-visibility race (Invariant 2).

Uses two scratch tables shaped exactly like `exposures_landed` (structure AND engine
copied via `create table … as`) and drops them, so it never touches the live serving
rows. Runs under any `make test-int*` target; asserts row content, not a profile's
numbers.
"""

from datetime import UTC, datetime, timedelta
from itertools import batched

import pytest

from clickhouse.client import connect
from lake.load_serving import EXPOSURE_COLS, _async_settings, _exposure_values
from producer.models import Exposure

N = 300  # > _BATCH (256), so batching across more than one chunk is exercised


def _rows(n: int) -> list[Exposure]:
    base = datetime(2026, 8, 1, tzinfo=UTC)
    return [
        Exposure(
            exposure_id=f"e-{i:06d}",
            event_time=base + timedelta(minutes=i),
            ingest_time=base + timedelta(minutes=i, seconds=5),
            campaign_id=f"camp-{i % 4:02d}",
            household_id=f"h-{i % 7:04d}",
            ip=f"10.0.0.{i % 254}",
            app_id="app-01",
            program_genre="drama",
            spend=round(0.01 * (i % 9), 2),
        )
        for i in range(n)
    ]


def _insert(client, table: str, rows: list[Exposure], async_insert: bool) -> None:
    settings = _async_settings(async_insert) or None
    for chunk in batched(rows, 256):
        client.insert(
            table,
            [_exposure_values(r) for r in chunk],
            column_names=EXPOSURE_COLS,
            settings=settings,
        )


@pytest.fixture
def probe_tables():
    client = connect()
    sync_t, async_t = "async_probe_sync", "async_probe_async"
    for t in (sync_t, async_t):
        client.command(f"drop table if exists {t}")
        client.command(f"create table {t} as exposures_landed")
    yield client, sync_t, async_t
    for t in (sync_t, async_t):
        client.command(f"drop table if exists {t}")


def test_serving_rows_are_byte_identical_with_async_on_and_off(probe_tables) -> None:
    client, sync_t, async_t = probe_tables
    rows = _rows(N)
    _insert(client, sync_t, rows, async_insert=False)
    _insert(client, async_t, rows, async_insert=True)
    for t in (sync_t, async_t):
        client.command(f"optimize table {t} final")

    sync_rows = client.query(f"select * from {sync_t} order by exposure_id").result_rows
    async_rows = client.query(
        f"select * from {async_t} order by exposure_id"
    ).result_rows
    assert async_rows == sync_rows
    assert len(async_rows) == N


def test_a_read_right_after_a_load_sees_every_row(probe_tables) -> None:
    client, _, async_t = probe_tables
    _insert(client, async_t, _rows(N), async_insert=True)
    # No sleep, no OPTIMIZE: wait_for_async_insert=1 means the rows are queryable now.
    got = client.query(f"select count() from {async_t}").result_rows[0][0]
    assert got == N
