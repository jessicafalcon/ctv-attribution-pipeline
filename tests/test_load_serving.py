"""The lake → ClickHouse loader inserts tz-aware UTC datetimes (offline pin).

clickhouse-connect writes a NAIVE datetime as local wall-clock but reads a
DateTime64(3,'UTC') column back as naive UTC (ARCHITECTURE §8). The lake readers
return naive UTC by contract, so a loader that passed them through would shift
every event_time by the machine's UTC offset — found live on an MDT laptop (+6h),
invisible in CI (UTC). This pins the fix independent of the machine's timezone.
"""

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from lake.load_serving import (
    _async_insert_enabled,
    _async_settings,
    _attributed_values,
    _exposure_values,
    insert_exposures,
)
from producer.models import AttributedConversion, Exposure

T_NAIVE = datetime(2026, 8, 8, 12, 2, 51, 841000)  # the naive-UTC read shape


def test_loader_values_are_tz_aware_utc_whatever_the_input_shape() -> None:
    exp = Exposure(
        exposure_id="e-1",
        event_time=T_NAIVE,
        ingest_time=T_NAIVE.replace(tzinfo=timezone(timedelta(hours=-6))),
        campaign_id="c",
        household_id="h",
        ip="1",
        app_id="a",
        program_genre="g",
        spend=1.0,
    )
    vals = _exposure_values(exp)
    assert vals[1] == T_NAIVE.replace(tzinfo=UTC)  # naive → taken as UTC, not local
    assert vals[2] == datetime(2026, 8, 8, 18, 2, 51, 841000, tzinfo=UTC)  # −6h → UTC
    assert all(v.tzinfo is not None for v in (vals[1], vals[2]))

    row = AttributedConversion(
        **exp.model_dump(include={"ip"}),
        conversion_id="c-1",
        event_time=T_NAIVE,
        ingest_time=T_NAIVE,
        device_id="d",
        conversion_type="purchase",
        revenue=1.0,
        order_id=None,
        household_id="h",
        resolution="device",
        ambiguous=False,
        candidate_count=1,
        exposure_id=None,
        assists=[],
        attributed=False,
        path="hot",
        processed_at=T_NAIVE,
        reason="state_miss",
    )
    vals = _attributed_values(row)
    for i in (1, 2, 16):  # event_time, ingest_time, processed_at
        assert vals[i] == T_NAIVE.replace(tzinfo=UTC)


class _CapturingClient:
    """A fake ClickHouse client that records the settings each insert is given."""

    def __init__(self) -> None:
        self.settings_seen: list[Any] = []

    def insert(self, table: str, data: Any, column_names: Any, settings: Any) -> None:
        self.settings_seen.append(settings)


def _one_exposure() -> Exposure:
    return Exposure(
        exposure_id="e-1",
        event_time=T_NAIVE,
        ingest_time=T_NAIVE,
        campaign_id="c",
        household_id="h",
        ip="1",
        app_id="a",
        program_genre="g",
        spend=1.0,
    )


def test_async_flag_defaults_off_and_make_run_enables_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OFF unless LAKE_ASYNC_INSERT=1 (set by `make run`). The mutation sentinel:
    an `invert-guard` on `_async_insert_enabled` flips the default and is KILLED."""
    monkeypatch.delenv("LAKE_ASYNC_INSERT", raising=False)
    assert _async_insert_enabled() is False
    monkeypatch.setenv("LAKE_ASYNC_INSERT", "1")
    assert _async_insert_enabled() is True
    monkeypatch.setenv("LAKE_ASYNC_INSERT", "0")
    assert _async_insert_enabled() is False


def test_the_insert_settings_carry_async_insert_and_wait() -> None:
    """Enabled → both async_insert and wait_for_async_insert; disabled → no
    settings. `wait_for_async_insert=1` is what keeps the read-after-load race out
    (Invariant 2), so it must always accompany async_insert."""
    assert _async_settings(True) == {"async_insert": 1, "wait_for_async_insert": 1}
    assert _async_settings(False) == {}

    # The setting is actually threaded to client.insert (None when disabled).
    client_on = _CapturingClient()
    insert_exposures(client_on, [_one_exposure()], async_insert=True)
    assert client_on.settings_seen == [{"async_insert": 1, "wait_for_async_insert": 1}]

    client_off = _CapturingClient()
    insert_exposures(client_off, [_one_exposure()], async_insert=False)
    assert client_off.settings_seen == [None]
