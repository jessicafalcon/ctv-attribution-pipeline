"""Phase-12 canary: an Exposure survives the Iceberg round-trip bit-for-bit.

The whole phase rests on lake-sourced reconcile == ClickHouse-sourced reconcile.
exposures_landed is DateTime64(3,'UTC'); the lake path must reconstruct the same
UTC instant to the millisecond or the window-match diverges. This test is the
fidelity gate — it runs BEFORE reconcile is wired to the lake (ARCHITECTURE §8
timezone gotcha; briefing #3). Offline: local Iceberg + DuckDB, no services.
"""

from datetime import UTC, datetime, timedelta

import pytest

from lake.land_exposures import _to_utc_ms, land
from lake.read_exposures import read_exposures_by_household
from producer.models import Exposure


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))


def _exposure(i: int, hh: str, when: datetime) -> Exposure:
    return Exposure(
        exposure_id=f"e-{i:06d}",
        event_time=when,
        ingest_time=when + timedelta(minutes=5),
        campaign_id=f"c-{i % 3:06d}",
        household_id=hh,
        ip="10.0.0.1",
        app_id="app-x",
        program_genre="sports",
        spend=12.5,
    )


def test_exposure_round_trips_field_by_field_to_the_ms() -> None:
    base = datetime(2026, 8, 1, 12, 0, 0, 123_000, tzinfo=UTC)  # tz-aware UTC, ms
    original = _exposure(0, "hh-1", base)
    land([original])

    got = read_exposures_by_household({"hh-1"})
    assert set(got) == {"hh-1"}
    assert len(got["hh-1"]) == 1
    back = got["hh-1"][0]

    # Every field identical; timestamps compared as UTC instants to the ms.
    assert back.exposure_id == original.exposure_id
    assert back.campaign_id == original.campaign_id
    assert back.household_id == original.household_id
    assert back.ip == original.ip
    assert back.app_id == original.app_id
    assert back.program_genre == original.program_genre
    assert back.spend == original.spend
    assert back.event_time == _to_utc_ms(original.event_time)
    assert back.ingest_time == _to_utc_ms(original.ingest_time)
    # tz-aware UTC on the way back (never naive, never local-tz).
    assert back.event_time.utcoffset() == timedelta(0)


def test_sub_ms_input_is_truncated_to_match_datetime64_3() -> None:
    # A stray sub-ms datetime must land at ms granularity (matching DateTime64(3)),
    # so the lake never carries precision the ClickHouse copy dropped.
    noisy = datetime(2026, 8, 1, 12, 0, 0, 123_456, tzinfo=UTC)
    land([_exposure(1, "hh-2", noisy)])
    back = read_exposures_by_household({"hh-2"})["hh-2"][0]
    assert back.event_time == datetime(2026, 8, 1, 12, 0, 0, 123_000, tzinfo=UTC)


def test_re_landing_is_idempotent_on_read() -> None:
    # Iceberg append accumulates, but distinct-on-exposure_id collapses the
    # re-sends to one row (invariant: exposure_id unique) — the exposures_landed
    # FINAL parity that reconcile depends on.
    e = _exposure(2, "hh-3", datetime(2026, 8, 2, 0, 0, 0, tzinfo=UTC))
    land([e])
    land([e])
    land([e])
    got = read_exposures_by_household({"hh-3"})
    assert len(got["hh-3"]) == 1


def test_day_partition_prune_drops_out_of_window_days() -> None:
    # min_event_time prunes older day-partitions; a row before the bound is not
    # returned (it would be discarded by the leaf anyway — output-invariant).
    old = _exposure(3, "hh-4", datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC))
    recent = _exposure(4, "hh-4", datetime(2026, 8, 1, 0, 0, 0, tzinfo=UTC))
    land([old, recent])
    got = read_exposures_by_household(
        {"hh-4"}, min_event_time=datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)
    )
    ids = {e.exposure_id for e in got["hh-4"]}
    assert ids == {"e-000004"}
