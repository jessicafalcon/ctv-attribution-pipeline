"""Feature 2 — watermarks + allowed-lateness release, no eviction yet. A
conversion is buffered until the watermark (max event_time − allowed_lateness)
reaches its event_time, so it always attributes against the COMPLETE eligible
set even when a matching exposure arrives after it. Pure, no services.

Release-gating changes WHEN a conversion attributes, never WHETHER: without
eviction a late conversion still probes intact state and attributes. The
"beyond-tolerance → unattributed" state-miss needs eviction and lands in
feature 3, not here."""

from datetime import UTC, datetime, timedelta

from producer.models import Exposure, ResolvedConversion
from streaming.attribute import (
    HOT_WINDOW,
    attribute_household,
    attribute_household_streaming,
    event_time_watermark,
)

T0 = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)


def _min(m: int) -> datetime:
    return T0 + timedelta(minutes=m)


def exp(eid: str, event_min: int, ingest_min: int, hh: str = "h-0") -> Exposure:
    return Exposure(
        exposure_id=eid,
        event_time=_min(event_min),
        ingest_time=_min(ingest_min),
        campaign_id="camp-00",
        household_id=hh,
        ip="10.0.0.1",
        app_id="app-01",
        program_genre="news",
        spend=0.05,
    )


def conv(
    cid: str, event_min: int, ingest_min: int, hh: str = "h-0"
) -> ResolvedConversion:
    return ResolvedConversion(
        conversion_id=cid,
        event_time=_min(event_min),
        ingest_time=_min(ingest_min),
        device_id="d-0",
        ip="10.0.0.1",
        conversion_type="site_visit",
        revenue=0.0,
        order_id=None,
        household_id=hh,
        resolution="device",
        ambiguous=False,
        candidate_count=1,
    )


def _rows(result):
    return [c.row for c in result.candidates]


def test_watermark_event_time_only_and_monotonic() -> None:
    lat = timedelta(hours=1)
    wm = event_time_watermark(None, _min(120), lat)  # 12:00 − 1h → 11:00
    assert wm == _min(60)
    assert event_time_watermark(wm, T0, lat) == wm  # older event cannot pull back
    assert event_time_watermark(wm, _min(240), lat) == _min(180)  # newer advances


def test_out_of_order_last_touch_recovered_by_release() -> None:
    # e-B is the true last-touch but arrives (ingest 13:30) AFTER the conversion
    # (ingest 12:05). A per-probe pass would credit e-A; release-gating waits for
    # completeness and credits e-B, recording e-A as the assist.
    a = exp("e-A", event_min=0, ingest_min=1)
    b = exp("e-B", event_min=60, ingest_min=210)
    c = conv("c-C", event_min=120, ingest_min=125)
    assert a.ingest_time < c.ingest_time < b.ingest_time  # b truly out of order
    arrival = [("exp", a), ("res", c), ("exp", b)]

    [row] = _rows(
        attribute_household_streaming(arrival, HOT_WINDOW, timedelta(hours=3))
    )
    assert row.attributed and row.exposure_id == "e-B"
    assert row.assists == ["e-A"]


def test_mid_stream_release_when_a_later_event_advances_the_watermark() -> None:
    # A later exposure pushes the watermark past c-C's event_time + lateness, so
    # c-C releases mid-stream (not only at EOF); e-Z is not eligible (after c-C).
    a = exp("e-A", event_min=0, ingest_min=1)
    c = conv("c-C", event_min=30, ingest_min=31)
    z = exp("e-Z", event_min=120, ingest_min=121)
    arrival = [("exp", a), ("res", c), ("exp", z)]

    [row] = _rows(
        attribute_household_streaming(arrival, HOT_WINDOW, timedelta(hours=1))
    )
    assert row.attributed and row.exposure_id == "e-A" and row.assists == []


def test_late_conversion_within_tolerance_still_attributes() -> None:
    # A conversion arriving 2h30m late is buffered, then attributed — never
    # dropped by a conversion-side gate (the invariant). Its exposure is intact.
    a = exp("e-A", event_min=0, ingest_min=1)
    c = conv("c-C", event_min=30, ingest_min=180)
    [row] = _rows(
        attribute_household_streaming(
            [("exp", a), ("res", c)], HOT_WINDOW, timedelta(hours=3)
        )
    )
    assert row.attributed and row.exposure_id == "e-A"


def test_streaming_equals_complete_state_core() -> None:
    # The non-evicting streaming pass returns exactly what the complete-state
    # core returns over the whole household — order-independent (this is the
    # per-household form of gate 0).
    exps = [exp("e-A", 0, 1), exp("e-B", 60, 210), exp("e-C", 90, 95)]
    convs = [conv("c-1", 120, 125), conv("c-2", 30, 400)]
    events = [("exp", e) for e in exps] + [("res", c) for c in convs]

    def digest(rows):
        return sorted(
            (r.conversion_id, r.exposure_id, tuple(r.assists), r.attributed)
            for r in rows
        )

    stream = _rows(
        attribute_household_streaming(events, HOT_WINDOW, timedelta(hours=12))
    )
    core = [c.row for c in attribute_household(exps, convs, HOT_WINDOW)]
    assert digest(stream) == digest(core)
