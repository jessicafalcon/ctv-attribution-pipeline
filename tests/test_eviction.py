"""Feature 3 — hot-window eviction. An exposure is evicted once the watermark
passes its `event_time + 7d` (strict `>`); release runs before eviction each
tick, so a conversion at the exact boundary still probes its exposure. Eviction
bounds join-state (the scaling constraint) without dropping any exposure an
in-tolerance conversion could still match. Pure, no services.

The one anti-pattern tiny cannot catch (it never evicts, span < 7d): evict
before release, or evict on `>=`, drops a boundary exposure — caught here and by
medium parity, not gate 0."""

from datetime import UTC, datetime, timedelta

from producer.models import Exposure, ResolvedConversion
from streaming import metrics
from streaming.attribute import HOT_WINDOW, StreamState, attribute_household_streaming

BASE = datetime(2026, 8, 1, tzinfo=UTC)
DAY = timedelta(days=1)


def exp(eid: str, event: datetime, ingest: datetime) -> Exposure:
    return Exposure(
        exposure_id=eid,
        event_time=event,
        ingest_time=ingest,
        campaign_id="camp-00",
        household_id="h-0",
        ip="10.0.0.1",
        app_id="app-01",
        program_genre="news",
        spend=0.05,
    )


def conv(cid: str, event: datetime, ingest: datetime) -> ResolvedConversion:
    return ResolvedConversion(
        conversion_id=cid,
        event_time=event,
        ingest_time=ingest,
        device_id="d-0",
        ip="10.0.0.1",
        conversion_type="site_visit",
        revenue=0.0,
        order_id=None,
        household_id="h-0",
        resolution="device",
        ambiguous=False,
        candidate_count=1,
    )


def test_exposure_evicted_only_strictly_past_the_window() -> None:
    # allowed_lateness 0 → watermark = max(event_time). At exactly event+7d the
    # exposure is kept (strict >); one minute past, it is evicted.
    e = exp("e-A", BASE, BASE)
    at_bound = exp("e-B", BASE + 7 * DAY, BASE + 7 * DAY)
    kept = attribute_household_streaming(
        [("exp", e), ("exp", at_bound)], HOT_WINDOW, timedelta(0)
    )
    assert kept.state.evicted == 0

    past = exp("e-C", BASE + 7 * DAY + timedelta(minutes=1), BASE + 7 * DAY)
    gone = attribute_household_streaming(
        [("exp", e), ("exp", past)], HOT_WINDOW, timedelta(0)
    )
    assert gone.state.evicted == 1


def test_release_before_eviction_saves_a_still_needed_exposure() -> None:
    # c-C is eligible for e-A and releases in the same tick e-A becomes evictable
    # (the later e-X advances the watermark past e-A.event + 7d). Release-first +
    # strict-> keep e-A present when c-C probes; evict-first would drop it and
    # leave c-C wrongly unattributed. This is the ordering guard.
    e = exp("e-A", BASE, BASE)
    c = conv("c-C", BASE + 6 * DAY, BASE + DAY)
    x = exp("e-X", BASE + 8 * DAY, BASE + 2 * DAY)
    res = attribute_household_streaming(
        [("exp", e), ("res", c), ("exp", x)], HOT_WINDOW, timedelta(minutes=30)
    )
    [row] = [cd.row for cd in res.candidates]
    assert row.attributed and row.exposure_id == "e-A"
    assert res.state.evicted == 1  # e-A evicted, but only AFTER c-C attributed


def test_beyond_tolerance_conversion_is_unattributed_state_miss() -> None:
    # A conversion arriving far beyond allowed_lateness: its exposure has already
    # aged out when it is processed → unattributed (a state-miss), handed to
    # reconciliation (Phase 6). This is the F3 "beyond-tolerance → unattributed"
    # case; medium keeps late ≤ allowed_lateness so it never happens there.
    e = exp("e-A", BASE, BASE)
    filler = exp("e-F", BASE + 8 * DAY, BASE + 8 * DAY)
    late = conv("c-L", BASE + timedelta(hours=1), BASE + 9 * DAY)
    res = attribute_household_streaming(
        [("exp", e), ("exp", filler), ("res", late)], HOT_WINDOW, timedelta(hours=6)
    )
    [row] = [cd.row for cd in res.candidates]
    assert not row.attributed and row.exposure_id is None
    assert res.state.evicted >= 1


def test_join_state_rises_then_falls() -> None:
    # Exposures accumulate, then a late event advances the watermark past their
    # window → peak > final and evicted > 0 (clause 2: state rose and fell).
    early = [
        exp(f"e-{i}", BASE + timedelta(hours=i), BASE + timedelta(hours=i))
        for i in range(5)
    ]
    tail = exp("e-tail", BASE + 10 * DAY, BASE + 10 * DAY)
    res = attribute_household_streaming(
        [("exp", e) for e in early] + [("exp", tail)], HOT_WINDOW, timedelta(0)
    )
    assert res.state.peak >= 5
    assert res.state.evicted == 5
    assert res.state.final < res.state.peak


def test_observe_state_metrics_are_evicted_sum_and_peak_high_water() -> None:
    metrics.EXPOSURES_EVICTED._value.set(0)
    metrics.JOIN_STATE_SIZE.set(0)
    metrics.observe_state(StreamState(peak=7, evicted=3, final=2))
    metrics.observe_state(StreamState(peak=4, evicted=1, final=1))  # lower peak
    assert metrics.EXPOSURES_EVICTED._value.get() == 4  # summed across households
    assert metrics.JOIN_STATE_SIZE._value.get() == 7  # high-water, not overwritten
    metrics.EXPOSURES_EVICTED._value.set(0)
    metrics.JOIN_STATE_SIZE.set(0)
