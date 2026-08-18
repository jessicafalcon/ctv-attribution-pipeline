"""Attribution leaves on hand-built input — last-touch, assists, window edge,
unattributed, the conversion_id-keyed reduction, and the resent-exposure
dedup of assists. No services."""

from datetime import UTC, datetime, timedelta

from producer.models import Exposure, ResolvedConversion
from streaming.attribute import HOT_WINDOW, attribute, attribute_household

T = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
H = timedelta(hours=1)


def _exp(eid: str, hh: str, t: datetime) -> Exposure:
    return Exposure(
        exposure_id=eid,
        event_time=t,
        ingest_time=t,
        campaign_id="camp-01",
        household_id=hh,
        ip="10.0.0.1",
        app_id="app-01",
        program_genre="drama",
        spend=0.1,
    )


def _res(
    cid: str,
    hh: str,
    t: datetime,
    *,
    ambiguous: bool = False,
    candidate_count: int = 1,
    ingest: datetime | None = None,
) -> ResolvedConversion:
    return ResolvedConversion(
        conversion_id=cid,
        event_time=t,
        ingest_time=ingest or t,
        device_id="d-1",
        ip="10.0.0.1",
        conversion_type="site_visit",
        revenue=0.0,
        order_id=None,
        household_id=hh,
        resolution="ip" if ambiguous else "device",
        ambiguous=ambiguous,
        candidate_count=candidate_count,
    )


# ---- stage 1: attribute_household -------------------------------------------


def test_last_touch_credits_latest_exposure_rest_assist() -> None:
    exps = [
        _exp("e-1", "h", T - 3 * H),
        _exp("e-2", "h", T - 2 * H),
        _exp("e-3", "h", T - H),
    ]
    (c,) = attribute_household(exps, [_res("c-1", "h", T)], HOT_WINDOW)
    assert c.row.attributed and c.row.exposure_id == "e-3"
    assert c.row.assists == ["e-1", "e-2"]
    assert c.last_touch_time == T - H


def test_last_touch_tiebreak_by_exposure_id() -> None:
    # Same event_time → higher exposure_id wins (deterministic total order).
    exps = [_exp("e-1", "h", T - H), _exp("e-2", "h", T - H)]
    (c,) = attribute_household(exps, [_res("c-1", "h", T)], HOT_WINDOW)
    assert c.row.exposure_id == "e-2" and c.row.assists == ["e-1"]


def test_window_boundaries_inclusive_lower_and_conversion() -> None:
    exps = [
        _exp("e-old", "h", T - HOT_WINDOW - timedelta(seconds=1)),  # just too old
        _exp("e-lo", "h", T - HOT_WINDOW),  # exactly at the window edge → in
        _exp("e-now", "h", T),  # exactly at the conversion → in
        _exp("e-future", "h", T + timedelta(seconds=1)),  # after conversion → out
    ]
    (c,) = attribute_household(exps, [_res("c-1", "h", T)], HOT_WINDOW)
    assert c.row.exposure_id == "e-now"  # latest eligible
    assert c.row.assists == ["e-lo"]  # e-old and e-future excluded


def test_unattributed_when_no_eligible_exposure() -> None:
    (c,) = attribute_household([], [_res("c-1", "h", T)], HOT_WINDOW)
    assert c.row.attributed is False
    assert c.row.exposure_id is None and c.row.assists == []
    assert c.last_touch_time is None


def test_assists_distinct_over_resent_exposure() -> None:
    # e-1 is resent (same id twice in eligible); it appears once in assists.
    exps = [
        _exp("e-1", "h", T - 2 * H),
        _exp("e-1", "h", T - 2 * H),
        _exp("e-2", "h", T - H),
    ]
    (c,) = attribute_household(exps, [_res("c-1", "h", T)], HOT_WINDOW)
    assert c.row.exposure_id == "e-2" and c.row.assists == ["e-1"]


def test_resent_last_touch_absent_from_own_assists() -> None:
    # The credited last-touch is itself resent — set difference keeps it out of
    # its own assists (the object-removal form would have leaked the twin's id).
    exps = [_exp("e-1", "h", T - H), _exp("e-2", "h", T), _exp("e-2", "h", T)]
    (c,) = attribute_household(exps, [_res("c-1", "h", T)], HOT_WINDOW)
    assert c.row.exposure_id == "e-2"
    assert c.row.assists == ["e-1"] and "e-2" not in c.row.assists


# ---- stage 2: the conversion_id-keyed reduction (via attribute) -------------


def test_reduction_keeps_most_recent_last_touch_across_households() -> None:
    exps = [_exp("e-a", "h-a", T - 2 * H), _exp("e-b", "h-b", T - H)]
    fanout = [
        _res("c-1", "h-a", T, ambiguous=True, candidate_count=2),
        _res("c-1", "h-b", T, ambiguous=True, candidate_count=2),
    ]
    (row,) = attribute(exps, fanout)
    assert row.household_id == "h-b" and row.exposure_id == "e-b"


def test_reduction_attributed_beats_unattributed() -> None:
    exps = [_exp("e-a", "h-a", T - H)]  # only h-a has an exposure
    fanout = [
        _res("c-1", "h-a", T, ambiguous=True, candidate_count=2),
        _res("c-1", "h-b", T, ambiguous=True, candidate_count=2),
    ]
    (row,) = attribute(exps, fanout)
    assert row.household_id == "h-a" and row.attributed


def test_reduction_all_unattributed_keeps_lowest_household() -> None:
    fanout = [
        _res("c-1", "h-b", T, ambiguous=True, candidate_count=2),
        _res("c-1", "h-a", T, ambiguous=True, candidate_count=2),
    ]
    (row,) = attribute([], fanout)
    assert row.household_id == "h-a" and row.attributed is False


def test_duplicate_resolved_row_collapses_to_one() -> None:
    res = _res("c-1", "h", T)
    out = attribute([_exp("e-1", "h", T - H)], [res, res])  # resend duplicate
    assert len(out) == 1 and out[0].exposure_id == "e-1"


def test_processed_at_is_ingest_time_and_path_hot() -> None:
    (row,) = attribute([_exp("e-1", "h", T - H)], [_res("c-1", "h", T, ingest=T + H)])
    assert row.processed_at == T + H and row.path == "hot"


def test_output_sorted_by_conversion_id() -> None:
    exps = [_exp("e-1", "h", T - H)]
    out = attribute(exps, [_res("c-2", "h", T), _res("c-1", "h", T)])
    assert [r.conversion_id for r in out] == ["c-1", "c-2"]
