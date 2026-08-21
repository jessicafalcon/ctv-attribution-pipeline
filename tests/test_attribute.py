"""Attribution leaves on hand-built input — last-touch, assists, window edge,
unattributed, the hot-path ambiguous_ip deferral (Phase 16: a shared-IP
conversion is never guessed hot), one-row-per-conversion collapse, and the
resent-exposure dedup of assists. No services."""

from datetime import UTC, datetime, timedelta

import pytest

from producer.models import Exposure, ResolvedConversion
from streaming.attribute import (
    HOT_WINDOW,
    attribute,
    attribute_household,
    last_touch,
    one_row_per_conversion,
)

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
    assert c.row.reason is None
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
    assert c.row.attributed is False and c.row.reason == "state_miss"
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


# ---- the hot-path ambiguity rule (Phase 16) ----------------------------------


def test_ambiguous_conversion_is_deferred_not_guessed() -> None:
    # Both candidate households have an in-window exposure; the old reduce would
    # have credited h-b (most recent). The hot path now emits ONE unattributed
    # placeholder (lowest household_id) and leaves the pick to reconciliation.
    exps = [_exp("e-a", "h-a", T - 2 * H), _exp("e-b", "h-b", T - H)]
    fanout = [
        _res("c-1", "h-b", T, ambiguous=True, candidate_count=2),
        _res("c-1", "h-a", T, ambiguous=True, candidate_count=2),
    ]
    (row,) = attribute(exps, fanout)
    assert row.attributed is False and row.exposure_id is None
    assert row.household_id == "h-a"  # placeholder: lowest household_id
    assert row.ambiguous and row.candidate_count == 2
    assert row.reason == "ambiguous_ip"
    assert row.path == "hot" and row.processed_at == T
    assert row.candidate_households == ["h-a", "h-b"]  # the full set, sorted


def test_ambiguous_never_probes_state_even_with_one_exposure() -> None:
    exps = [_exp("e-a", "h-a", T - H)]
    conv = _res("c-1", "h-a", T, ambiguous=True, candidate_count=2)
    (c,) = attribute_household(exps, [conv], HOT_WINDOW, {"c-1": ["h-a", "h-b"]})
    assert c.row.attributed is False and c.last_touch_time is None
    assert c.row.candidate_households == ["h-a", "h-b"]  # persisted for reconcile
    # Without its candidate set a deferred row could never be reconciled → refused.
    with pytest.raises(ValueError, match="no candidate_households"):
        attribute_household(exps, [conv], HOT_WINDOW)


def test_mixed_topic_fanout_is_refused_with_a_reseed_message() -> None:
    # Two profiles' events under shared conversion_ids: dedup collapses a fan-out
    # pair and an ambiguous row arrives with fewer candidates than its count. The
    # validator must name the real fix (re-seed a clean broker), not
    # "re-run the engine" (review gate, functionality-tester F3).
    with pytest.raises(ValueError, match="MIXED topic.*re-seed"):
        attribute_household(
            [],
            [_res("c-1", "h-a", T, ambiguous=True, candidate_count=2)],
            HOT_WINDOW,
            {"c-1": ["h-a"]},
        )


def test_last_touch_leaf_is_ambiguity_blind() -> None:
    # Reconciliation scores each candidate household with the leaf directly.
    exps = [_exp("e-a", "h-a", T - H)]
    c = last_touch(
        exps,
        _res("c-1", "h-a", T, ambiguous=True, candidate_count=2),
        HOT_WINDOW,
        ["h-a", "h-b"],
    )
    assert c.row.attributed and c.row.exposure_id == "e-a"


def test_single_candidate_ip_fallback_attributes_as_before() -> None:
    conv = _res("c-1", "h", T)
    conv = conv.model_copy(update={"resolution": "ip"})  # unique-IP fallback
    (row,) = attribute([_exp("e-1", "h", T - H)], [conv])
    assert row.attributed and row.exposure_id == "e-1"


def test_one_row_per_conversion_collapses_fanout_to_lowest_household() -> None:
    rows = [
        _res("c-2", "h-z", T, ambiguous=True, candidate_count=3),
        _res("c-1", "h-b", T, ambiguous=True, candidate_count=2),
        _res("c-1", "h-a", T, ambiguous=True, candidate_count=2),
        _res("c-2", "h-a", T, ambiguous=True, candidate_count=3),
        _res("c-3", "h-q", T),
    ]
    out = one_row_per_conversion(rows)
    assert [(r.conversion_id, r.household_id) for r in out] == [
        ("c-2", "h-a"),  # first-arrival slot kept, lowest household chosen
        ("c-1", "h-a"),
        ("c-3", "h-q"),
    ]


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
