"""Phase-6 reconciliation — pure matcher + version derivation, no services.

`reconcile()` reuses the hot engine's last-touch leaf at a 90d window, so a
conversion whose causing exposure is >7d back (hot-missed) but ≤90d back is
recovered; one with no in-90d exposure is left alone; recovered rows are stamped
path=reconciled with a version strictly above the hot row's; a second pass is a
no-op. The DB-side "only hot-unattributed rows are candidates" WHERE and the
rollup/restatement live behavior are proven in tests/integration/test_reconcile.py.
"""

from datetime import UTC, datetime, timedelta

import pytest

from accuracy.score import score
from producer.config import load_profile
from producer.generate import generate
from producer.models import Exposure, Household, ResolvedConversion
from reconcile.reconcile import (
    LONG_WINDOW,
    RECONCILE_DELTA,
    expand_candidates,
    pick_household,
    reconcile,
    reconciled_at_for,
)
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream
from streaming.attribute import HOT_WINDOW, attribute, dedup_streams, last_touch

T0 = datetime(2026, 8, 1, tzinfo=UTC)


def _exposure(eid: str, household: str, day: float) -> Exposure:
    t = T0 + timedelta(days=day)
    return Exposure(
        exposure_id=eid,
        event_time=t,
        ingest_time=t,
        campaign_id="camp-00",
        household_id=household,
        ip="10.0.0.1",
        app_id="app-01",
        program_genre="news",
        spend=0.05,
    )


def _candidate(cid: str, household: str, day: float) -> ResolvedConversion:
    t = T0 + timedelta(days=day)
    return ResolvedConversion(
        conversion_id=cid,
        event_time=t,
        ingest_time=t,
        device_id="d-1",
        ip="10.0.0.1",
        conversion_type="purchase",
        revenue=42.0,
        order_id=f"o-{cid}",
        household_id=household,
        resolution="device",
        ambiguous=False,
        candidate_count=1,
    )


def test_long_delay_miss_is_recovered_over_90d() -> None:
    # Exposure 20 days before the conversion: outside the 7d hot window (a genuine
    # hot miss), inside the 90d long window (recoverable).
    exp = _exposure("e-1", "H1", day=0)
    conv = _candidate("c-1", "H1", day=20)
    assert conv.event_time - exp.event_time > HOT_WINDOW  # was a hot miss
    at = reconciled_at_for(T0 + timedelta(days=25))

    out = reconcile([conv], {"H1": [exp]}, LONG_WINDOW, at)

    assert len(out) == 1
    row = out[0]
    assert row.conversion_id == "c-1"
    assert row.attributed is True
    assert row.exposure_id == "e-1"
    assert row.path == "reconciled"
    assert row.processed_at == at
    assert row.household_id == "H1"


def test_candidate_with_no_in_window_exposure_is_left_alone() -> None:
    # No exposure in the household at all → not recovered, and NOT emitted (it
    # stays as its hot unattributed row). This is what makes a second pass a no-op.
    conv = _candidate("c-2", "H2", day=20)
    out = reconcile([conv], {}, LONG_WINDOW, reconciled_at_for(T0))
    assert out == []


def test_exposure_older_than_90d_is_not_recovered() -> None:
    # >90d back: outside the long window too → permanently unattributed here.
    exp = _exposure("e-3", "H3", day=0)
    conv = _candidate("c-3", "H3", day=120)
    out = reconcile([conv], {"H3": [exp]}, LONG_WINDOW, reconciled_at_for(T0))
    assert out == []


def test_second_pass_is_a_no_op() -> None:
    exp = _exposure("e-1", "H1", day=0)
    conv = _candidate("c-1", "H1", day=20)
    at = reconciled_at_for(T0 + timedelta(days=25))
    first = reconcile([conv], {"H1": [exp]}, LONG_WINDOW, at)
    # A real second pass re-selects only still-unattributed rows; the recovered
    # one is no longer a candidate, so it is not passed in again → nothing new.
    second = reconcile([], {"H1": [exp]}, LONG_WINDOW, at)
    assert second == []
    # And re-running the SAME input is deterministic (idempotent rows).
    again = reconcile([conv], {"H1": [exp]}, LONG_WINDOW, at)
    assert [r.model_dump() for r in again] == [r.model_dump() for r in first]


def test_observe_restatement_sets_the_gauge_once_per_pass() -> None:
    # The gauge is overwritten each pass (not cumulative); the load-bearing SQL
    # (_restatement_abs_delta) is proven live in tests/integration/test_reconcile.py
    # and flows into the promtool fixture via make metrics-capture.
    from reconcile import metrics

    metrics.observe_restatement(1.25)
    assert metrics.RESTATEMENT_ROAS_ABS_DELTA._value.get() == 1.25
    metrics.observe_restatement(0.0)
    assert metrics.RESTATEMENT_ROAS_ABS_DELTA._value.get() == 0.0


def test_reconciled_at_is_strictly_after_the_base_by_the_delta() -> None:
    base = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
    at = reconciled_at_for(base)
    assert at == base + RECONCILE_DELTA
    assert at > base
    # A hot row's processed_at is a conversion's ingest_time, which is ≤ base
    # (base is the max ingest_time), so the reconciled version always wins RMT.
    hot_processed_at = base
    assert at > hot_processed_at


# ---- Phase 16: ambiguous_ip candidates are reconciliation's to pick -----------


def _ambiguous(cid: str, household: str, day: float, ip: str, n: int = 2):
    return _candidate(cid, household, day).model_copy(
        update={"ip": ip, "resolution": "ip", "ambiguous": True, "candidate_count": n}
    )


def _shared_ip_graph() -> GraphIndex:
    # H1 and H2 share 100.64.0.1; H3 owns 10.0.0.9 alone.
    return GraphIndex.from_households(
        [
            Household(household_id="H1", devices=[], ips=["100.64.0.1"]),
            Household(household_id="H2", devices=[], ips=["100.64.0.1"]),
            Household(household_id="H3", devices=[], ips=["10.0.0.9"]),
        ]
    )


def test_pick_household_keeps_most_recent_last_touch_then_exposure_then_hh():
    conv = _ambiguous("c-9", "H1", day=10, ip="100.64.0.1")
    older = last_touch([_exposure("e-a", "H1", day=5)], conv, LONG_WINDOW)
    newer = last_touch(
        [_exposure("e-b", "H2", day=6)],
        conv.model_copy(update={"household_id": "H2"}),
        LONG_WINDOW,
    )
    assert pick_household([older, newer]) is newer
    # Same event_time → exposure_id decides (a total order; household_id vestigial).
    tie_a = last_touch([_exposure("e-a", "H1", day=6)], conv, LONG_WINDOW)
    assert pick_household([tie_a, newer]) is newer  # "e-b" > "e-a"
    # Attributed beats unattributed; all unattributed → None (stays hot row).
    missed = last_touch([], conv, LONG_WINDOW)
    assert pick_household([missed, older]) is older
    assert pick_household([missed]) is None


def test_expand_candidates_re_enumerates_owners_from_the_graph():
    graph = _shared_ip_graph()
    placeholder = _ambiguous("c-1", "H1", day=10, ip="100.64.0.1")  # lowest hh
    plain = _candidate("c-2", "H3", day=10)
    out = expand_candidates([placeholder, plain], graph)
    assert [(r.conversion_id, r.household_id) for r in out] == [
        ("c-1", "H1"),
        ("c-1", "H2"),
        ("c-2", "H3"),
    ]
    assert all(r.candidate_count == 2 and r.ambiguous for r in out[:2])
    # An IP nobody owns any more expands to nothing (left as its hot row).
    assert (
        expand_candidates([_ambiguous("c-3", "H1", day=1, ip="0.0.0.0")], graph) == []
    )


def test_reconcile_refuses_an_unexpanded_ambiguous_candidate():
    with pytest.raises(ValueError, match="not expanded"):
        reconcile(
            [_ambiguous("c-1", "H1", day=10, ip="100.64.0.1")],
            {},
            LONG_WINDOW,
            reconciled_at_for(T0),
        )


def test_ambiguous_conversion_is_credited_to_the_most_recent_household():
    graph = _shared_ip_graph()
    at = reconciled_at_for(T0 + timedelta(days=30))
    expanded = expand_candidates(
        [_ambiguous("c-1", "H1", day=10, ip="100.64.0.1")], graph
    )
    exposures = {
        "H1": [_exposure("e-1", "H1", day=8.0)],
        "H2": [_exposure("e-2", "H2", day=9.5)],  # more recent → H2 wins
    }
    (row,) = reconcile(expanded, exposures, LONG_WINDOW, at)
    assert (row.household_id, row.exposure_id, row.path) == ("H2", "e-2", "reconciled")
    assert row.processed_at == at and row.ambiguous and row.candidate_count == 2
    assert row.reason is None  # credited → the deferral reason is cleared
    # No candidate household has an exposure → not recovered (idempotent no-op).
    assert reconcile(expanded, {}, LONG_WINDOW, at) == []


def test_shared_ip_spike_post_reconcile_is_at_least_as_correct_as_the_old_hot_guess():
    """The spec's central constraint on the fault profile: hot wrong-household is
    0 by construction; after the reconcile pass the shared-IP conversions are
    credited to the correct household at least as often as the old hot reduce
    managed (69/80). Offline: generate → resolve → dedup → hot oracle → expand →
    reconcile over the same exposures, scored against truth."""
    p = load_profile("shared_ip_spike")
    s = generate(p, p.seed)
    graph = GraphIndex.from_households(s.graph.households)
    exps, res, _ = dedup_streams(s.exposures, resolve_stream(s.conversions, graph))
    hot = attribute(exps, res)
    truth = {t.conversion_id: t.truth_exposure_id for t in s.truth_links}
    exp_hh = {e.exposure_id: e.household_id for e in exps}

    def _score(rows):
        credited = {
            r.conversion_id: (r.household_id, r.exposure_id)
            for r in rows
            if r.attributed
        }
        return score(credited, truth, exp_hh, "shared_ip_spike")

    hot_report = _score(hot)
    assert hot_report.caused_wrong_household == 0  # never guessed hot

    candidates = [r for r in hot if not r.attributed]
    by_hh: dict[str, list] = {}
    for e in exps:
        by_hh.setdefault(e.household_id, []).append(e)
    recovered = reconcile(
        expand_candidates(candidates, graph),
        by_hh,
        LONG_WINDOW,
        reconciled_at_for(max(e.ingest_time for e in exps)),
    )
    post = {r.conversion_id: r for r in hot}
    post.update({r.conversion_id: r for r in recovered})
    post_report = _score(post.values())

    assert post_report.caused_missed == 0  # every deferral was recovered
    assert post_report.household_correct >= 69  # ≥ the old hot reduce's 69/80
    assert post_report.caused_wrong_household <= 11  # ≤ the old hot reduce's 11
    assert all(r.path == "reconciled" for r in recovered)
    print(
        f"shared_ip_spike post-reconcile: correct {post_report.household_correct}/80, "
        f"wrong-household {post_report.caused_wrong_household}, "
        f"recovered {len(recovered)}"
    )
