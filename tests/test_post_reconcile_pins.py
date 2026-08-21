"""Phase 16's central constraint, asserted offline: SAME ANSWER AFTER
RECONCILIATION. The hot path defers every shared-IP (ambiguous_ip) conversion; the
reconcile pass — candidate households re-enumerated from the device graph, the one
most-recent-exposure tiebreak — credits them. tiny and medium post-reconcile must
equal their pre-Phase-16 hot numbers (52/35/35, 130/92/92); shared_ip_spike must
reproduce the deleted reduce's pick (69/80, 11 wrong-household). No services:
generate → resolve → dedup → evicting engine → expand → reconcile over the same
exposures, scored against truth (tests/pins.py)."""

import pytest

from accuracy.score import score
from producer.config import load_profile
from producer.generate import generate
from reconcile.reconcile import (
    LONG_WINDOW,
    expand_candidates,
    reconcile,
    reconciled_at_for,
)
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream
from streaming.attribute import ALLOWED_LATENESS, dedup_streams
from streaming.dataflow import run_attribution
from tests.pins import (
    MEDIUM_HOT,
    MEDIUM_POST,
    SHARED_IP_HOT,
    SHARED_IP_POST,
    SHARED_IP_POST_WRONG_HOUSEHOLD,
    TINY_HOT,
    TINY_POST,
)


def hot_and_post(name: str):
    """(hot report, post-reconcile report, recovered rows) for a profile."""
    p = load_profile(name)
    s = generate(p, p.seed)
    graph = GraphIndex.from_households(s.graph.households)
    exps, res, _ = dedup_streams(s.exposures, resolve_stream(s.conversions, graph))
    hot = run_attribution(exps, res, ALLOWED_LATENESS)
    truth = {t.conversion_id: t.truth_exposure_id for t in s.truth_links}
    exp_hh = {e.exposure_id: e.household_id for e in exps}

    def _score(rows):
        credited = {
            r.conversion_id: (r.household_id, r.exposure_id)
            for r in rows
            if r.attributed
        }
        return score(credited, truth, exp_hh, name)

    by_hh: dict[str, list] = {}
    for e in exps:
        by_hh.setdefault(e.household_id, []).append(e)
    candidates = [r for r in hot if not r.attributed]
    recovered = reconcile(
        expand_candidates(candidates, graph),
        by_hh,
        LONG_WINDOW,
        reconciled_at_for(max(e.ingest_time for e in exps)),
    )
    post = {r.conversion_id: r for r in hot}
    post.update({r.conversion_id: r for r in recovered})
    return _score(hot), _score(post.values()), recovered


def _counts(r):
    return (r.credited, r.truth_links, r.household_correct)


def _pin(p):
    return (p.credited, p.truth, p.correct)


@pytest.mark.parametrize(
    "name,hot_pin,post_pin,deferred",
    [("tiny", TINY_HOT, TINY_POST, 5), ("medium", MEDIUM_HOT, MEDIUM_POST, 1)],
)
def test_clean_profiles_post_reconcile_equal_the_pre_phase16_hot_numbers(
    name, hot_pin, post_pin, deferred
) -> None:
    hot, post, recovered = hot_and_post(name)
    assert _counts(hot) == _pin(hot_pin)
    assert hot.caused_wrong_household == 0
    assert _counts(post) == _pin(post_pin)
    assert post.caused_wrong_household == 0 and post.caused_missed == 0
    # Every recovery is a deferred shared-IP conversion; state-misses stay missed.
    assert len(recovered) == deferred
    assert all(r.path == "reconciled" and r.reason is None for r in recovered)


def test_shared_ip_spike_post_reconcile_reproduces_the_deleted_reduce() -> None:
    hot, post, recovered = hot_and_post("shared_ip_spike")
    assert _counts(hot) == _pin(SHARED_IP_HOT)
    assert hot.caused_wrong_household == 0  # never guessed hot
    assert hot.caused_missed == 19  # all deferred, none lost
    assert _counts(post) == _pin(SHARED_IP_POST)
    assert post.caused_wrong_household == SHARED_IP_POST_WRONG_HOUSEHOLD
    assert post.caused_missed == 0
    assert len(recovered) == 25  # 19 caused + 6 organic ambiguous
