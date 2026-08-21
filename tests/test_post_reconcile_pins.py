"""Phase 16's central constraint, asserted offline: SAME ANSWER AFTER
RECONCILIATION. The hot path defers every shared-IP (ambiguous_ip) conversion; the
reconcile pass — candidate households re-enumerated from the device graph, the one
most-recent-exposure tiebreak — credits them. tiny and medium post-reconcile must
equal their pre-Phase-16 hot numbers (52/35/35, 130/92/92); shared_ip_spike must
reproduce the deleted reduce's pick (69/80, 11 wrong-household). No services:
generate → resolve → dedup → evicting engine → expand → reconcile over the same
exposures, scored against truth (tests/pins.py).

Two drivers, on purpose: this file runs the EVICTING engine (`run_attribution`);
`tests/test_reconcile.py`'s shared_ip_spike proof runs the non-evicting oracle
(`attribute`). Both must land on the same pins — that is the Phase-5 parity claim
carried through reconciliation, not a duplicate test."""

import pytest

from accuracy.score import AccuracyReport, score
from producer.config import load_profile
from producer.generate import generate
from producer.models import AttributedConversion
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
    LONG_DELAY_HOT,
    LONG_DELAY_POST,
    MEDIUM_HOT,
    MEDIUM_POST,
    SHARED_IP_HOT,
    SHARED_IP_POST,
    SHARED_IP_POST_WRONG_HOUSEHOLD,
    TINY_HOT,
    TINY_POST,
    AccuracyPin,
)


def hot_and_post(
    name: str,
) -> tuple[AccuracyReport, AccuracyReport, list[AttributedConversion]]:
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


def _counts(r: AccuracyReport) -> tuple[int, int, int]:
    return (r.credited, r.truth_links, r.household_correct)


def _pin(p: AccuracyPin) -> tuple[int, int, int]:
    return (p.credited, p.truth, p.correct)


@pytest.mark.parametrize(
    "name,hot_pin,post_pin,recovered_n,post_wrong",
    [
        # tiny / medium: every recovery is a deferred shared-IP conversion (their
        # state-misses have no in-90d exposure); post == the pre-Phase-16 hot numbers.
        ("tiny", TINY_HOT, TINY_POST, 5, 0),
        ("medium", MEDIUM_HOT, MEDIUM_POST, 1, 0),
        # long_delay: 29 state-misses + 3 deferrals recovered; the 2 caused deferrals
        # land on the wrong shared-IP household (the same outcome the old hot reduce
        # produced). POST 112/75/73 unchanged from before Phase 16.
        ("long_delay", LONG_DELAY_HOT, LONG_DELAY_POST, 32, 2),
    ],
)
def test_post_reconcile_equals_the_pinned_numbers(
    name: str,
    hot_pin: AccuracyPin,
    post_pin: AccuracyPin,
    recovered_n: int,
    post_wrong: int,
) -> None:
    hot, post, recovered = hot_and_post(name)
    assert _counts(hot) == _pin(hot_pin)
    assert hot.caused_wrong_household == 0  # never guessed hot
    assert _counts(post) == _pin(post_pin)
    assert post.caused_wrong_household == post_wrong and post.caused_missed == 0
    assert len(recovered) == recovered_n
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
