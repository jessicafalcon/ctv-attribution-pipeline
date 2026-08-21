"""Feature 3b — medium profile: robustness-oracle equality (Done-when clause 1)
and eviction firing (clause 2), computed OFFLINE with no services: generate →
resolve → dedup → (evicting engine) vs (non-evicting oracle) over the SAME E.

The evicting engine's output must equal the non-evicting oracle byte-for-byte —
that is the proof dedup + watermark-release + eviction drop no in-tolerance data.
medium keeps late.max ≤ allowed_lateness, so there are no state-misses; the one
recall gap is a caused shared-IP conversion deferred to reconciliation (Phase
16). precision < 1 is organic last-touch over-credit only (no wrong-household —
that is a Phase-8 fault story, not medium).

These numbers are pinned like tiny's counts: changing producer/profiles/
medium.json means updating these assertions in the same change, never silently.
"""

from datetime import timedelta

import pytest

from accuracy.score import score
from producer.config import load_profile
from producer.generate import generate
from producer.serialize import jsonl
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream
from streaming import metrics
from streaming.attribute import ALLOWED_LATENESS, attribute, dedup_streams
from streaming.dataflow import run_attribution
from tests.pins import MEDIUM_HOT


@pytest.fixture(scope="module")
def medium():
    p = load_profile("medium")
    s = generate(p, p.seed)
    idx = GraphIndex.from_households(s.graph.households)
    resolved = resolve_stream(s.conversions, idx)
    exps, res, suppressed = dedup_streams(s.exposures, resolved)
    return {
        "profile": p,
        "stream": s,
        "exps": exps,
        "res": res,
        "suppressed": suppressed,
    }


def _score(rows, stream, exps):
    truth = {t.conversion_id: t.truth_exposure_id for t in stream.truth_links}
    exp_hh = {e.exposure_id: e.household_id for e in exps}
    credited = {
        r.conversion_id: (r.household_id, r.exposure_id) for r in rows if r.attributed
    }
    return score(credited, truth, exp_hh, "medium")


def _run_engine(exps, res):
    metrics.EXPOSURES_EVICTED._value.set(0)
    metrics.reset_join_state_peak()
    return run_attribution(exps, res, ALLOWED_LATENESS)


def test_allowed_lateness_covers_medium_late_max(medium) -> None:
    # The guard the whole parity rests on: every late arrival is within tolerance,
    # so the oracle is a plain dedup+replay with no lateness-drop bookkeeping.
    late_max = timedelta(minutes=medium["profile"].events.late.max_minutes)
    assert late_max <= ALLOWED_LATENESS


def test_dedup_fires_on_medium(medium) -> None:
    assert medium["suppressed"] == 70  # non-trivial re-send suppression


def test_engine_equals_non_evicting_oracle_byte_for_byte(medium) -> None:
    # Clause 1 (strong form): eviction + release drop nothing.
    oracle = attribute(medium["exps"], medium["res"])
    engine = _run_engine(medium["exps"], medium["res"])
    assert jsonl(engine) == jsonl(oracle)
    assert len(engine) == 132


def test_engine_pr_equals_oracle_pr_clean_baseline(medium) -> None:
    oracle = attribute(medium["exps"], medium["res"])
    engine = _run_engine(medium["exps"], medium["res"])
    ro = _score(oracle, medium["stream"], medium["exps"])
    re = _score(engine, medium["stream"], medium["exps"])
    assert (re.precision, re.recall) == (ro.precision, ro.recall)  # clause 1
    assert re.recall == MEDIUM_HOT.recall and re.caused_wrong_household == 0  # clean
    assert re.caused_missed == 1  # the single ambiguous_ip deferral (not a loss)
    assert re.precision == MEDIUM_HOT.precision  # organic last-touch over-credit only
    assert (re.credited, re.truth_links, re.household_correct) == (
        MEDIUM_HOT.credited,
        MEDIUM_HOT.truth,
        MEDIUM_HOT.correct,
    )


def test_eviction_runs_on_medium(medium) -> None:
    # Clause 2: join-state built up (gauge > 0) and came back down (evicted > 0).
    _run_engine(medium["exps"], medium["res"])
    assert metrics.EXPOSURES_EVICTED._value.get() > 0
    assert metrics.JOIN_STATE_SIZE._value.get() > 0
    metrics.EXPOSURES_EVICTED._value.set(0)
    metrics.JOIN_STATE_SIZE.set(0)


def test_oracle_is_deterministic(medium) -> None:
    p = medium["profile"]
    s2 = generate(p, p.seed)
    idx = GraphIndex.from_households(s2.graph.households)
    exps2, res2, _ = dedup_streams(s2.exposures, resolve_stream(s2.conversions, idx))
    assert jsonl(attribute(exps2, res2)) == jsonl(
        attribute(medium["exps"], medium["res"])
    )
