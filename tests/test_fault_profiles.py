"""Phase 8 Done-when 1: the five fault profiles run REPRODUCIBLY and each carries
its ONE isolated fault, proven OFFLINE through the evicting engine (run_attribution) —
no services. Numbers are pinned like the medium-parity counts: re-tuning a profile
JSON means updating the matching assertion in the SAME change, never silently.

Taxonomy (DECISIONS Phase 8): shared_ip_spike / late_burst / co_view_bug /
real_lift are DIAGNOSABLE faults; duplicate_flood is a benign CONTROL — dedup
absorbs the flood and the attribution decision is byte-identical to its dedup-off
self, so ClickHouse carries no fingerprint (the correct future agent output is
no-fault; Phase 10 scores it as a false-positive control).
"""

from collections import defaultdict

import pytest

from accuracy.score import AccuracyReport, score
from producer.config import load_profile
from producer.generate import generate
from producer.serialize import jsonl
from reconcile.reconcile import LONG_WINDOW
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream
from streaming import metrics
from streaming.attribute import ALLOWED_LATENESS, HOT_WINDOW, attribute, dedup_streams
from streaming.dataflow import run_attribution

FAULT_PROFILES = [
    "shared_ip_spike",
    "real_lift",
    "co_view_bug",
    "late_burst",
    "duplicate_flood",
]


def _engine(exps, res):
    metrics.EXPOSURES_EVICTED._value.set(0)
    metrics.reset_join_state_peak()
    return run_attribution(exps, res, ALLOWED_LATENESS)


class FaultRun:
    def __init__(self, name: str) -> None:
        p = load_profile(name)
        self.profile = p
        s = generate(p, p.seed)
        self.stream = s
        idx = GraphIndex.from_households(s.graph.households)
        resolved = resolve_stream(s.conversions, idx)
        self.exps, self.res, self.suppressed = dedup_streams(s.exposures, resolved)
        self.rows = _engine(self.exps, self.res)

    def score(self) -> AccuracyReport:
        truth = {t.conversion_id: t.truth_exposure_id for t in self.stream.truth_links}
        exp_hh = {e.exposure_id: e.household_id for e in self.exps}
        credited = {
            r.conversion_id: (r.household_id, r.exposure_id)
            for r in self.rows
            if r.attributed
        }
        return score(credited, truth, exp_hh, self.profile.name)

    def truth_caused_per_genre_exposure(self) -> dict[str, float]:
        exp_genre = {e.exposure_id: e.program_genre for e in self.exps}
        exp_per_genre: dict[str, int] = defaultdict(int)
        for e in self.exps:
            exp_per_genre[e.program_genre] += 1
        caused: dict[str, int] = defaultdict(int)
        for t in self.stream.truth_links:
            caused[exp_genre[t.truth_exposure_id]] += 1
        return {g: caused[g] / exp_per_genre[g] for g in exp_per_genre}


@pytest.fixture(scope="module")
def runs() -> dict[str, FaultRun]:
    return {n: FaultRun(n) for n in FAULT_PROFILES}


@pytest.mark.parametrize("name", FAULT_PROFILES)
def test_profile_is_reproducible(name: str) -> None:
    p = load_profile(name)
    a = generate(p, p.seed)
    b = generate(p, p.seed)
    assert jsonl(a.exposures) == jsonl(b.exposures)
    assert jsonl(a.conversions) == jsonl(b.conversions)
    assert jsonl(a.truth_links) == jsonl(b.truth_links)


def test_shared_ip_spike_defers_ambiguous_hot_no_wrong_household(runs) -> None:
    # Phase 16: the hot path never guesses a shared-IP household, so the 11 caused
    # wrong-household misattributions the old reduce made (BACKLOG 20) are gone
    # hot — at the price of 19 caused conversions deferred (ambiguous_ip) to
    # reconciliation, where the cross-household pick is proven
    # (tests/test_reconcile.py, live: make test-int-shared-ip).
    r = runs["shared_ip_spike"].score()
    assert r.caused_wrong_household == 0
    assert r.caused_missed == 19  # every one an ambiguous_ip deferral, not a loss
    assert (r.truth_links, r.household_correct) == (80, 61)
    assert r.recall < 1.0
    deferred = [c for c in runs["shared_ip_spike"].rows if not c.attributed]
    assert sum(c.candidate_count > 1 for c in deferred) >= 19


def test_real_lift_is_a_clean_lift_no_shared_ip_fault(runs) -> None:
    # Near-miss counterpart: reported ROAS rises for REAL reasons — more caused
    # conversions, zero wrong-household. IP-cluster stats are what tell it apart
    # from shared_ip_spike (both lift the headline).
    r = runs["real_lift"].score()
    assert r.caused_wrong_household == 0
    assert r.recall == 1.0
    assert r.truth_links == 157  # ≈2× the medium baseline (92)


def test_co_view_bug_skews_one_genre_below_saturation(runs) -> None:
    # Row 15: the multiplier (0.2 × 4.0 = 0.8) stays BELOW the min(1.0, rate)
    # clamp, so the skew is observable, not saturated away. sports caused-rate is
    # ~4× the flat genres, truth-side (the ClickHouse proxy is genre_reach).
    rates = runs["co_view_bug"].truth_caused_per_genre_exposure()
    others = [v for g, v in rates.items() if g != "sports"]
    assert rates["sports"] > 0.6
    assert rates["sports"] < 1.0  # not clamped
    assert rates["sports"] > 2.5 * (sum(others) / len(others))
    assert runs["co_view_bug"].score().caused_wrong_household == 0  # isolated


def test_late_burst_pushes_conversions_past_the_hot_window(runs) -> None:
    # The late injector (arrival lateness, not event-time delay) pushes a burst of
    # conversions so late that their exposure is evicted before release → hot-miss.
    run = runs["late_burst"]
    r = run.score()
    # 5 state-misses + 1 ambiguous_ip deferral (all recovered by reconciliation)
    assert r.caused_missed == 6
    assert sum(1 for c in run.rows if not c.attributed and c.candidate_count > 1) == 1
    assert r.caused_wrong_household == 0
    peak_lateness = max(
        (c.ingest_time - c.event_time).total_seconds() for c in run.stream.conversions
    )
    assert peak_lateness > 7 * 86400  # well past the 7d hot window


def test_duplicate_flood_is_a_benign_control(runs) -> None:
    # Control invariant: the attribution DECISION per conversion_id is identical
    # with dedup ON (deduped streams) vs OFF (raw duplicated streams) — RMT /
    # reduction transparency. So ClickHouse FINAL carries no duplicate fingerprint;
    # the flood is real (many suppressed) but benign.
    run = runs["duplicate_flood"]
    assert run.suppressed == 335  # the flood is large
    on = attribute(run.exps, run.res)
    off = attribute(
        run.stream.exposures,
        resolve_stream(
            run.stream.conversions,
            GraphIndex.from_households(run.stream.graph.households),
        ),
    )

    def decide(rows):
        return {
            r.conversion_id: (r.household_id, r.exposure_id, r.attributed) for r in rows
        }

    assert decide(on) == decide(off)
    assert run.score().caused_wrong_household == 0


# --- Phase 10: the no-fault baseline (the sweep's entry condition) -----------
# A healthy pipeline the agent must LEAVE ALONE — offline-pinned non-alarming, the
# same way the fault profiles are pinned. Not in FAULT_PROFILES: it carries no fault.


def test_no_fault_baseline_is_reproducible() -> None:
    p = load_profile("no_fault_baseline")
    a = generate(p, p.seed)
    b = generate(p, p.seed)
    assert jsonl(a.exposures) == jsonl(b.exposures)
    assert jsonl(a.conversions) == jsonl(b.conversions)
    assert jsonl(a.truth_links) == jsonl(b.truth_links)


def test_no_fault_baseline_is_clean_nothing_to_flag() -> None:
    run = FaultRun("no_fault_baseline")
    r = run.score()
    assert r.caused_wrong_household == 0  # no shared-IP misattribution
    assert r.caused_missed == 0  # no state-misses
    assert r.recall == 1.0
    assert (r.truth_links, r.household_correct) == (90, 90)
    # Delays sit inside the 7d hot window — nothing for reconciliation to restate.
    peak_late = max(
        (c.ingest_time - c.event_time).total_seconds() for c in run.stream.conversions
    )
    assert peak_late < 7 * 86400


# --- Phase 10: the full-run sweep injects no spurious STATE-MISS restatement --
# `make agent-eval` runs the FULL pipeline (incl. reconciliation) per scenario so
# late_burst's restatement exists. Confirm the three IN-WINDOW scenarios (all event-time
# delays inside the 7d window) recover no state-miss on the long window — the long
# window credits exactly the hot set. (late_burst is excluded: its misses are arrival
# lateness / eviction, not event-time, so it genuinely restates.)
# Phase 16 caveat: these profiles DO now restate through a second channel — their
# shared-IP conversions are deferred hot (ambiguous_ip) and credited by the
# reconcile pass — so report_snapshots PRE != FINAL wherever a profile has ambiguous
# conversions. That restatement is the deferral landing, not a late-arrival signal;
# agent-eval is not re-run in Phase 16 (API tokens) — recorded as an open risk.


@pytest.mark.parametrize("name", ["shared_ip_spike", "real_lift", "no_fault_baseline"])
def test_in_window_scenarios_recover_nothing_on_the_long_window(name: str) -> None:
    p = load_profile(name)
    s = generate(p, p.seed)
    idx = GraphIndex.from_households(s.graph.households)
    exps, res, _ = dedup_streams(s.exposures, resolve_stream(s.conversions, idx))

    def attributed_ids(window):
        return {r.conversion_id for r in attribute(exps, res, window) if r.attributed}

    hot = attributed_ids(HOT_WINDOW)
    long = attributed_ids(LONG_WINDOW)
    # The long window credits exactly the hot set — reconciliation recovers nothing,
    # so no restatement (roas_delta) is produced on these profiles.
    assert long == hot
