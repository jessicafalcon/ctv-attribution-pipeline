"""Phase-10 Ruling C: the pure scoring rubric, proven exhaustively with synthetic
findings — no LLM, no tokens. This is what lets the live sweep be a thin data-capture
step: every way a finding can score is decided here, offline.

The rule most easily gotten wrong (DECISIONS Phase 9 forward-note): an
AMBIGUOUS_NEEDS_HUMAN escalation is an ABSTENTION, never a diagnosis of its
escalation-default `upstream_data_change` top_hypothesis."""

import pytest

from agent.config import EVAL_REPS
from agent.eval import scenarios as scen
from agent.eval.scenarios import Scenario
from agent.eval.scoring import (
    Outcome,
    RepResult,
    ScenarioResult,
    SweepResult,
    is_abstention,
    score_rep,
)
from agent.finding import AttributionFinding, RankedHypothesis, escalation
from agent.hypotheses import Hypothesis


def finding(
    top: Hypothesis, verdict: str, profile: str = "p", probes=("ip_cluster_detail",)
) -> AttributionFinding:
    return AttributionFinding(
        profile=profile,
        top_hypothesis=top,
        ranked=[
            RankedHypothesis(
                hypothesis=top, evidence_for=[], evidence_against=[], weight=0.9
            )
        ],
        ruled_out=[],
        recommended_action="hold pending review",
        verdict=verdict,
        probes_run=list(probes),
    )


# --- fault_recall scoring ---------------------------------------------------

FAULT = scen.by_name("shared_ip_spike")  # expected = DEVICE_GRAPH_MISMATCH


def test_fault_recall_confident_correct_top_is_correct_diagnosis():
    f = finding(Hypothesis.DEVICE_GRAPH_MISMATCH, "CONFIDENT")
    assert score_rep(FAULT, f) == Outcome.CORRECT_DIAGNOSIS


def test_fault_recall_confident_wrong_top_is_wrong_diagnosis():
    f = finding(Hypothesis.REAL_PERFORMANCE_CHANGE, "CONFIDENT")
    assert score_rep(FAULT, f) == Outcome.WRONG_DIAGNOSIS


def test_fault_recall_ambiguous_is_abstained_not_a_diagnosis():
    # Even if the model named the right cause, AMBIGUOUS = over-cautious miss.
    f = finding(Hypothesis.DEVICE_GRAPH_MISMATCH, "AMBIGUOUS_NEEDS_HUMAN")
    assert score_rep(FAULT, f) == Outcome.ABSTAINED


def test_escalation_default_never_read_as_a_diagnosis():
    # escalation() sets top=upstream_data_change; on a fault_recall scenario that must
    # score ABSTAINED (abstention), NOT wrong_diagnosis (the forward-note bias trap).
    esc = escalation("p", "budget exhausted", ["ip_cluster_detail"])
    assert esc.top_hypothesis == Hypothesis.UPSTREAM_DATA_CHANGE
    assert score_rep(FAULT, esc) == Outcome.ABSTAINED


# --- near-miss negative half ------------------------------------------------

REAL_LIFT = scen.by_name("real_lift")  # expected = REAL_PERFORMANCE_CHANGE


def test_real_lift_confident_real_change_is_correct():
    f = finding(Hypothesis.REAL_PERFORMANCE_CHANGE, "CONFIDENT")
    assert score_rep(REAL_LIFT, f) == Outcome.CORRECT_DIAGNOSIS


def test_real_lift_confident_device_graph_is_the_near_miss_failure():
    # The exact negative-half failure §4.3 warns about: firing device_graph_mismatch
    # on a genuine lift.
    f = finding(Hypothesis.DEVICE_GRAPH_MISMATCH, "CONFIDENT")
    assert score_rep(REAL_LIFT, f) == Outcome.WRONG_DIAGNOSIS


# --- capability_boundary and control scoring --------------------------------

CO_VIEW = scen.by_name("co_view_bug")
CONTROL = scen.by_name("no_fault_baseline")
DUP = scen.by_name("duplicate_flood")


@pytest.mark.parametrize("scenario", [CO_VIEW, CONTROL, DUP])
def test_abstention_expected_ambiguous_is_correct_abstention(scenario: Scenario):
    f = finding(Hypothesis.UPSTREAM_DATA_CHANGE, "AMBIGUOUS_NEEDS_HUMAN")
    assert score_rep(scenario, f) == Outcome.CORRECT_ABSTENTION


@pytest.mark.parametrize("scenario", [CO_VIEW, CONTROL, DUP])
@pytest.mark.parametrize(
    "top",
    [
        Hypothesis.DEVICE_GRAPH_MISMATCH,
        Hypothesis.CO_VIEW_INFLATION,
        Hypothesis.REAL_PERFORMANCE_CHANGE,
    ],
)
def test_abstention_expected_confident_is_false_positive(scenario: Scenario, top):
    # Any CONFIDENT verdict on an abstention-expected scenario is a false positive,
    # regardless of which fault it named.
    assert score_rep(scenario, finding(top, "CONFIDENT")) == Outcome.FALSE_POSITIVE


def test_is_abstention_reads_only_the_verdict():
    assert is_abstention(
        finding(Hypothesis.DEVICE_GRAPH_MISMATCH, "AMBIGUOUS_NEEDS_HUMAN")
    )
    assert not is_abstention(finding(Hypothesis.DEVICE_GRAPH_MISMATCH, "CONFIDENT"))


# --- aggregation and FP rate ------------------------------------------------


_ABSTAIN_OUTCOMES = {Outcome.CORRECT_ABSTENTION, Outcome.ABSTAINED}


def _sr(scenario: Scenario, outcomes: list[Outcome]) -> ScenarioResult:
    reps = [
        RepResult(
            scenario=scenario.name,
            outcome=o,
            verdict="AMBIGUOUS_NEEDS_HUMAN" if o in _ABSTAIN_OUTCOMES else "CONFIDENT",
            top_hypothesis="x",
            probes_run=(),
        )
        for o in outcomes
    ]
    return ScenarioResult(scenario=scenario, reps=reps, headline={})


def test_scenario_result_correct_counts_both_correct_outcomes():
    sr = _sr(FAULT, [Outcome.CORRECT_DIAGNOSIS] * 4 + [Outcome.WRONG_DIAGNOSIS])
    assert (sr.correct, sr.total) == (4, 5)
    ab = _sr(CONTROL, [Outcome.CORRECT_ABSTENTION] * 5)
    assert (ab.correct, ab.total) == (5, 5)


def test_false_positive_rate_is_over_the_two_controls_only():
    # 1 FP across duplicate_flood + no_fault_baseline (10 control reps) → 0.1.
    # A co_view_bug false positive must NOT change it.
    sweep = SweepResult(
        scenarios=[
            _sr(scen.by_name("shared_ip_spike"), [Outcome.CORRECT_DIAGNOSIS] * 5),
            _sr(
                CO_VIEW, [Outcome.FALSE_POSITIVE] * 5
            ),  # boundary, excluded from FP rate
            _sr(DUP, [Outcome.CORRECT_ABSTENTION] * 5),
            _sr(CONTROL, [Outcome.FALSE_POSITIVE] + [Outcome.CORRECT_ABSTENTION] * 4),
        ]
    )
    assert sweep.false_positive_rate == pytest.approx(1 / 10)


def test_false_positive_rate_none_when_no_controls():
    sweep = SweepResult(scenarios=[_sr(FAULT, [Outcome.CORRECT_DIAGNOSIS])])
    assert sweep.false_positive_rate is None


def test_near_miss_orders_real_lift_then_shared_ip():
    sweep = SweepResult(
        scenarios=[
            _sr(scen.by_name("shared_ip_spike"), [Outcome.CORRECT_DIAGNOSIS]),
            _sr(REAL_LIFT, [Outcome.CORRECT_DIAGNOSIS]),
        ]
    )
    assert [s.scenario.name for s in sweep.near_miss] == [
        "real_lift",
        "shared_ip_spike",
    ]


def test_eval_reps_times_scenarios_is_thirty():
    assert scen.TOTAL_INVOCATIONS == EVAL_REPS * len(scen.SCENARIOS) == 30
