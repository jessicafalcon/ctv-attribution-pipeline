"""The scenario catalog contract and the PURE table renderers (Phase-10 Ruling B).
Renders are asserted offline so the RESULTS tables' shape is proven before the sweep."""

from agent.config import EVAL_REPS
from agent.eval import scenarios as scen
from agent.eval.scoring import Outcome, RepResult, ScenarioResult, SweepResult
from agent.eval.tables import fault_diagnosis_table, near_miss_table
from agent.hypotheses import Hypothesis


def test_catalog_is_six_scenarios_matching_config():
    assert len(scen.SCENARIOS) == 6
    assert scen.TOTAL_INVOCATIONS == EVAL_REPS * 6


def test_scenario_kinds_are_the_expected_split():
    kinds = [s.kind for s in scen.SCENARIOS]
    assert kinds.count("control") == 2  # the FP-rate denominator
    assert kinds.count("capability_boundary") == 1  # co_view_bug
    assert kinds.count("fault_recall") == 2  # shared_ip_spike, late_burst
    assert kinds.count("negative_confirmation") == 1  # real_lift (near-miss ⊖)


def test_only_diagnosable_scenarios_carry_an_expected_hypothesis():
    # fault_recall AND negative_confirmation carry an expected (the latter for the
    # bonus match / table label); the abstention-only kinds carry None.
    for s in scen.SCENARIOS:
        if s.kind in ("fault_recall", "negative_confirmation"):
            assert isinstance(s.expected, Hypothesis)
        else:
            assert s.expected is None


def test_co_view_bug_is_a_capability_boundary_not_a_control():
    # The record-fix: co_view_bug abstains for a *seeing* reason, not a benign one.
    assert scen.by_name("co_view_bug").kind == "capability_boundary"
    assert scen.by_name("duplicate_flood").kind == "control"
    assert scen.by_name("no_fault_baseline").kind == "control"


def test_near_miss_pair_is_real_lift_and_shared_ip():
    assert scen.NEAR_MISS_PAIR == ("real_lift", "shared_ip_spike")


def _rep(name, outcome, verdict, top):
    return RepResult(name, outcome, verdict, top, ("ip_cluster_detail",))


def _example_sweep() -> SweepResult:
    results = []
    for s in scen.SCENARIOS:
        if s.kind == "fault_recall":
            reps = [
                _rep(s.name, Outcome.CORRECT_DIAGNOSIS, "CONFIDENT", s.expected.value)
            ] * 5
        else:
            reps = [
                _rep(
                    s.name,
                    Outcome.CORRECT_ABSTENTION,
                    "AMBIGUOUS_NEEDS_HUMAN",
                    "upstream_data_change",
                )
            ] * 5
        head = {
            "ip_resolved_fraction": 0.42 if s.name == "shared_ip_spike" else 0.05,
            "match_rate": 0.9,
        }
        results.append(ScenarioResult(scenario=s, reps=list(reps), headline=head))
    return SweepResult(scenarios=results)


def test_fault_diagnosis_table_renders_every_scenario_and_fp_line():
    txt = fault_diagnosis_table(_example_sweep())
    for s in scen.SCENARIOS:
        assert f"`{s.name}`" in txt
    assert "5/5" in txt
    # FP rate over the two controls, all-correct → 0/10 = 0%.
    assert "0/10 = 0%" in txt
    # The two abstentions are labeled distinctly.
    assert "capability boundary" in txt


def test_near_miss_table_shows_the_discriminator():
    txt = near_miss_table(_example_sweep())
    assert "`real_lift`" in txt and "`shared_ip_spike`" in txt
    assert "0.420" in txt  # shared_ip elevated fraction
    assert "ip_resolved_fraction" in txt


def test_fp_rate_reflects_a_control_false_positive():
    results = []
    for s in scen.SCENARIOS:
        if s.name == "no_fault_baseline":
            reps = [
                _rep(
                    s.name, Outcome.FALSE_POSITIVE, "CONFIDENT", "device_graph_mismatch"
                )
            ] + [
                _rep(
                    s.name,
                    Outcome.CORRECT_ABSTENTION,
                    "AMBIGUOUS_NEEDS_HUMAN",
                    "upstream_data_change",
                )
            ] * 4
        elif s.kind == "fault_recall":
            reps = [
                _rep(s.name, Outcome.CORRECT_DIAGNOSIS, "CONFIDENT", s.expected.value)
            ] * 5
        else:
            reps = [
                _rep(
                    s.name,
                    Outcome.CORRECT_ABSTENTION,
                    "AMBIGUOUS_NEEDS_HUMAN",
                    "upstream_data_change",
                )
            ] * 5
        results.append(
            ScenarioResult(
                scenario=s,
                reps=reps,
                headline={"ip_resolved_fraction": 0.1, "match_rate": 0.9},
            )
        )
    txt = fault_diagnosis_table(SweepResult(scenarios=results))
    assert "1/10 = 10%" in txt
