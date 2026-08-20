"""AttributionFinding — the typed output contract, no services. Validation is the
escalation gate: a malformed finding becomes AMBIGUOUS_NEEDS_HUMAN, never a silent
retry."""

import pytest
from pydantic import ValidationError

from agent.finding import AttributionFinding, RankedHypothesis, escalation
from agent.hypotheses import Hypothesis

_GOOD = {
    "profile": "shared_ip_spike",
    "top_hypothesis": "device_graph_mismatch",
    "ranked": [
        {
            "hypothesis": "device_graph_mismatch",
            "evidence_for": ["ip_resolved_fraction 0.42 elevated"],
            "evidence_against": [],
            "weight": 0.9,
        }
    ],
    "ruled_out": ["real_performance_change"],
    "recommended_action": "hold ROAS pending review",
    "verdict": "CONFIDENT",
    "probes_run": ["ip_cluster_detail"],
}


def test_valid_finding_parses() -> None:
    finding = AttributionFinding.model_validate(_GOOD)
    assert finding.top_hypothesis is Hypothesis.DEVICE_GRAPH_MISMATCH
    assert finding.ranked[0].weight == 0.9
    assert finding.verdict == "CONFIDENT"


def test_bad_verdict_is_rejected() -> None:
    with pytest.raises(ValidationError):
        AttributionFinding.model_validate({**_GOOD, "verdict": "MAYBE"})


def test_unknown_hypothesis_is_rejected() -> None:
    with pytest.raises(ValidationError):
        AttributionFinding.model_validate({**_GOOD, "top_hypothesis": "vibes"})


def test_escalation_is_ambiguous_and_carries_the_reason() -> None:
    finding = escalation("late_burst", "signals conflict", ["window_edge_distribution"])
    assert finding.verdict == "AMBIGUOUS_NEEDS_HUMAN"
    assert finding.probes_run == ["window_edge_distribution"]
    assert "signals conflict" in finding.ranked[0].evidence_for[0]
    assert "signals conflict" in finding.recommended_action


def test_ranked_hypothesis_roundtrips() -> None:
    rh = RankedHypothesis(
        hypothesis=Hypothesis.WINDOW_EDGE_EFFECT,
        evidence_for=["near-boundary pile-up"],
        evidence_against=["small sample"],
        weight=0.4,
    )
    assert rh.hypothesis is Hypothesis.WINDOW_EDGE_EFFECT
