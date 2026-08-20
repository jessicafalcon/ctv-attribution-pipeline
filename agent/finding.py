"""The typed `AttributionFinding` (ARCHITECTURE §4.2 "Report") — the agent's only
output. Schema-constrained: the loop produces it via a terminal `submit_finding`
tool whose input schema IS this model, and a pydantic `ValidationError` escalates to
AMBIGUOUS_NEEDS_HUMAN rather than silently retrying (CLAUDE.md output contract).

`profile` is a caller-supplied LABEL, never treated as an authoritative claim about
which rows were read (Phase-8 CA-minor: the serving tables have no `profile` column;
a clean single-profile stack is what makes the label accurate)."""

from typing import Literal

from pydantic import BaseModel

from agent.hypotheses import Hypothesis

Verdict = Literal["CONFIDENT", "AMBIGUOUS_NEEDS_HUMAN"]


class RankedHypothesis(BaseModel):
    """One catalog cause with the evidence weighed for and against it. `weight` is the
    agent's evidence weight in [0, 1]; the finding's `ranked` list is ordered by it
    descending — that ordering IS the §4.2 "Rank" step."""

    hypothesis: Hypothesis
    evidence_for: list[str]
    evidence_against: list[str]
    weight: float


class AttributionFinding(BaseModel):
    """The §4.2 report. `top_hypothesis` is the highest-weighted cause;
    `recommended_action` is the human-facing call (e.g. "hold cmp-002 ROAS pending
    review"); `verdict` escalates to AMBIGUOUS_NEEDS_HUMAN when signals conflict or a
    non-discriminable cause (co-view) leads. `probes_run` names the probes the agent
    executed — non-empty by loop contract, so every finding cites a Test step."""

    profile: str
    top_hypothesis: Hypothesis
    ranked: list[RankedHypothesis]
    ruled_out: list[Hypothesis]
    recommended_action: str
    verdict: Verdict
    probes_run: list[str]


def escalation(profile: str, reason: str, probes_run: list[str]) -> AttributionFinding:
    """A synthesized AMBIGUOUS_NEEDS_HUMAN finding — the single escape path when the
    model's `submit_finding` payload fails validation, when the loop hits
    AGENT_MAX_PROBE_ROUNDS, or when a finding is submitted with no probe run. Never a
    silent retry: the reason is surfaced in `evidence_for` for the human."""
    return AttributionFinding(
        profile=profile,
        top_hypothesis=Hypothesis.UPSTREAM_DATA_CHANGE,
        ranked=[
            RankedHypothesis(
                hypothesis=Hypothesis.UPSTREAM_DATA_CHANGE,
                evidence_for=[reason],
                evidence_against=[],
                weight=0.0,
            )
        ],
        ruled_out=[],
        recommended_action="escalate to a human: " + reason,
        verdict="AMBIGUOUS_NEEDS_HUMAN",
        probes_run=probes_run,
    )
