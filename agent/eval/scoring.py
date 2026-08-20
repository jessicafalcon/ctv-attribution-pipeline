"""The PURE scoring rubric (Phase-10 Ruling C). No I/O, no clock, no LLM — it maps a
scenario + an `AttributionFinding` to one `Outcome`, and aggregates reps into the two
tables' data. This is unit-tested exhaustively with synthetic findings, so the whole
rubric is proven before a single API token is spent; the live sweep only supplies the
findings.

The one rule that is easy to get wrong (DECISIONS Phase 9 forward-note): a
`verdict == AMBIGUOUS_NEEDS_HUMAN` is ALWAYS an ABSTENTION. Never read the finding's
`top_hypothesis` on an escalation — `escalation()` sets it to the neutral
`upstream_data_change` default, which is NOT a diagnosis; reading it as one would score
a (correct) abstention as a wrong diagnosis and bias both tables."""

from collections import Counter
from dataclasses import dataclass
from enum import StrEnum

from agent.eval.scenarios import NEAR_MISS_PAIR, Scenario
from agent.finding import AttributionFinding


class Outcome(StrEnum):
    # fault_recall outcomes
    CORRECT_DIAGNOSIS = "correct_diagnosis"  # CONFIDENT ∧ top == expected
    WRONG_DIAGNOSIS = "wrong_diagnosis"  # CONFIDENT ∧ top != expected (dangerous)
    ABSTAINED = "abstained"  # AMBIGUOUS on a diagnosable fault (over-cautious miss)
    # capability_boundary / control outcomes
    CORRECT_ABSTENTION = "correct_abstention"  # AMBIGUOUS (correctly declined)
    FALSE_POSITIVE = "false_positive"  # CONFIDENT with a fault (fired on a clean case)


def is_abstention(finding: AttributionFinding) -> bool:
    """The one authoritative read: AMBIGUOUS_NEEDS_HUMAN is an abstention, full stop —
    the escalation-default top_hypothesis is never a diagnosis (forward-note)."""
    return finding.verdict == "AMBIGUOUS_NEEDS_HUMAN"


def score_rep(scenario: Scenario, finding: AttributionFinding) -> Outcome:
    """Score one live invocation. `fault_recall` wants a CONFIDENT correct top;
    `capability_boundary`/`control` want an abstention. A CONFIDENT verdict on an
    abstention-expected scenario is a false positive regardless of which fault it
    named."""
    if scenario.kind == "fault_recall":
        if is_abstention(finding):
            return Outcome.ABSTAINED
        return (
            Outcome.CORRECT_DIAGNOSIS
            if finding.top_hypothesis == scenario.expected
            else Outcome.WRONG_DIAGNOSIS
        )
    if scenario.kind == "negative_confirmation":
        # A genuine lift: abstaining is correct (the honest read of a healthy-but-higher
        # pipeline from the frozen context), and a CONFIDENT real_performance_change is
        # the bonus. ANY other CONFIDENT verdict is a failure — a CONFIDENT
        # device_graph_mismatch is the near-miss NEGATIVE-half failure §4.3 warns of.
        if is_abstention(finding):
            return Outcome.CORRECT_ABSTENTION
        return (
            Outcome.CORRECT_DIAGNOSIS
            if finding.top_hypothesis == scenario.expected
            else Outcome.WRONG_DIAGNOSIS
        )
    # capability_boundary or control: correct == abstain.
    return (
        Outcome.CORRECT_ABSTENTION if is_abstention(finding) else Outcome.FALSE_POSITIVE
    )


CORRECT_OUTCOMES = frozenset({Outcome.CORRECT_DIAGNOSIS, Outcome.CORRECT_ABSTENTION})


@dataclass(frozen=True)
class RepResult:
    """One rep: the scored outcome plus the raw finding facts the tables show (kept so
    tables.py renders from data, not from the live client)."""

    scenario: str
    outcome: Outcome
    verdict: str
    top_hypothesis: str
    probes_run: tuple[str, ...]


@dataclass
class ScenarioResult:
    """All reps for one scenario, plus the deterministic live context headline captured
    once per scenario (FG2 pin — BACKLOG 31): e.g. ip_resolved_fraction, match_rate."""

    scenario: Scenario
    reps: list[RepResult]
    headline: dict[str, float | int | None]

    @property
    def correct(self) -> int:
        return sum(1 for r in self.reps if r.outcome in CORRECT_OUTCOMES)

    @property
    def total(self) -> int:
        return len(self.reps)

    @property
    def outcome_counts(self) -> Counter:
        return Counter(r.outcome for r in self.reps)

    @property
    def verdict_counts(self) -> Counter:
        return Counter(r.verdict for r in self.reps)

    @property
    def top_hypothesis_counts(self) -> Counter:
        return Counter(r.top_hypothesis for r in self.reps)


@dataclass
class SweepResult:
    """The whole sweep. `false_positive_rate` is measured over the two `control`
    scenarios only (§4.3) — capability_boundary fires are reported in their own row,
    never folded into the headline FP rate."""

    scenarios: list[ScenarioResult]

    def by_name(self, name: str) -> ScenarioResult:
        for s in self.scenarios:
            if s.scenario.name == name:
                return s
        raise KeyError(name)

    @property
    def control_results(self) -> list[ScenarioResult]:
        return [s for s in self.scenarios if s.scenario.kind == "control"]

    @property
    def false_positive_rate(self) -> float | None:
        controls = self.control_results
        total = sum(s.total for s in controls)
        if total == 0:
            return None
        fps = sum(
            1 for s in controls for r in s.reps if r.outcome == Outcome.FALSE_POSITIVE
        )
        return fps / total

    @property
    def near_miss(self) -> list[ScenarioResult]:
        """real_lift and shared_ip_spike, in that order, for the near-miss table."""
        return [self.by_name(n) for n in NEAR_MISS_PAIR]
