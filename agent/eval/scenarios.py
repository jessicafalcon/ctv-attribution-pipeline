"""The frozen scenario catalog (Phase-10 Ruling B). Six scenarios — the five fault
profiles plus the no-fault baseline — each labeled by how the agent is scored on it.

The three `kind`s are NOT the same "correct = abstain":
- `fault_recall`: the agent MUST confidently name `expected` (a diagnosable fault).
- `capability_boundary`: a REAL fault the agent cannot see from serving data by design
  (co_view_bug — the co-view adjusted factor is a DECISIONS won't-do, BACKLOG 26).
  Correct = abstain, but it is NOT a false-positive control — it is a labeled limit.
- `control`: nothing is wrong (duplicate_flood absorbed by dedup; no_fault_baseline a
  healthy pipeline). Correct = abstain. These two are the false-positive-rate
  denominator (§4.3 "two controls the agent must correctly leave alone").
"""

from dataclasses import dataclass
from typing import Literal

from agent.config import EVAL_REPS
from agent.hypotheses import Hypothesis

Kind = Literal["fault_recall", "capability_boundary", "control"]


@dataclass(frozen=True)
class Scenario:
    """One row of the sweep. `profile` is the producer profile seeded live; `expected`
    is the correct top hypothesis for `fault_recall` (None when the correct outcome is
    an abstention). `note` is the human-readable reason, carried into the RESULTS table
    so the two abstentions are never conflated (Ruling B record-fix)."""

    name: str
    profile: str
    kind: Kind
    expected: Hypothesis | None
    note: str


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        "shared_ip_spike",
        "shared_ip_spike",
        "fault_recall",
        Hypothesis.DEVICE_GRAPH_MISMATCH,
        "shared-IP wrong-household matches (ip_resolved_fraction elevated)",
    ),
    Scenario(
        "real_lift",
        "real_lift",
        "fault_recall",
        Hypothesis.REAL_PERFORMANCE_CHANGE,
        "near-miss NEGATIVE half: a genuine lift — must be ruled clean, "
        "NOT device_graph_mismatch (ip_resolved_fraction flat)",
    ),
    Scenario(
        "late_burst",
        "late_burst",
        "fault_recall",
        Hypothesis.LATE_ARRIVAL_DISTORTION,
        "late arrivals recovered by reconciliation → material restatement",
    ),
    Scenario(
        "co_view_bug",
        "co_view_bug",
        "capability_boundary",
        None,
        "undiagnosable from serving data by design: the co-view adjusted factor is a "
        "DECISIONS won't-do (BACKLOG 26); the raw genre skew is ~7% in serving data → "
        "correct outcome is abstention (a known capability boundary)",
    ),
    Scenario(
        "duplicate_flood",
        "duplicate_flood",
        "control",
        None,
        "benign control: dedup absorbs the flood, ClickHouse carries no fingerprint → "
        "nothing is wrong, correct outcome is abstention",
    ),
    Scenario(
        "no_fault_baseline",
        "no_fault_baseline",
        "control",
        None,
        "no-fault baseline: a healthy pipeline → correct outcome is abstention",
    ),
)

# The near-miss pair (§4.3): a genuine lift vs shared-IP inflation, both raise ROAS.
NEAR_MISS_PAIR = ("real_lift", "shared_ip_spike")

# The config↔sweep contract: EVAL_REPS is defined in agent/config.py (Phase 9) and
# consumed here. 5 reps × 6 scenarios = 30 invocations (the fixed N behind the tables).
TOTAL_INVOCATIONS = EVAL_REPS * len(SCENARIOS)


def by_name(name: str) -> Scenario:
    for s in SCENARIOS:
        if s.name == name:
            return s
    raise KeyError(f"unknown scenario {name!r}")
