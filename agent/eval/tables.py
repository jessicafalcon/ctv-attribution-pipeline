"""PURE Markdown renderers for the two Phase-10 tables (§4.3). Take a scored
`SweepResult` and produce the exact text appended to `docs/RESULTS.md`. No I/O — the
sweep runner writes the returned strings — so the rendering is unit-testable offline."""

from collections import Counter

from agent.eval.scenarios import Scenario
from agent.eval.scoring import SweepResult

EXPECTED_LABEL = {
    "fault_recall": lambda s: f"CONFIDENT {s.expected.value}",
    "capability_boundary": lambda s: "abstain (capability boundary)",
    "control": lambda s: "abstain (control)",
}


def _expected_outcome(scenario: Scenario) -> str:
    return EXPECTED_LABEL[scenario.kind](scenario)


def _spread(counts: Counter) -> str:
    """`3× a, 2× b`, highest count first, deterministic (ties broken by key)."""
    items = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(f"{n}× {k}" for k, n in items)


def _fmt(v: float | int | None, places: int = 3) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{places}f}"
    return str(v)


def fault_diagnosis_table(sweep: SweepResult) -> str:
    lines = [
        "| scenario | kind | expected outcome | correct | verdict spread | "
        "top-hypothesis spread |",
        "|---|---|---|---|---|---|",
    ]
    for sr in sweep.scenarios:
        lines.append(
            f"| `{sr.scenario.name}` | {sr.scenario.kind} | "
            f"{_expected_outcome(sr.scenario)} | {sr.correct}/{sr.total} | "
            f"{_spread(sr.verdict_counts)} | {_spread(sr.top_hypothesis_counts)} |"
        )
    fpr = sweep.false_positive_rate
    controls = sweep.control_results
    fp_reps = sum(
        1 for s in controls for r in s.reps if r.outcome.value == "false_positive"
    )
    ctrl_reps = sum(s.total for s in controls)
    fpr_txt = "n/a" if fpr is None else f"{fp_reps}/{ctrl_reps} = {fpr:.0%}"
    ctrl_names = ", ".join(f"`{s.scenario.name}`" for s in controls)
    lines.append("")
    lines.append(
        f"**False-positive rate (controls {ctrl_names}): {fpr_txt}.** "
        "The two abstentions are distinct: `duplicate_flood` and `no_fault_baseline` "
        "abstain because nothing is wrong (the FP controls); `co_view_bug` abstains "
        "because the fault is undiagnosable from serving data by design (a labeled "
        "capability boundary — the co-view adjusted factor is a DECISIONS won't-do), "
        "so it is not folded into the FP rate."
    )
    return "\n".join(lines)


def near_miss_table(sweep: SweepResult) -> str:
    lines = [
        "| profile | ip_resolved_fraction | match_rate | top-hypothesis spread | "
        "verdict spread |",
        "|---|---|---|---|---|",
    ]
    for sr in sweep.near_miss:
        h = sr.headline
        lines.append(
            f"| `{sr.scenario.name}` | {_fmt(h.get('ip_resolved_fraction'))} | "
            f"{_fmt(h.get('match_rate'))} | {_spread(sr.top_hypothesis_counts)} | "
            f"{_spread(sr.verdict_counts)} |"
        )
    lines.append("")
    lines.append(
        "Both profiles raise reported ROAS, but the discriminator is "
        "`ip_resolved_fraction` — elevated on `shared_ip_spike` (wrong-household "
        "inflation → `device_graph_mismatch`), flat on `real_lift` (a genuine lift → "
        "`real_performance_change`). The agent must tell them apart on that named "
        'number, not on "ROAS went up".'
    )
    return "\n".join(lines)
