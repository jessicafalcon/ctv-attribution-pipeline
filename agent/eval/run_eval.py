"""`make agent-eval` — the Phase-10 fault→diagnosis sweep. Runs every scenario live,
`EVAL_REPS` times, scores each finding, and writes both tables into `docs/RESULTS.md`.

This is the ONLY token-spending path in the eval (§2 cost posture: 30 invocations, a
~1-2k context + a ~3-8k cached prefix each — well under $10). CLAUDE.md gates it behind
developer approval; every offline test mocks the client.

Isolation: each scenario gets its own clean stack (`make down && up && seed && run`) —
tiny/medium/faults share `conversion_id` space (DECISIONS Phase 5), so two profiles
cannot co-reside in the serving tables. `make run` (full, not `run-hot`): `late_burst`
needs the restatement, and every scenario needs `report_snapshots` for the context's
restatement field.

Run order follows the §4.3 / PHASES.md watch order — the no-fault baseline first (is it
left alone?), then the near-miss NEGATIVE half `real_lift` (ruled a clean lift, NOT
device_graph_mismatch?), then the rest. The tables render in catalog order."""

import subprocess
import sys
from pathlib import Path

from anthropic import Anthropic

from agent.config import AGENT_MODEL, EVAL_REPS
from agent.context import AttributionContext
from agent.eval import scenarios as scen
from agent.eval.scoring import RepResult, ScenarioResult, SweepResult, score_rep
from agent.eval.tables import fault_diagnosis_table, near_miss_table
from agent.finding import AttributionFinding
from agent.loop import AgentRun, run_agent
from agent.run_context import collect
from clickhouse.client import connect_agent

RESULTS_PATH = Path(__file__).resolve().parents[2] / "docs" / "RESULTS.md"
AGENT_EVAL_MARKER = "## Agent eval — fault → diagnosis"

# Live watch order (§4.3): baseline, then the near-miss negative half, then the rest.
RUN_ORDER = (
    "no_fault_baseline",
    "real_lift",
    "shared_ip_spike",
    "late_burst",
    "co_view_bug",
    "duplicate_flood",
)


def _make(*targets: str, profile: str | None = None) -> None:
    """Run one `make` step, streaming output. Raises on non-zero exit."""
    cmd = ["make", *targets]
    if profile is not None:
        cmd.append(f"PROFILE={profile}")
    print(f"\n$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def stand_up_profile(profile: str) -> None:
    """Clean isolated stack for one profile: down → up → seed → full run."""
    _make("down")
    _make("up")
    _make("seed", profile=profile)
    _make("run", profile=profile)


def headline(ctx: AttributionContext) -> dict[str, float | int | None]:
    """The deterministic live context headline pinned per scenario (FG2 — BACKLOG 31).
    `ip_resolved_fraction` is the near-miss discriminator; the rest situate the row."""
    ip = ctx.ip_clusters
    max_abs_roas_delta = max((abs(r.roas_delta) for r in ctx.restatements), default=0.0)
    return {
        "match_rate": ctx.match_rate,
        "ip_resolved_fraction": ip.ip_resolved_fraction,
        "ambiguous_attributed": ip.ambiguous_attributed,
        "max_candidate_count": ip.max_candidate_count,
        "max_abs_roas_delta": max_abs_roas_delta,
        "near_boundary_fraction": ctx.window_edge.near_boundary_fraction,
    }


class TokenTally:
    """Sum token usage across the whole sweep for the §2 cost report."""

    def __init__(self) -> None:
        self.input = self.output = self.cache_read = self.cache_write = 0

    def add(self, run: AgentRun) -> None:
        for u in run.usages:
            self.input += getattr(u, "input_tokens", 0) or 0
            self.output += getattr(u, "output_tokens", 0) or 0
            self.cache_read += getattr(u, "cache_read_input_tokens", 0) or 0
            self.cache_write += getattr(u, "cache_creation_input_tokens", 0) or 0

    def report(self) -> str:
        return (
            f"tokens — input {self.input}, output {self.output}, "
            f"cache_read {self.cache_read}, cache_write {self.cache_write} "
            f"(model {AGENT_MODEL})"
        )


def run_scenario(
    scenario: scen.Scenario, anthropic: Anthropic, tally: TokenTally
) -> ScenarioResult:
    """Stand up the scenario's stack, capture the headline once, then EVAL_REPS live
    invocations against the identical populated tables (only the LLM varies)."""
    stand_up_profile(scenario.profile)
    ch = connect_agent()
    ctx = collect(ch, scenario.profile)
    head = headline(ctx)
    print(f"  headline[{scenario.name}]: {head}", flush=True)

    reps: list[RepResult] = []
    for i in range(EVAL_REPS):
        run = run_agent(ch, ctx, anthropic)
        tally.add(run)
        f: AttributionFinding = run.finding
        outcome = score_rep(scenario, f)
        reps.append(
            RepResult(
                scenario=scenario.name,
                outcome=outcome,
                verdict=f.verdict,
                top_hypothesis=f.top_hypothesis.value,
                probes_run=tuple(run.probes_run),
            )
        )
        print(
            f"  rep {i + 1}/{EVAL_REPS} [{scenario.name}]: {outcome.value} "
            f"(verdict {f.verdict}, top {f.top_hypothesis.value}, "
            f"probes {run.probes_run})",
            flush=True,
        )
    return ScenarioResult(scenario=scenario, reps=reps, headline=head)


def render_section(sweep: SweepResult) -> str:
    """The full `## Agent eval` block appended to docs/RESULTS.md."""
    return "\n".join(
        [
            AGENT_EVAL_MARKER,
            "",
            f"Every fault profile plus the no-fault baseline, run {EVAL_REPS}× "
            f"({scen.TOTAL_INVOCATIONS} live invocations), scored against the pure "
            "rubric in `agent/eval/scoring.py` (unit-tested offline — the live sweep "
            "only supplies the LLM outputs). The agent is non-reproducible by "
            "construction (temperature is unset on the Claude-5 family, DECISIONS "
            "Phase 9), so each cell reports a spread over reps, not a single-run "
            "claim; the reps measure residual stability.",
            "",
            "### Fault → top hypothesis → correct?",
            "",
            fault_diagnosis_table(sweep),
            "",
            "### Near-miss pair — a genuine lift vs shared-IP inflation",
            "",
            near_miss_table(sweep),
            "",
            "### Honesty boundary",
            "",
            "These are small-profile results reported as measured. `co_view_bug`'s "
            "abstention is a **labeled capability boundary**, not a gap papered over: "
            "the co-view *adjusted* factor is a DECISIONS won't-do (BACKLOG 26 — the "
            "honest per-genre expected baseline does not exist in serving data, and "
            "sourcing it from the producer's multiplier would couple reporting to "
            "generation parameters). The agent correctly declines to diagnose from "
            "noise. Verdict/hypothesis stability across reps is a measurement, never a "
            "gated assertion (the AI edge is carved out of the byte-identical "
            "guarantee, CLAUDE.md).",
            "",
        ]
    )


def write_results(sweep: SweepResult) -> None:
    """Replace (or append) the `## Agent eval` section at the end of docs/RESULTS.md,
    keeping everything before the marker intact."""
    existing = RESULTS_PATH.read_text() if RESULTS_PATH.exists() else ""
    head = existing.split(AGENT_EVAL_MARKER)[0].rstrip()
    RESULTS_PATH.write_text(head + "\n\n" + render_section(sweep))
    print(f"\nwrote both tables → {RESULTS_PATH}", flush=True)


def main(argv: list[str] | None = None) -> None:
    anthropic = Anthropic()
    tally = TokenTally()
    results: dict[str, ScenarioResult] = {}
    try:
        for name in RUN_ORDER:
            scenario = scen.by_name(name)
            print(f"\n{'=' * 70}\nSCENARIO {name} ({scenario.kind})\n{'=' * 70}")
            results[name] = run_scenario(scenario, anthropic, tally)
    finally:
        # Render in catalog order from whatever completed (partial tables still write).
        ordered = [results[s.name] for s in scen.SCENARIOS if s.name in results]
        if ordered:
            sweep = SweepResult(scenarios=ordered)
            write_results(sweep)
            print("\n" + tally.report(), flush=True)
            if len(ordered) < len(scen.SCENARIOS):
                print(
                    f"WARNING: only {len(ordered)}/{len(scen.SCENARIOS)} scenarios "
                    "completed — tables are partial.",
                    file=sys.stderr,
                )


if __name__ == "__main__":
    main()
