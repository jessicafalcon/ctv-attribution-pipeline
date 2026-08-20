"""`make agent-run PROFILE=<fault>` — run the agent loop once against a live stack and
print the finding. This is the ONE end-to-end path that calls the LLM; it costs API
tokens (ARCHITECTURE §2), so CLAUDE.md gates it behind developer approval — every
other agent test mocks the client.

Reads as `agent_ro` (SELECT-only, Ruling E): the collectors AND the probes go through
the same read-only handle. Prints the typed `AttributionFinding` and the turn-2
`cache_read_input_tokens` (Ruling B — prefix caching, expected > 0 given the ≥1-probe
loop contract guarantees a turn 2)."""

import argparse

from anthropic import Anthropic

from agent.finding import AttributionFinding
from agent.loop import AgentRun, run_agent
from agent.run_context import collect
from clickhouse.client import connect_agent


def format_finding(finding: AttributionFinding) -> str:
    lines = [
        f"attribution finding — profile {finding.profile}",
        "-" * 60,
        f"  verdict        : {finding.verdict}",
        f"  top hypothesis : {finding.top_hypothesis}",
        f"  probes run     : {', '.join(finding.probes_run) or '(none)'}",
        f"  action         : {finding.recommended_action}",
        "",
        "  ranked:",
    ]
    for r in finding.ranked:
        lines.append(f"    [{r.weight:.2f}] {r.hypothesis}")
        for e in r.evidence_for:
            lines.append(f"        + {e}")
        for e in r.evidence_against:
            lines.append(f"        - {e}")
    if finding.ruled_out:
        lines.append("  ruled out: " + ", ".join(finding.ruled_out))
    return "\n".join(lines)


def _cache_line(run: AgentRun) -> str:
    if len(run.usages) < 2:
        return "cache_read_input_tokens (turn 2): n/a (loop ended in one turn)"
    cr = getattr(run.usages[1], "cache_read_input_tokens", 0) or 0
    return f"cache_read_input_tokens (turn 2): {cr}"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="run the attribution-integrity agent")
    parser.add_argument("--profile", default="shared_ip_spike")
    args = parser.parse_args(argv)

    ch = connect_agent()
    ctx = collect(ch, args.profile)
    run = run_agent(ch, ctx, Anthropic())

    print(format_finding(run.finding))
    print()
    print(_cache_line(run))


if __name__ == "__main__":
    main()
