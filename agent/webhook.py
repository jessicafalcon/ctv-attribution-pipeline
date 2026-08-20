"""Alertmanager webhook endpoint (ARCHITECTURE §4.2 trigger). FastAPI app; a firing
alert triggers an agent sweep.

BOUNDARY (security, designed): the alert payload is a TRIGGER ONLY. The sweep
re-derives the `AttributionContext` deterministically from ClickHouse and does NOT
feed alert labels/annotations into the LLM prompt — alert labels are attacker-
influenceable, and re-observing from the DB is also the cleanest determinism story.
The alertname is echoed in the HTTP response only, never passed into the sweep.

The live scrape → Alertmanager → webhook push chain is deferred (BACKLOG): the batch
stages exit before a scrape, so this endpoint is built and tested (with the LLM
mocked, zero tokens) but not wired to a running Alertmanager. "One sweep per firing
alert" is a token-amplification vector once the push lands — BACKLOG carries the
trigger to dedupe/bound sweeps by alertname/fingerprint before wiring it."""

from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, FastAPI
from pydantic import BaseModel

from agent.finding import AttributionFinding

app = FastAPI(title="attribution-integrity agent")


class Alert(BaseModel):
    status: str | None = None
    labels: dict[str, str] = {}
    annotations: dict[str, str] = {}


class AlertmanagerWebhook(BaseModel):
    """The subset of the Alertmanager webhook payload we read."""

    status: str | None = None
    alerts: list[Alert] = []


def run_sweep() -> AttributionFinding:
    """The real sweep: re-observe from ClickHouse (as agent_ro) and run the loop with
    a live Anthropic client. Costs API tokens — reached only under a live push, which
    is deferred. Imports are local so importing the app (and the tests) needs no
    Anthropic client or live DB."""
    from anthropic import Anthropic

    from agent.loop import run_agent
    from agent.run_context import collect
    from clickhouse.client import connect_agent

    ch = connect_agent()
    ctx = collect(ch, "webhook")
    return run_agent(ch, ctx, Anthropic()).finding


def get_sweep() -> Callable[[], AttributionFinding]:
    """Dependency seam: tests override this with a mocked sweep so the endpoint is
    exercised end to end with zero tokens."""
    return run_sweep


@app.post("/alerts")
def alerts(
    payload: AlertmanagerWebhook,
    sweep: Annotated[Callable[[], AttributionFinding], Depends(get_sweep)],
) -> dict:
    """Trigger one sweep per FIRING alert. The alertname is read only to echo it back;
    it is never passed into `sweep()` (trigger-only boundary)."""
    handled = []
    for alert in payload.alerts:
        if (alert.status or payload.status) != "firing":
            continue
        finding = sweep()
        handled.append(
            {
                "alertname": alert.labels.get("alertname", "unknown"),
                "verdict": finding.verdict,
                "top_hypothesis": finding.top_hypothesis,
            }
        )
    return {"handled": handled}
