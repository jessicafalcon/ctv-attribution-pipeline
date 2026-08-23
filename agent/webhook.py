"""Alertmanager webhook endpoint (ARCHITECTURE §4.2 trigger). FastAPI app; a firing
alert triggers an agent sweep.

BOUNDARY (security, designed): the alert payload is a TRIGGER ONLY. The sweep
re-derives the `AttributionContext` deterministically from ClickHouse and does NOT
feed alert labels/annotations into the LLM prompt — alert labels are attacker-
influenceable, and re-observing from the DB is also the cleanest determinism story.
The alertname is echoed in the HTTP response only, never passed into the sweep.

Since Phase 18b the live scrape → Alertmanager → webhook push chain is wired (the
Pushgateway path, Done-when 4). To keep the LLM cost bounded under a real Alertmanager
(which posts one webhook per alert GROUP, each carrying many possibly-flapping
alerts), the handler dedupes by `groupKey`: ONE sweep per firing group, not per alert
(Invariant 7). The sweep re-observes the whole DB, so a second sweep for the same
group would only burn tokens (closed BACKLOG 'webhook sweep amplification')."""

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
    """The subset of the Alertmanager webhook payload we read. `groupKey` is
    Alertmanager's per-group identifier — one webhook per group (Phase 18b)."""

    status: str | None = None
    groupKey: str | None = None  # noqa: N815 — matches the Alertmanager wire field
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


def _firing(alert: Alert, payload: AlertmanagerWebhook) -> bool:
    return (alert.status or payload.status) == "firing"


def _dedupe_by_group_key(payload: AlertmanagerWebhook) -> list[str]:
    """The distinct alert GROUPS in this webhook with ≥1 FIRING alert — one sweep
    each (Invariant 7). Alertmanager posts one webhook per group, so this is normally
    a single-element list; deduping bounds the sweep at one-per-group even when a
    group carries many duplicate or flapping alerts. A missing `groupKey` (hand-posted
    payloads, tests) collapses to a single 'default' group so one firing webhook still
    triggers exactly one sweep."""
    groups: list[str] = []
    for alert in payload.alerts:
        if not _firing(alert, payload):
            continue
        key = payload.groupKey or "default"
        if key not in groups:
            groups.append(key)
    return groups


@app.post("/alerts")
def alerts(
    payload: AlertmanagerWebhook,
    sweep: Annotated[Callable[[], AttributionFinding], Depends(get_sweep)],
) -> dict:
    """Trigger ONE sweep per firing alert GROUP, not per alert (Phase 18b, Invariant
    7). Firing alertnames are read only to echo them back; they are never passed into
    `sweep()` (trigger-only boundary, Invariant 8)."""
    handled = []
    for group_key in _dedupe_by_group_key(payload):
        finding = sweep()
        alertnames = sorted(
            {
                a.labels.get("alertname", "unknown")
                for a in payload.alerts
                if _firing(a, payload)
            }
        )
        handled.append(
            {
                "group_key": group_key,
                "alertnames": alertnames,
                "verdict": finding.verdict,
                "top_hypothesis": finding.top_hypothesis,
            }
        )
    return {"handled": handled}
