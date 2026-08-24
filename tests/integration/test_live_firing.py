"""LIVE (Phase 18b, Done-when 4a): the alert firing PATH, proven in two legs, each a
bounded poll on a concrete state — never a fixed sleep, never a wall-clock timer.

1. Real data: a reconcile registry pushed to the Pushgateway (exactly as `make run`'s
   reconcile stage pushes it) is scraped by Prometheus and makes `RestatementMagnitude`
   ACTIVE — proving push → scrape → evaluate on live data, the "batch stages exit before
   a scrape" gap every prior phase deferred. (The `for: 5m` → firing → Alertmanager hop
   is promtool's job, proven deterministically via eval_time; not re-timed here.)
2. Synthetic delivery: a firing alert POSTed to Alertmanager's API is delivered
   AM → `agent/webhook.py` — proving the receiver is wired (the "alerts fire but never
   reach the agent" bug). Clearly synthetic (the 18a alerts_synthetic_test.yml precedent
   for the leg real data cannot drive fast). The sweep is mocked (zero tokens).

Runs under `make test-int-long-delay` (needs the pushgateway + alertmanager services).
"""

import json
import threading
import time
import urllib.request

import pytest
from prometheus_client import CollectorRegistry, Gauge

from observability.push import push_registry, reset_gateway

PROM = "http://127.0.0.1:9090"
ALERTMANAGER = "http://127.0.0.1:9093"
GATEWAY = "http://127.0.0.1:9091"
AGENT_PORT = 18099  # must match observability/alertmanager.yml's webhook url


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def _rule_state(name: str) -> str:
    groups = _get(f"{PROM}/api/v1/rules?type=alert")["data"]["groups"]
    for g in groups:
        for r in g["rules"]:
            if r.get("name") == name:
                return r.get("state", "unknown")
    return "absent"


def test_pushed_reconcile_metric_makes_restatementmagnitude_active_in_prometheus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Push the reconcile restatement gauge above the rule threshold (>1.0), exactly as
    # the reconcile stage does — with PUSHGATEWAY_URL set so this is a real push.
    monkeypatch.setenv("PUSHGATEWAY_URL", GATEWAY)
    reset_gateway()
    registry = CollectorRegistry()
    Gauge(
        "reconcile_restatement_roas_abs_delta",
        "largest abs ROAS restatement (test push)",
        registry=registry,
    ).set(41.42)
    push_registry(registry, "reconcile")

    # Bounded poll: Prometheus scrapes the gateway (15s) then evaluates the rule (15s).
    deadline = time.monotonic() + 90
    metric_seen = False
    state = "inactive"
    while time.monotonic() < deadline:
        result = _get(
            f"{PROM}/api/v1/query?query=reconcile_restatement_roas_abs_delta"
        )["data"]["result"]
        metric_seen = any(float(r["value"][1]) > 1.0 for r in result)
        state = _rule_state("RestatementMagnitude")
        if metric_seen and state in ("pending", "firing"):
            break
        time.sleep(3)

    assert metric_seen, "Prometheus never scraped the pushed metric from the gateway"
    assert state in ("pending", "firing"), f"rule never went active (state={state})"


def test_a_synthetic_firing_alert_is_delivered_from_alertmanager_to_the_agent() -> None:
    import uvicorn

    from agent import webhook
    from agent.finding import AttributionFinding
    from agent.hypotheses import Hypothesis

    received: list[str] = []

    def mock_sweep() -> AttributionFinding:  # no LLM, zero tokens
        received.append("hit")
        return AttributionFinding(
            profile="webhook",
            top_hypothesis=Hypothesis.DEVICE_GRAPH_MISMATCH,
            ranked=[],
            ruled_out=[],
            recommended_action="hold",
            verdict="CONFIDENT",
            probes_run=[],
        )

    webhook.app.dependency_overrides[webhook.get_sweep] = lambda: mock_sweep
    # 0.0.0.0 so the alertmanager container reaches it via host.docker.internal. SAFE
    # here only because this is a test-only bind with a MOCKED sweep, torn down in
    # finally; a STANDING agent webhook must be loopback-bound + authed, never 0.0.0.0
    # (BACKLOG "The live-firing test binds the agent webhook on 0.0.0.0").
    config = uvicorn.Config(
        webhook.app,
        host="0.0.0.0",  # noqa: S104 — test-only; see the BACKLOG note above
        port=AGENT_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        for _ in range(50):
            if server.started:
                break
            time.sleep(0.1)
        assert server.started, "the agent webhook did not start"

        alert = [
            {
                "labels": {"alertname": "SyntheticFiringProbe", "severity": "warning"},
                "annotations": {"summary": "synthetic — proves AM→agent delivery"},
                "startsAt": "2026-08-23T00:00:00.000Z",
                "endsAt": "2030-01-01T00:00:00.000Z",
            }
        ]
        req = urllib.request.Request(
            f"{ALERTMANAGER}/api/v2/alerts",
            data=json.dumps(alert).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        assert urllib.request.urlopen(req, timeout=10).status == 200

        deadline = time.monotonic() + 40
        while time.monotonic() < deadline and not received:
            time.sleep(1)
        assert received, "Alertmanager never delivered the alert to agent/webhook.py"
    finally:
        # Resolve the synthetic alert and stop the server.
        resolved = [
            {
                "labels": {"alertname": "SyntheticFiringProbe", "severity": "warning"},
                "startsAt": "2026-08-23T00:00:00.000Z",
                "endsAt": "2026-08-23T00:00:01.000Z",
            }
        ]
        try:
            urllib.request.urlopen(
                urllib.request.Request(
                    f"{ALERTMANAGER}/api/v2/alerts",
                    data=json.dumps(resolved).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                ),
                timeout=10,
            )
        except Exception:
            pass
        server.should_exit = True
        thread.join(timeout=10)
        webhook.app.dependency_overrides.clear()
