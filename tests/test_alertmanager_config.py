"""Offline config pin (Phase 18b, Done-when 4a — CONFIG leg): the Alertmanager
webhook receiver is wired to the agent's /alerts endpoint. A null/mis-wired receiver
is the "alerts fire but never reach the agent" bug; this catches it without a stack."""

import re
from pathlib import Path

AM_CONFIG = Path(__file__).parent.parent / "observability" / "alertmanager.yml"


def test_the_webhook_receiver_points_at_the_agent() -> None:
    text = AM_CONFIG.read_text()
    # The route delivers to a named receiver, not the boot-time "null".
    receiver = re.search(r"(?m)^\s*receiver:\s*(\S+)", text).group(1)
    assert receiver != "null", "alertmanager route still points at the null receiver"
    assert re.search(rf"(?m)^\s*-\s*name:\s*{re.escape(receiver)}\b", text), (
        f"route names receiver {receiver!r} but no such receiver is defined"
    )
    # That receiver has a webhook_configs url ending in /alerts (agent/webhook.py).
    assert re.search(r"webhook_configs:", text), "receiver has no webhook_configs"
    url = re.search(r"(?m)^\s*-?\s*url:\s*\"?(\S+?)\"?\s*$", text).group(1)
    assert url.endswith("/alerts"), f"webhook url does not target /alerts: {url!r}"
