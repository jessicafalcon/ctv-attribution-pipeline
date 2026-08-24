"""Offline pin (Phase 18b): the Pushgateway helpers are a NO-OP unless
PUSHGATEWAY_URL is set. This is what keeps the golden / oracle / capture / offline
paths free of any dependency on a running gateway — they never set the env, so nothing
there pushes or resets."""

import pytest
from prometheus_client import CollectorRegistry, Gauge

from observability import push


def test_push_is_a_no_op_without_the_gateway_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
    assert push.gateway_url() is None

    # No env → no network, no error (would raise on a connect attempt otherwise).
    registry = CollectorRegistry()
    Gauge("probe_metric", "x", registry=registry).set(1.0)
    push.push_registry(registry, "probe")  # returns cleanly
    push.reset_gateway()  # returns cleanly


def test_gateway_url_reads_the_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUSHGATEWAY_URL", "http://127.0.0.1:9091")
    assert push.gateway_url() == "http://127.0.0.1:9091"
    monkeypatch.setenv("PUSHGATEWAY_URL", "")
    assert push.gateway_url() is None  # empty string is treated as unset
