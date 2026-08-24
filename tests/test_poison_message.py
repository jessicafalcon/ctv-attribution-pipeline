"""Poison-message disposition (BACKLOG 87, fix/poison-message-disposition).

Invariant: for all drained payloads, a message that fails schema decode/validation
HALTS the pass — it is never skipped, never partially processed. A silent
skip-and-continue would break the byte-identical determinism guarantee (the same
seed no longer reproduces the same output once one message is dropped).

`common.kafka.drain` returns raw bytes; consume-time validation lives in the
callers' bare `model_validate_json` comprehensions. This pins the invariant at
the THREE REAL sites by patching the drain seam and calling the production
functions — a malformed element mid-batch must raise `ValidationError` out of the
real code path, not be swallowed:

- `streaming.dataflow.run_engine`  → exposures decode (dataflow.py:157) and
  conversions decode (dataflow.py:163), seam = `streaming.dataflow._drain_topic`
- `resolve.graph_loader.load_graph_index` → Household decode (graph_loader.py:37),
  seam = `resolve.graph_loader.drain`

`pytest.raises(ValidationError)` is the assertion that gives this its teeth: the
correct code raises a pydantic `ValidationError` at the decode comprehension
before any broker is touched, so the committed test never connects. A regression
that wraps a decode site in `try/except: continue` would NOT raise a
`ValidationError` there — it swallows the poison row and falls through past the
decode, so `pytest.raises` goes red. The two `run_engine` tests also stub
`load_graph_index` (`_no_resolve`) so that a swallowed row fails as a crisp
`AssertionError` at the site under test rather than via an incidental
unresolvable-broker call downstream. Offline: no broker, no services (the patched
seams keep every path off the network).
"""

import pytest
from pydantic import ValidationError

from resolve import graph_loader
from resolve.index import GraphIndex
from streaming import dataflow

# Well-formed wire payloads (bytes, as the drain returns). Every required field
# present, types valid.
_GOOD_EXPOSURE = (
    b'{"exposure_id": "e1", "event_time": "2026-01-01T00:00:00Z",'
    b' "ingest_time": "2026-01-01T00:00:01Z", "campaign_id": "c1",'
    b' "household_id": "h1", "ip": "10.0.0.1", "app_id": "a1",'
    b' "program_genre": "news", "spend": 1.5}'
)
_GOOD_CONVERSION = (
    b'{"conversion_id": "cv1", "event_time": "2026-01-01T00:00:00Z",'
    b' "ingest_time": "2026-01-01T00:00:01Z", "device_id": "d1",'
    b' "ip": "10.0.0.1", "conversion_type": "purchase", "revenue": 9.99,'
    b' "order_id": "o1"}'
)
_GOOD_HOUSEHOLD = (
    b'{"household_id": "h1", "devices": [{"device_id": "d1", "kind": "tv"}],'
    b' "ips": ["10.0.0.1"]}'
)
# Malformed: valid JSON, but missing required fields (and a negative spend) —
# fails schema validation exactly as a corrupted/poison wire byte would.
_MALFORMED = b'{"exposure_id": "e2", "spend": -1}'


def _no_resolve(broker):
    """Stand-in for `load_graph_index` in the run_engine tests: reaching resolve
    means the poison row was swallowed and decode did NOT halt. Raise a crisp,
    broker-free error so a skip-and-continue regression fails as an assertion at
    the site under test (not `ValidationError`, so `pytest.raises` still goes red)
    rather than via an incidental unresolvable-broker call."""
    raise AssertionError(
        "decode must halt before resolve; the poison row was swallowed"
    )


def test_run_engine_halts_on_malformed_exposure(monkeypatch) -> None:
    """A poison exposure mid-drain raises out of the real exposures decode
    (dataflow.py:157) — run_engine does not skip it and carry on."""

    def fake_drain(broker, topic, group):
        if topic == dataflow.EXPOSURES_TOPIC:
            return [_GOOD_EXPOSURE, _MALFORMED, _GOOD_EXPOSURE]
        return []

    monkeypatch.setattr(dataflow, "_drain_topic", fake_drain)
    monkeypatch.setattr(dataflow, "load_graph_index", _no_resolve)
    with pytest.raises(ValidationError):
        dataflow.run_engine("unused:9092")


def test_run_engine_halts_on_malformed_conversion(monkeypatch) -> None:
    """A poison conversion mid-drain raises out of the real conversions decode
    (dataflow.py:163). Exposures decode cleanly first, so this pins the second
    site specifically; the decode runs before resolve, so no broker is reached."""

    def fake_drain(broker, topic, group):
        if topic == dataflow.EXPOSURES_TOPIC:
            return [_GOOD_EXPOSURE, _GOOD_EXPOSURE]
        return [_GOOD_CONVERSION, _MALFORMED, _GOOD_CONVERSION]

    monkeypatch.setattr(dataflow, "_drain_topic", fake_drain)
    monkeypatch.setattr(dataflow, "load_graph_index", _no_resolve)
    with pytest.raises(ValidationError):
        dataflow.run_engine("unused:9092")


def test_load_graph_index_halts_on_malformed_household(monkeypatch) -> None:
    """A poison Household in the compacted-topic drain raises out of the real
    graph-loader decode (graph_loader.py:37). The Consumer is constructed but
    never connects (the patched drain returns before any poll)."""
    monkeypatch.setattr(graph_loader, "drain", lambda consumer, topic: [_MALFORMED])
    with pytest.raises(ValidationError):
        graph_loader.load_graph_index("unused:9092")


def test_load_graph_index_control_good_household(monkeypatch) -> None:
    """Control: the SAME real path over a clean payload returns a GraphIndex, so
    the raise above is the poison row and not the harness or the patched seam."""
    monkeypatch.setattr(
        graph_loader, "drain", lambda consumer, topic: [_GOOD_HOUSEHOLD]
    )
    assert isinstance(graph_loader.load_graph_index("unused:9092"), GraphIndex)
