"""Offline coverage for metrics.observe() — all three branches, no services.
The empty-list (unresolved) branch is unreachable from the tiny fixture (every
conversion there resolves), so this is its only guard. Counters are module
globals, so reset around each case."""

from datetime import UTC, datetime

import pytest

from producer.models import ResolvedConversion
from resolve import metrics

T = datetime(2026, 8, 1, tzinfo=UTC)
_COUNTERS = (metrics.CONSUMED, metrics.RESOLVED, metrics.UNRESOLVED, metrics.AMBIGUOUS)


def _reset() -> None:
    for c in _COUNTERS:
        c._value.set(0)
    for resolution in ("device", "ip"):
        metrics.EMITTED.labels(resolution=resolution)._value.set(0)


@pytest.fixture(autouse=True)
def _clean_counters() -> None:
    _reset()
    yield
    _reset()


def _record(resolution: str, ambiguous: bool, candidate_count: int, household: str):
    return ResolvedConversion(
        conversion_id="c-1",
        event_time=T,
        ingest_time=T,
        device_id="d-x",
        ip="10.0.0.1",
        conversion_type="site_visit",
        revenue=0.0,
        order_id=None,
        household_id=household,
        resolution=resolution,
        ambiguous=ambiguous,
        candidate_count=candidate_count,
    )


def _val(counter, **labels) -> float:
    return (counter.labels(**labels) if labels else counter)._value.get()


def test_unresolved_branch() -> None:
    metrics.observe([])
    assert _val(metrics.CONSUMED) == 1
    assert _val(metrics.UNRESOLVED) == 1
    assert _val(metrics.RESOLVED) == 0
    assert _val(metrics.AMBIGUOUS) == 0
    assert _val(metrics.EMITTED, resolution="device") == 0
    assert _val(metrics.EMITTED, resolution="ip") == 0


def test_single_device_hit() -> None:
    metrics.observe([_record("device", False, 1, "h-0")])
    assert _val(metrics.CONSUMED) == 1
    assert _val(metrics.RESOLVED) == 1
    assert _val(metrics.UNRESOLVED) == 0
    assert _val(metrics.AMBIGUOUS) == 0
    assert _val(metrics.EMITTED, resolution="device") == 1
    assert _val(metrics.EMITTED, resolution="ip") == 0


def test_ambiguous_two_candidate_fan_out() -> None:
    metrics.observe([_record("ip", True, 2, "h-0"), _record("ip", True, 2, "h-1")])
    assert _val(metrics.CONSUMED) == 1
    assert _val(metrics.RESOLVED) == 1
    assert _val(metrics.AMBIGUOUS) == 1
    assert _val(metrics.UNRESOLVED) == 0
    assert _val(metrics.EMITTED, resolution="ip") == 2
    assert _val(metrics.EMITTED, resolution="device") == 0
