"""Resolver branches on a hand-built graph — device hit, unique-IP fallback,
ambiguous-IP fan-out, unresolvable. No services."""

from datetime import UTC, datetime

from producer.models import Conversion, Device, Household
from resolve.index import GraphIndex
from resolve.resolver import resolve_one, resolve_stream

T = datetime(2026, 8, 1, tzinfo=UTC)


def _graph() -> GraphIndex:
    return GraphIndex.from_households(
        [
            # h-a and h-b share 100.64.0.1; h-c owns 10.0.0.9 alone.
            Household(
                household_id="h-a",
                devices=[Device(device_id="d-a-0", kind="tv")],
                ips=["10.0.0.1", "100.64.0.1"],
            ),
            Household(
                household_id="h-b",
                devices=[Device(device_id="d-b-0", kind="phone")],
                ips=["100.64.0.1"],
            ),
            Household(
                household_id="h-c",
                devices=[Device(device_id="d-c-0", kind="laptop")],
                ips=["10.0.0.9"],
            ),
        ]
    )


def _conv(device_id: str, ip: str, cid: str = "c-1") -> Conversion:
    return Conversion(
        conversion_id=cid,
        event_time=T,
        ingest_time=T,
        device_id=device_id,
        ip=ip,
        conversion_type="site_visit",
        revenue=0.0,
        order_id=None,
    )


def test_device_hit_beats_ip() -> None:
    # Known device on a SHARED ip still resolves by device, not fan-out.
    (r,) = resolve_one(_conv("d-a-0", "100.64.0.1"), _graph())
    assert (r.household_id, r.resolution, r.ambiguous, r.candidate_count) == (
        "h-a",
        "device",
        False,
        1,
    )


def test_unique_ip_fallback() -> None:
    (r,) = resolve_one(_conv("u-999", "10.0.0.9"), _graph())
    assert (r.household_id, r.resolution, r.ambiguous, r.candidate_count) == (
        "h-c",
        "ip",
        False,
        1,
    )


def test_ambiguous_ip_fans_out_sorted() -> None:
    rs = resolve_one(_conv("u-999", "100.64.0.1"), _graph())
    assert [r.household_id for r in rs] == ["h-a", "h-b"]  # sorted, deterministic
    assert all(r.resolution == "ip" and r.ambiguous for r in rs)
    assert {r.candidate_count for r in rs} == {2}
    # Same conversion_id on every fan-out record — the engine dedups later.
    assert {r.conversion_id for r in rs} == {"c-1"}


def test_unresolvable_emits_nothing() -> None:
    assert resolve_one(_conv("u-999", "203.0.113.7"), _graph()) == []


def test_resolved_carries_conversion_fields() -> None:
    conv = _conv("d-c-0", "10.0.0.9")
    (r,) = resolve_one(conv, _graph())
    assert r.conversion_id == conv.conversion_id
    assert r.event_time == conv.event_time
    assert r.conversion_type == conv.conversion_type


def test_stream_is_stateless_duplicates_in_duplicates_out() -> None:
    conv = _conv("d-a-0", "10.0.0.1")
    out = resolve_stream([conv, conv], _graph())
    assert len(out) == 2 and out[0].model_dump() == out[1].model_dump()
