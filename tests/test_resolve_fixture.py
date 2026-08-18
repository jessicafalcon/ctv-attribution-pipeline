"""Replaying the frozen tiny fixture reproduces the golden expected resolved
file byte for byte, and the resolve-case counts are pinned so a regen cannot
silently drop a branch or the candidate_count=3 fan-out. Read-only ground
truth after Phase 2 (mirrors tests/test_fixtures.py for the producer)."""

from collections import Counter
from pathlib import Path

from producer.models import Conversion, Household
from producer.serialize import jsonl
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"
EXPECTED = FIXTURES / "expected" / "conversions_resolved.jsonl"


def _read(name: str, model: type) -> list:
    return [
        model.model_validate_json(line)
        for line in (FIXTURES / name).read_text().splitlines()
    ]


def _resolved():
    index = GraphIndex.from_households(_read("device_graph.jsonl", Household))
    return resolve_stream(_read("conversions.jsonl", Conversion), index)


def test_replay_reproduces_expected_fixture() -> None:
    assert EXPECTED.read_text() == jsonl(_resolved())


def test_resolved_counts_pinned() -> None:
    rows = _resolved()
    # Raw rows (stateless map over the duplicate-carrying stream).
    assert len(rows) == 68
    by_resolution = Counter(r.resolution for r in rows)
    assert by_resolution == {"device": 44, "ip": 24}
    assert sum(r.ambiguous for r in rows) == 11

    # Per distinct conversion_id (duplicates collapsed) — same basis as
    # tests/test_fixtures.py: 38 device + 12 unique-IP + 5 ambiguous.
    by_conv: dict[str, list] = {}
    for r in rows:
        by_conv.setdefault(r.conversion_id, []).append(r)
    assert len(by_conv) == 55

    resolutions = {cid: rs[0].resolution for cid, rs in by_conv.items()}
    ambiguous = {cid: rs[0].ambiguous for cid, rs in by_conv.items()}
    device = [c for c, res in resolutions.items() if res == "device"]
    unique_ip = [
        c for c, res in resolutions.items() if res == "ip" and not ambiguous[c]
    ]
    ambiguous_ip = [c for c in by_conv if ambiguous[c]]
    assert len(device) == 38
    assert len(unique_ip) == 12
    assert len(ambiguous_ip) == 5

    # Fan-out shape per distinct ambiguous conversion: 4 with 2 owners, 1 with 3.
    fan_out = Counter(by_conv[c][0].candidate_count for c in ambiguous_ip)
    assert fan_out == {2: 4, 3: 1}


def test_every_resolved_household_is_real() -> None:
    households = {h.household_id for h in _read("device_graph.jsonl", Household)}
    assert all(r.household_id in households for r in _resolved())
