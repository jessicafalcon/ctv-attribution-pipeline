"""Guard the golden fixtures: regenerating the tiny profile must reproduce
fixtures/tiny byte for byte. Read-only ground truth after Phase 1."""

from collections import Counter
from pathlib import Path

import pytest

from producer.config import load_profile
from producer.generate import generate
from producer.models import Conversion, Household
from producer.serialize import jsonl

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"


def _fixture_households() -> list[Household]:
    return [
        Household.model_validate_json(line)
        for line in (FIXTURES / "device_graph.jsonl").read_text().splitlines()
    ]


def _fixture_conversion_rows() -> list[Conversion]:
    return [
        Conversion.model_validate_json(line)
        for line in (FIXTURES / "conversions.jsonl").read_text().splitlines()
    ]


@pytest.mark.parametrize(
    "filename,field",
    [
        ("device_graph.jsonl", "graph"),
        ("exposures.jsonl", "exposures"),
        ("conversions.jsonl", "conversions"),
        ("truth_links.jsonl", "truth_links"),
    ],
)
def test_tiny_fixture_is_reproducible(filename: str, field: str) -> None:
    profile = load_profile("tiny")
    stream = generate(profile, profile.seed)
    data = stream.graph.households if field == "graph" else getattr(stream, field)
    assert (FIXTURES / filename).read_text() == jsonl(data)


def test_tiny_fixture_covers_all_resolve_cases() -> None:
    """Phase-1 → Phase-2 contract: the frozen fixture must reach every resolve
    branch. Counts are per distinct conversion_id (duplicates collapsed, the
    basis DECISIONS.md documents) and pinned exactly so a regen cannot
    silently lose a case or the candidate_count=3 fan-out shape."""
    households = _fixture_households()
    # dict keyed by conversion_id collapses duplicate rows.
    conversions = {c.conversion_id: c for c in _fixture_conversion_rows()}
    graph_devices = {d.device_id for h in households for d in h.devices}
    ip_owners: dict[str, set[str]] = {}
    for h in households:
        for ip in h.ips:
            ip_owners.setdefault(ip, set()).add(h.household_id)

    device_hit = [c for c in conversions.values() if c.device_id in graph_devices]
    unknown = [c for c in conversions.values() if c.device_id not in graph_devices]
    unknown_unique_ip = [c for c in unknown if len(ip_owners[c.ip]) == 1]
    unknown_shared_ip = [c for c in unknown if len(ip_owners[c.ip]) >= 2]

    assert len(device_hit) == 38
    assert len(unknown_unique_ip) == 12
    assert len(unknown_shared_ip) == 5
    fan_out_shapes = Counter(len(ip_owners[c.ip]) for c in unknown_shared_ip)
    assert fan_out_shapes == {2: 4, 3: 1}


def test_tiny_fixture_unknown_device_namespace() -> None:
    """u- ids are disjoint from the graph, unique per conversion, and stable
    across duplicate rows — an unknown device must never read as a duplicate
    (BACKLOG, Phase 5 trigger)."""
    households = _fixture_households()
    rows = _fixture_conversion_rows()
    graph_devices = {d.device_id for h in households for d in h.devices}

    assert not any(d.startswith("u-") for d in graph_devices)
    device_by_conversion: dict[str, set[str]] = {}
    for c in rows:
        device_by_conversion.setdefault(c.conversion_id, set()).add(c.device_id)
    # Duplicate rows of one conversion carry the same device id.
    assert all(len(devices) == 1 for devices in device_by_conversion.values())
    u_ids = [
        next(iter(devices))
        for devices in device_by_conversion.values()
        if next(iter(devices)).startswith("u-")
    ]
    assert u_ids and len(u_ids) == len(set(u_ids))
    assert all(u not in graph_devices for u in u_ids)
