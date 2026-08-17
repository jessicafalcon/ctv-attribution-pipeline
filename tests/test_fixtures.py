"""Guard the golden fixtures: regenerating the tiny profile must reproduce
fixtures/tiny byte for byte. Read-only ground truth after Phase 1."""

from pathlib import Path

import pytest

from producer.config import load_profile
from producer.generate import generate
from producer.serialize import jsonl

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"


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
    branch — device hit, unknown device on a unique IP (single-household IP
    fallback), unknown device on a shared IP (ambiguous fan-out)."""
    from producer.models import Conversion, Household

    households = [
        Household.model_validate_json(line)
        for line in (FIXTURES / "device_graph.jsonl").read_text().splitlines()
    ]
    conversions = {
        c.conversion_id: c
        for c in (
            Conversion.model_validate_json(line)
            for line in (FIXTURES / "conversions.jsonl").read_text().splitlines()
        )
    }
    graph_devices = {d.device_id for h in households for d in h.devices}
    ip_owners: dict[str, set[str]] = {}
    for h in households:
        for ip in h.ips:
            ip_owners.setdefault(ip, set()).add(h.household_id)

    device_hit = [c for c in conversions.values() if c.device_id in graph_devices]
    unknown = [c for c in conversions.values() if c.device_id not in graph_devices]
    unknown_unique_ip = [c for c in unknown if len(ip_owners[c.ip]) == 1]
    unknown_shared_ip = [c for c in unknown if len(ip_owners[c.ip]) >= 2]

    assert device_hit, "fixture has no device-hit conversion"
    assert unknown_unique_ip, "fixture has no unknown-device conversion on a unique IP"
    assert unknown_shared_ip, (
        "fixture has no unknown-device conversion on a shared IP (ambiguous fan-out)"
    )
