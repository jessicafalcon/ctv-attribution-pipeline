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
