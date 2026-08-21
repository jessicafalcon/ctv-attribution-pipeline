"""Attributing the frozen tiny fixture reproduces the golden expected file byte
for byte, with the attribution counts pinned so a regen cannot silently drop a
case or credit an ambiguous conversion hot. Re-frozen once in Phase 16 (the one
sanctioned exception, DECISIONS Phase 16); read-only otherwise (mirrors
tests/test_resolve_fixture.py)."""

from pathlib import Path

from pydantic import BaseModel

from producer.models import AttributedConversion, Exposure, ResolvedConversion
from producer.serialize import jsonl
from streaming.attribute import attribute

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"
EXPECTED = FIXTURES / "expected" / "attributed.jsonl"


def _read[M: BaseModel](name: str, model: type[M]) -> list[M]:
    return [
        model.model_validate_json(line)
        for line in (FIXTURES / name).read_text().splitlines()
    ]


def _attributed() -> list[AttributedConversion]:
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    return attribute(exposures, resolved)


def test_replay_reproduces_expected_fixture() -> None:
    assert EXPECTED.read_text() == jsonl(_attributed())


def test_attribute_is_deterministic() -> None:
    # The determinism policy, encoded directly: same inputs → byte-identical
    # canonical output, run to run (independent of the frozen golden file).
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    assert jsonl(attribute(exposures, resolved)) == jsonl(
        attribute(exposures, resolved)
    )


def test_attributed_counts_pinned() -> None:
    rows = _attributed()
    # Exactly one row per distinct conversion_id (fan-out + resend duplicates
    # collapsed by one_row_per_conversion).
    assert len(rows) == 55
    assert len({r.conversion_id for r in rows}) == 55
    # 47 hot-attributed; 8 unattributed = 3 state-misses + the 5 shared-IP
    # conversions deferred to reconciliation (ambiguous_ip, Phase 16).
    assert sum(r.attributed for r in rows) == 47
    assert sum(not r.attributed for r in rows) == 8
    ambiguous = [r for r in rows if r.ambiguous]
    assert len(ambiguous) == 5
    assert all(not r.attributed and r.candidate_count > 1 for r in ambiguous)


def test_every_attributed_exposure_is_real_and_not_self_assisted() -> None:
    exposure_ids = {e.exposure_id for e in _read("exposures.jsonl", Exposure)}
    for r in _attributed():
        assert r.assists == sorted(set(r.assists))  # distinct
        assert r.exposure_id not in r.assists  # never credits itself
        if r.attributed:
            assert r.exposure_id in exposure_ids
            assert r.processed_at == r.ingest_time  # event-derived RMT version
        else:
            assert r.exposure_id is None and r.assists == []
