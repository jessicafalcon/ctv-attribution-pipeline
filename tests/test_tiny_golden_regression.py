"""Gate 0 (Phase 5) — the frozen 55-row tiny golden is the regression oracle for
the engine rewrite. Phase 5 replaces the order-independent core with an
arrival-ordered, watermarked, evicting operator; this test proves the LIVE
operator graph (build_flow) still reproduces the golden byte for byte.

If this fails, STOP — a changed tiny row means the eviction bound is wrong or a
conversion was dropped that should not have been. Never edit the frozen fixture
to make it pass. Tiny's event-time span (~3.5d) is < the 7d hot window, so zero
exposures evict and output must be identical (spec, ARCHITECTURE §8)."""

from pathlib import Path

from bytewax.testing import TestingSink, run_main
from pydantic import BaseModel

from producer.models import Exposure, ResolvedConversion
from producer.serialize import jsonl
from streaming.dataflow import build_flow

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"
GOLDEN = FIXTURES / "expected" / "attributed.jsonl"


def _read[M: BaseModel](name: str, model: type[M]) -> list[M]:
    return [
        model.model_validate_json(line)
        for line in (FIXTURES / name).read_text().splitlines()
    ]


def test_engine_reproduces_tiny_golden_byte_for_byte() -> None:
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)

    attributed: list = []
    landed: list = []
    run_main(
        build_flow(exposures, resolved, TestingSink(attributed), TestingSink(landed))
    )

    # Bytewax emits per key in an unspecified order; the golden is sorted by
    # conversion_id, so sort before the canonical comparison.
    got = sorted(attributed, key=lambda r: r.conversion_id)
    assert jsonl(got) == GOLDEN.read_text()
    assert len(got) == 55
