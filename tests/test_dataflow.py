"""The engine driver (`run_attribution`: household grouping + the evicting
watermark-gated pass), run offline, produces the same attributed rows as the
non-evicting pure oracle on tiny — proving the live path and the golden replay
cannot diverge (they share the leaf functions; this pins the wiring around
them). No ClickHouse."""

from pathlib import Path

from pydantic import BaseModel

from producer.models import Exposure, ResolvedConversion
from producer.serialize import canonical_bytes
from streaming.attribute import attribute
from streaming.dataflow import run_attribution

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"


def _read[M: BaseModel](name: str, model: type[M]) -> list[M]:
    return [
        model.model_validate_json(line)
        for line in (FIXTURES / name).read_text().splitlines()
    ]


def test_engine_driver_matches_pure_core() -> None:
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)

    got = run_attribution(exposures, resolved)
    expected = attribute(exposures, resolved)
    assert [canonical_bytes(r) for r in got] == [canonical_bytes(r) for r in expected]
    assert len(got) == 55  # one row per distinct conversion_id
