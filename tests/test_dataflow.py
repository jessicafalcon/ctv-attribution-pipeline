"""The Bytewax operator graph, run offline with capturing sinks, produces the
same attributed rows as the pure core — proving the live path and the golden
replay cannot diverge (they share the leaf functions; this pins the wiring
around them). No ClickHouse: sinks capture to lists."""

from pathlib import Path

from bytewax.testing import TestingSink, run_main
from pydantic import BaseModel

from producer.models import Exposure, ResolvedConversion
from producer.serialize import canonical_bytes
from streaming.attribute import attribute
from streaming.dataflow import build_flow

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"


def _read[M: BaseModel](name: str, model: type[M]) -> list[M]:
    return [
        model.model_validate_json(line)
        for line in (FIXTURES / name).read_text().splitlines()
    ]


def test_dataflow_matches_pure_core() -> None:
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)

    attributed: list = []
    landed: list = []
    run_main(
        build_flow(exposures, resolved, TestingSink(attributed), TestingSink(landed))
    )

    # Same 55 winners as the pure core, compared canonically (Bytewax emits per
    # key in an unspecified order; sort both by conversion_id).
    got = sorted(attributed, key=lambda r: r.conversion_id)
    expected = attribute(exposures, resolved)
    assert [canonical_bytes(r) for r in got] == [canonical_bytes(r) for r in expected]

    # Every drained exposure is offered to the landing sink (RMT dedups on read).
    assert len(landed) == len(exposures)
