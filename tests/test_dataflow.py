"""The engine driver (`run_attribution`: household grouping + the evicting
watermark-gated pass), run offline, produces the same attributed rows as the
non-evicting pure oracle on tiny — proving the live path and the golden replay
cannot diverge (they share the leaf functions; this pins the wiring around
them). No ClickHouse."""

from pathlib import Path

from pydantic import BaseModel

from producer.models import Exposure, ResolvedConversion
from producer.serialize import canonical_bytes
from streaming import metrics
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


def test_ambiguous_deferred_counter_increments_by_tiny_shared_ip_set() -> None:
    # engine_conversions_ambiguous_deferred_total is the only hot-path signal that
    # ambiguity is being deferred (the Phase-18 dirty-set precursor): tiny's 5
    # shared-IP conversions, exactly — and they are a subset of unattributed.
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    metrics.AMBIGUOUS_DEFERRED._value.set(0)
    metrics.UNATTRIBUTED._value.set(0)
    run_attribution(exposures, resolved)
    assert metrics.AMBIGUOUS_DEFERRED._value.get() == 5
    assert metrics.UNATTRIBUTED._value.get() == 8  # 5 deferred + 3 state-misses
    metrics.AMBIGUOUS_DEFERRED._value.set(0)
    metrics.UNATTRIBUTED._value.set(0)
