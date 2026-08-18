"""Feature 1 — dedup as a full seen-set (batch drain). Exact re-sends are
dropped and counted; shared-IP fan-out candidates and non-repeating
unknown-device conversions are NOT mistaken for duplicates. Pure, no services.

Not a TTL: the seeded duplicate is timestamp-identical to its original, so a
re-send at any offset distance must still be suppressed (DECISIONS Phase 5)."""

from collections import Counter as Multiset
from pathlib import Path

from pydantic import BaseModel

from producer.models import Exposure, ResolvedConversion
from streaming import metrics
from streaming.attribute import dedup_by_id, dedup_streams

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"


def _read[M: BaseModel](name: str, model: type[M]) -> list[M]:
    return [
        model.model_validate_json(line)
        for line in (FIXTURES / name).read_text().splitlines()
    ]


def test_dedup_by_id_drops_resends_keeps_first_in_order() -> None:
    kept, suppressed = dedup_by_id(["a", "b", "a", "c", "b"], key=lambda s: s)
    assert kept == ["a", "b", "c"]  # first occurrence, order preserved
    assert suppressed == 2


def test_full_seen_set_suppresses_at_any_offset_distance() -> None:
    # A re-send far from its original — a TTL could evict the id first and miss
    # it; the full seen-set never forgets within a drain, so it is suppressed.
    rows = ["x", *[f"f-{i}" for i in range(1000)], "x"]
    kept, suppressed = dedup_by_id(rows, key=lambda s: s)
    assert suppressed == 1
    assert kept[0] == "x" and "x" not in kept[1:]


def test_dedup_streams_over_tiny_fixture() -> None:
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    exp, res, suppressed = dedup_streams(exposures, resolved)

    # Exposures collapse to distinct exposure_ids; tiny carries re-sends.
    assert len(exp) == len({e.exposure_id for e in exposures})
    assert len(exp) < len(exposures)

    # Every distinct conversion survives and the 5 shared-IP fan-outs are kept
    # (dedup keys on (conversion_id, household_id), never conversion_id alone).
    assert {r.conversion_id for r in res} == {r.conversion_id for r in resolved}
    assert len({r.conversion_id for r in res}) == 55
    per_conv = Multiset(r.conversion_id for r in res)
    assert sum(1 for n in per_conv.values() if n > 1) == 5

    assert suppressed == (len(exposures) - len(exp)) + (len(resolved) - len(res))
    assert suppressed > 0


def test_fanout_candidates_are_not_duplicates() -> None:
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    ambiguous_cid = next(r.conversion_id for r in resolved if r.candidate_count > 1)
    candidates = [r for r in resolved if r.conversion_id == ambiguous_cid]
    # Distinct households under one conversion_id — legitimate fan-out.
    assert len({r.household_id for r in candidates}) == len(candidates) > 1
    kept, suppressed = dedup_by_id(
        candidates, key=lambda r: (r.conversion_id, r.household_id)
    )
    assert suppressed == 0 and len(kept) == len(candidates)


def test_resend_of_a_fanout_candidate_is_dropped_width_preserved() -> None:
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    ambiguous_cid = next(r.conversion_id for r in resolved if r.candidate_count > 1)
    candidates = [r for r in resolved if r.conversion_id == ambiguous_cid]
    # Re-send every candidate: each dropped, fan-out width unchanged.
    kept, suppressed = dedup_by_id(
        [*candidates, *candidates], key=lambda r: (r.conversion_id, r.household_id)
    )
    assert suppressed == len(candidates)
    assert len(kept) == len(candidates)


def test_unknown_device_conversions_are_not_duplicates() -> None:
    # Unknown u- devices never repeat (BACKLOG Phase 1): distinct conversions
    # that fell back to IP resolution must not be mistaken for a re-send.
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    base = resolved[0]
    a = base.model_copy(update={"conversion_id": "c-900001", "device_id": "u-000001"})
    b = base.model_copy(update={"conversion_id": "c-900002", "device_id": "u-000002"})
    kept, suppressed = dedup_by_id(
        [a, b], key=lambda r: (r.conversion_id, r.household_id)
    )
    assert suppressed == 0 and len(kept) == 2


def test_dedup_counter_increments_by_suppressed() -> None:
    exposures = _read("exposures.jsonl", Exposure)
    resolved = _read("expected/conversions_resolved.jsonl", ResolvedConversion)
    metrics.DEDUP_SUPPRESSED._value.set(0)
    _, _, suppressed = dedup_streams(exposures, resolved)
    metrics.DEDUP_SUPPRESSED.inc(suppressed)  # the increment run_engine performs
    assert metrics.DEDUP_SUPPRESSED._value.get() == suppressed > 0
    metrics.DEDUP_SUPPRESSED._value.set(0)
