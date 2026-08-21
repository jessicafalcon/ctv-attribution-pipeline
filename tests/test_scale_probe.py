"""Phase 14 — measured scaling curve. The reported number must be a STRUCTURAL
measure (deterministic on re-run), occupancy must scale with the event count, and
the per-exposure cost must be flat across tiers. `tracemalloc` peak is a labeled
cross-check and is deliberately never asserted (it is allocation-nondeterministic).

Small tiers here keep the suite fast; the full 1k/10k/100k curve is `make
scale-curve` (streaming.scale_probe.TIERS), guarded below only for its shape.
"""

from producer.config import load_profile
from streaming import metrics
from streaming.scale_probe import (
    TIERS,
    CurvePoint,
    deep_sizeof,
    measure_tier,
    render_block,
    run_curve,
)

# Fast stand-in tiers for the determinism/monotonicity properties (the properties
# are of the code, provable at any scale; the headline curve runs in make scale-curve).
SMALL_TIERS = (500, 1_000, 2_000)


def _structural(p: CurvePoint) -> tuple[int, int, int, float, int, int]:
    """Every field that reaches the byte-stable committed SCALING.md must be pinned
    here — that is the rule the tracemalloc blocker taught: an unpinned rendered
    column is how nondeterminism slips into the committed doc. So this covers all
    four table columns (exposures_in_window, structural_bytes, bytes_per_exposure,
    join_state_current) plus join_state_peak. Only tracemalloc_peak_bytes is
    excluded, and only because it is console-only, never written to the doc."""
    return (
        p.n_exposures,
        p.exposures_in_window,
        p.structural_bytes,
        p.bytes_per_exposure,
        p.join_state_current,
        p.join_state_peak,
    )


def test_deep_sizeof_is_deterministic_and_counts_shared_objects_once() -> None:
    child = [1, 2, 3, "abcdef"]
    assert deep_sizeof(child) == deep_sizeof(child)  # re-run identical
    shared = [child, child]  # same object twice
    distinct = [child, [1, 2, 3, "abcdef"]]  # equal-but-separate copy
    # The shared child is counted once; the copy adds its own bytes → strictly more.
    assert deep_sizeof(shared) < deep_sizeof(distinct)


def test_curve_is_deterministic_on_rerun() -> None:
    first = run_curve(SMALL_TIERS)
    second = run_curve(SMALL_TIERS)
    assert [_structural(p) for p in first] == [_structural(p) for p in second]


def test_occupancy_is_monotonic_in_tier() -> None:
    points = run_curve(SMALL_TIERS)
    occ = [p.exposures_in_window for p in points]
    assert occ == sorted(occ) and len(set(occ)) == len(occ)  # strictly increasing
    # No eviction at these spans → occupancy equals the deduped event count.
    assert [p.exposures_in_window for p in points] == list(SMALL_TIERS)


def test_bytes_per_exposure_is_flat_across_tiers() -> None:
    # The per-entry cost is a structural constant, not a function of count. It moves
    # only slightly (category strings amortize as the pool fills), so pin the shape:
    # every tier within a few percent of the mean.
    per = [p.bytes_per_exposure for p in run_curve(SMALL_TIERS)]
    mean = sum(per) / len(per)
    assert all(abs(x - mean) / mean < 0.05 for x in per)
    assert 200 < mean < 5_000  # sane order of magnitude for a pydantic Exposure


def test_measure_tier_reports_the_phase7_gauge_and_no_eviction() -> None:
    # measure_tier drains the REAL engine (run_attribution) and reads the
    # Phase-7 engine_join_state_current gauge; it raises if eviction fired (which
    # would break the retained-state == input assumption). Occupancy < window span,
    # so eviction must not fire.
    metrics.reset_join_state_peak()
    point = measure_tier(load_profile("scale_curve"), 1_000)
    assert point.exposures_in_window == 1_000
    assert point.join_state_current > 0  # gauge was populated by the drain
    assert point.join_state_peak >= point.join_state_current


def test_scaling_block_is_rederived_from_the_measured_constant() -> None:
    # The extrapolation must be built from the measured bytes_per_exposure, not the
    # retired ~200 B / ~3 TB guess.
    points = run_curve(SMALL_TIERS)
    block = render_block(points)
    assert "~200 B guess" in block  # names what it replaces
    assert "3 TB" not in block  # the old guessed total is gone
    assert "TB at the measured" in block


def test_curve_tiers_are_the_documented_shape() -> None:
    assert TIERS == (1_000, 10_000, 100_000)
