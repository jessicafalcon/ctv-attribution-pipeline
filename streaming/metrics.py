"""Prometheus metrics for the attribution engine (`engine_` prefix per
CLAUDE.md). Counters only — rates are computed at query time in Prometheus,
which keeps the collector deterministic and side-effect-free."""

from prometheus_client import Counter, Gauge

from producer.models import AttributedConversion
from streaming.attribute import StreamState

PROCESSED = Counter(
    "engine_conversions_processed_total",
    "Conversions processed into a final attributed record (cumulative across "
    "runs; a re-run re-counts, ReplacingMergeTree dedups the rows on read).",
)
ATTRIBUTED = Counter(
    "engine_conversions_attributed_total",
    "Conversions credited to a last-touch exposure.",
)
UNATTRIBUTED = Counter(
    "engine_conversions_unattributed_total",
    "Conversions with no in-window exposure (reconciliation retries later).",
)
ASSISTS = Counter(
    "engine_assists_recorded_total",
    "Assist exposures recorded across all attributed conversions.",
)
AMBIGUOUS_REDUCED = Counter(
    "engine_ambiguous_reductions_total",
    "Conversions collapsed from more than one candidate row (shared-IP fan-out "
    "or resend duplicate) by the conversion_id-keyed reduction.",
)
EXPOSURES_LANDED = Counter(
    "engine_exposures_landed_total",
    "Exposure rows offered to exposures_landed (cumulative across runs; "
    "post-dedup, so re-sends are already dropped before landing).",
)
DEDUP_SUPPRESSED = Counter(
    "engine_dedup_suppressed_total",
    "Exact re-send rows (same id, byte-identical) dropped by the seen-set before "
    "the join and before landing. ReplacingMergeTree would collapse them on read "
    "regardless, so this counts the mechanism firing, not a correctness delta.",
)
EXPOSURES_EVICTED = Counter(
    "engine_exposures_evicted_total",
    "Exposures aged out of the hot window (watermark past event_time + 7d). > 0 "
    "means eviction ran — the join state came back down (feature 3).",
)
JOIN_STATE_SIZE = Gauge(
    "engine_join_state_size",
    "Peak exposures held in one household's hot window during the run (the "
    "central scaling constraint). High-water mark across households; eviction is "
    "what keeps it bounded below the full exposure count.",
)


def observe(row: AttributedConversion, candidate_count: int) -> None:
    """One final attributed record and how many candidate rows it collapsed."""
    PROCESSED.inc()
    if row.attributed:
        ATTRIBUTED.inc()
        ASSISTS.inc(len(row.assists))
    else:
        UNATTRIBUTED.inc()
    if candidate_count > 1:
        AMBIGUOUS_REDUCED.inc()


_join_state_peak = 0  # high-water across households; the gauge only climbs


def observe_state(state: StreamState) -> None:
    """One household's join-state summary: add its evictions to the counter and
    raise the peak high-water gauge. The peak is tracked in a module variable
    (not read back off the Prometheus gauge, whose getter is a private API)."""
    global _join_state_peak
    EXPOSURES_EVICTED.inc(state.evicted)
    _join_state_peak = max(_join_state_peak, state.peak)
    JOIN_STATE_SIZE.set(_join_state_peak)


def reset_join_state_peak() -> None:
    """Reset the high-water tracker and its gauge (used by tests between runs)."""
    global _join_state_peak
    _join_state_peak = 0
    JOIN_STATE_SIZE.set(0)
