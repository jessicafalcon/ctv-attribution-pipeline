"""Prometheus metrics for the attribution engine (`engine_` prefix per
CLAUDE.md). Counters only — rates are computed at query time in Prometheus,
which keeps the collector deterministic and side-effect-free."""

from prometheus_client import Counter

from producer.models import AttributedConversion

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
