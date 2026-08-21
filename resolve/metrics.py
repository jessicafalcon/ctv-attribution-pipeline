"""Prometheus metrics for the resolve step (`resolve_` prefix per CLAUDE.md;
emitted from the engine process since Phase 16).

Counters only — rates are ratios computed at query time in Prometheus, which
keeps this collector deterministic and side-effect-free (the determinism
policy: anything computable is computed, never sampled). Derived signals:
  resolve rate   = resolved / consumed
  ambiguity rate = ambiguous / resolved
  fan-out factor = records_emitted / resolved
"""

from prometheus_client import Counter, Gauge

from producer.models import ResolvedConversion

CONSUMED = Counter("resolve_conversions_consumed_total", "Conversion rows consumed.")
INPUT_BACKLOG = Gauge(
    "resolve_input_backlog",
    "Messages the resolve consumer must clear from `conversions` at drain start "
    "(offset 0 → end-of-log). BATCH PROXY for consumer lag: the stage reads from "
    "OFFSET_BEGINNING every pass with no committed group offsets (BACKLOG 19), so "
    "this is the topic backlog ≈ f(volume), not live consumer-group lag. Set once "
    "per run (the backlog the consumer started behind by).",
)
RESOLVED = Counter(
    "resolve_conversions_resolved_total",
    "Conversions that resolved to at least one household.",
)
UNRESOLVED = Counter(
    "resolve_conversions_unresolved_total",
    "Conversions with an unknown device and an IP owned by no household.",
)
AMBIGUOUS = Counter(
    "resolve_conversions_ambiguous_total",
    "Conversions that fanned out across a shared IP (candidate_count > 1).",
)
EMITTED = Counter(
    "resolve_records_emitted_total",
    "Resolved records produced (fan-out means > 1 per conversion).",
    ["resolution"],
)


def observe_backlog(n: int) -> None:
    """Record the input backlog the consumer faced at drain start (once per run)."""
    INPUT_BACKLOG.set(n)


def observe(resolved: list[ResolvedConversion]) -> None:
    """Update counters for one consumed conversion and its resolved records."""
    CONSUMED.inc()
    if not resolved:
        UNRESOLVED.inc()
        return
    RESOLVED.inc()
    if resolved[0].ambiguous:
        AMBIGUOUS.inc()
    for r in resolved:
        EMITTED.labels(resolution=r.resolution).inc()
