"""Pure attribution core — no I/O, no clock, no entropy: a function of
(exposures, resolved conversions, window) only, so the same input is
byte-identical every run.

Every attribution DECISION lives in the two leaf functions here; both the
offline replay (`attribute` below) and the live Bytewax engine
(`streaming/dataflow.py`) call the SAME leaves, so they cannot diverge
(DECISIONS Phase 3, "Bytewax owns plumbing, the pure core owns decisions").

Two stages, in order:

1. `attribute_household` — household-local last-touch. For each resolved
   conversion, credit the most-recent in-window exposure in its household and
   record the rest as assists; emit an unattributed row if there is none.
2. `reduce_conversion` — collapse every candidate row sharing a `conversion_id`
   (shared-IP fan-out across households, plus byte-identical resend duplicates)
   to exactly one winner: the most-recent last-touch exposure.
"""

from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from producer.models import AttributedConversion, Exposure, ResolvedConversion

# Default hot window: exposures up to 7 days before a conversion are eligible
# (ARCHITECTURE §3.3). tiny stays inside it, so every conversion is hot-path
# attributable without reconciliation (DECISIONS Phase 1).
HOT_WINDOW = timedelta(days=7)

# Allowed lateness = the watermark's grace. A conversion is released for
# attribution once the watermark (max event_time seen − allowed_lateness)
# reaches its event_time, which guarantees every in-tolerance late exposure
# (arrival lateness ≤ allowed_lateness) has already arrived (feature 2). It must
# be ≥ a profile's late.max_minutes (tiny = 180; medium sized under this). Env
# override in the live engine: ENGINE_ALLOWED_LATENESS_MINUTES.
ALLOWED_LATENESS = timedelta(hours=6)


def dedup_by_id[M](
    rows: Iterable[M], key: Callable[[M], Hashable]
) -> tuple[list[M], int]:
    """Drop exact re-sends: keep the first row per `key` in arrival order and
    report how many were suppressed. Pure (order-preserving, no I/O).

    `key` must be a row's full identity, NOT just its event id. A resolved
    conversion fans out to one row per candidate household under the SAME
    `conversion_id` (shared-IP resolve fan-out), so keying resolved rows on
    `conversion_id` alone would drop legitimate candidates, not just duplicates
    — see `dedup_streams`. Batch mode keeps a full seen-set (no TTL): the seeded
    duplicate is timestamp-identical to its original, so there is nothing an
    event-time TTL could measure against (DECISIONS/SCALING Phase 5)."""
    seen: set[Hashable] = set()
    kept: list[M] = []
    suppressed = 0
    for row in rows:
        k = key(row)
        if k in seen:
            suppressed += 1
            continue
        seen.add(k)
        kept.append(row)
    return kept, suppressed


def dedup_streams(
    exposures: Iterable[Exposure], resolved: Iterable[ResolvedConversion]
) -> tuple[list[Exposure], list[ResolvedConversion], int]:
    """Drop exact re-sends from both engine input streams before the join.
    Exposures key on `exposure_id` (globally unique, no fan-out); resolved
    conversions key on `(conversion_id, household_id)` so shared-IP fan-out
    candidates survive (same `conversion_id`, different household). Returns the
    deduped streams and the total suppressed count for the
    `engine_dedup_suppressed` counter. Pure, so the engine and any offline
    oracle dedup identically."""
    exp, exp_n = dedup_by_id(exposures, lambda e: e.exposure_id)
    res, res_n = dedup_by_id(resolved, lambda r: (r.conversion_id, r.household_id))
    return exp, res, exp_n + res_n


@dataclass(frozen=True)
class Candidate:
    """One per-candidate attribution result. `last_touch_time` is the credited
    exposure's `event_time` (None if unattributed), carried so the
    `conversion_id`-keyed reduction can compare recency across households
    without re-reading exposures. It is not part of the persisted schema."""

    row: AttributedConversion
    last_touch_time: datetime | None


def attribute_household(
    exposures: list[Exposure],
    resolved: list[ResolvedConversion],
    window: timedelta,
) -> list[Candidate]:
    """Stage 1 (leaf). `exposures` and `resolved` are the rows of ONE household.
    Credit each conversion's last-touch: the eligible exposure with the latest
    `event_time` (ties broken by `exposure_id`); the others become assists. An
    exposure is eligible when `conv.event_time - window <= exp.event_time <=
    conv.event_time` (in-window and not after the conversion). No eligible
    exposure → an unattributed row so reconciliation (Phase 6) can retry."""
    out: list[Candidate] = []
    for conv in resolved:
        lo = conv.event_time - window
        eligible = [e for e in exposures if lo <= e.event_time <= conv.event_time]
        if eligible:
            last_touch = max(eligible, key=lambda e: (e.event_time, e.exposure_id))
            # Distinct assist ids, and never the credited exposure itself — set
            # difference by id, so a *resent* last-touch exposure (same id twice
            # in `eligible`) cannot survive into its own assists. Pure-function
            # set semantics, distinct from Phase-5 TTL'd stream dedup on the join.
            assists = sorted(
                {e.exposure_id for e in eligible} - {last_touch.exposure_id}
            )
            out.append(
                Candidate(
                    _attributed(conv, last_touch.exposure_id, assists, attributed=True),
                    last_touch.event_time,
                )
            )
        else:
            out.append(Candidate(_attributed(conv, None, [], attributed=False), None))
    return out


def _arrival_key(
    tagged: tuple[str, Exposure | ResolvedConversion],
) -> tuple[datetime, str, str]:
    """Deterministic arrival order for one household's interleaved events:
    ingest_time, then a stable (kind, id) tiebreak. The global stream is emitted
    in ingest order, so ingest_time reconstructs arrival order; the tiebreak only
    fixes ordering when two events share an ingest_time."""
    kind, model = tagged
    ident = model.exposure_id if isinstance(model, Exposure) else model.conversion_id
    return (model.ingest_time, kind, ident)


def event_time_watermark(
    watermark: datetime | None, event_time: datetime, allowed_lateness: timedelta
) -> datetime:
    """Advance the watermark to `max(seen event_time) − allowed_lateness`. Pure,
    event-time only (never wall clock), monotonic non-decreasing — an out-of-order
    (older) event cannot pull it back."""
    candidate = event_time - allowed_lateness
    return candidate if watermark is None or candidate > watermark else watermark


def attribute_household_streaming(
    events: list[tuple[str, Exposure | ResolvedConversion]],
    window: timedelta,
    allowed_lateness: timedelta,
) -> list[Candidate]:
    """Stage 1, streaming form (feature 2). Process ONE household's exposures and
    resolved conversions in arrival (ingest) order through a watermark-gated
    release. Exposures accumulate in state; a conversion is buffered until the
    watermark reaches its `event_time`, which guarantees every eligible exposure
    has arrived (arrival lateness ≤ `allowed_lateness`), then it is attributed
    against state via `attribute_household`. No eviction here — that is feature 3;
    without it a conversion always probes the complete eligible set, so this
    returns exactly what the non-evicting `attribute_household` returns over the
    full household (gate 0 proves it byte-for-byte on tiny).

    At end-of-input every still-pending conversion is released against the full
    state (the completeness backstop): a conversion whose release watermark is
    never reached mid-stream is still attributed against complete state, so
    release-gating provably equals `fold_final` regardless of arrival pathology."""
    ordered = sorted(events, key=_arrival_key)
    exposures: list[Exposure] = []
    pending: list[ResolvedConversion] = []
    out: list[Candidate] = []
    watermark: datetime | None = None
    for kind, model in ordered:
        watermark = event_time_watermark(watermark, model.event_time, allowed_lateness)
        if kind == "exp":
            exposures.append(model)  # type: ignore[arg-type]
        else:
            pending.append(model)  # type: ignore[arg-type]
        if pending and watermark is not None:
            released = [c for c in pending if c.event_time <= watermark]
            if released:
                pending = [c for c in pending if c.event_time > watermark]
                for conv in released:
                    out.extend(attribute_household(exposures, [conv], window))
    for conv in pending:  # EOF flush: complete state, watermark → +∞
        out.extend(attribute_household(exposures, [conv], window))
    return out


def reduce_conversion(candidates: list[Candidate]) -> AttributedConversion:
    """Stage 2 (leaf). Collapse all candidate rows sharing a `conversion_id` to
    one winner. An attributed candidate always beats an unattributed one; among
    attributed, keep the most-recent last-touch exposure — `(last_touch_time,
    exposure_id)` is already a total order (exposure_id is globally unique, so
    two candidates crediting different exposures never tie), and `household_id`
    is a vestigial final tiebreak. If every candidate is unattributed, keep the
    lowest `household_id`. Byte-identical resend duplicates collapse harmlessly
    (same row either way)."""
    attributed = [c for c in candidates if c.row.attributed]
    if attributed:
        return max(
            attributed,
            key=lambda c: (c.last_touch_time, c.row.exposure_id, c.row.household_id),
        ).row
    return min(candidates, key=lambda c: c.row.household_id).row


def attribute(
    exposures: Iterable[Exposure],
    resolved: Iterable[ResolvedConversion],
    window: timedelta = HOT_WINDOW,
) -> list[AttributedConversion]:
    """Orchestrate the two leaves over in-memory groups (the offline-replay
    path). Groups exposures and resolved conversions by `household_id`, runs
    stage 1 per household, regroups candidates by `conversion_id`, runs stage 2,
    and returns exactly one attributed record per distinct `conversion_id`,
    sorted by `conversion_id` for byte-identical output."""
    exp_by_hh: dict[str, list[Exposure]] = defaultdict(list)
    for e in exposures:
        exp_by_hh[e.household_id].append(e)
    res_by_hh: dict[str, list[ResolvedConversion]] = defaultdict(list)
    for r in resolved:
        res_by_hh[r.household_id].append(r)

    by_conv: dict[str, list[Candidate]] = defaultdict(list)
    for hid, res_rows in res_by_hh.items():
        for cand in attribute_household(exp_by_hh.get(hid, []), res_rows, window):
            by_conv[cand.row.conversion_id].append(cand)

    winners = [reduce_conversion(cands) for cands in by_conv.values()]
    return sorted(winners, key=lambda r: r.conversion_id)


def _attributed(
    conv: ResolvedConversion,
    exposure_id: str | None,
    assists: list[str],
    *,
    attributed: bool,
) -> AttributedConversion:
    return AttributedConversion(
        **conv.model_dump(),
        exposure_id=exposure_id,
        assists=assists,
        attributed=attributed,
        path="hot",
        processed_at=conv.ingest_time,  # event-derived RMT version (DECISIONS Phase 3)
    )
