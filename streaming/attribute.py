"""Pure attribution core — no I/O, no clock, no entropy: a function of
(exposures, resolved conversions, window) only, so the same input is
byte-identical every run.

Every attribution DECISION lives in the leaf functions here; the offline
oracle (`attribute` below), the engine driver (`streaming/dataflow.py`) and the
reconciliation matcher (`reconcile/reconcile.py`) call the SAME leaves, so they
cannot diverge (DECISIONS Phase 3, refined Phase 16).

Hot-path rule (Phase 16): the hot path attributes only when the household is
CERTAIN — a device-graph hit or a single-owner IP. A shared-IP conversion
(`candidate_count > 1`) is emitted unattributed (reason: ambiguous_ip) and left
for reconciliation, which holds every exposure and applies the most-recent-
exposure tiebreak across the candidate households. The old `conversion_id`-keyed
reduce that guessed hot is gone; exactly one row per `conversion_id` still
reaches the sink (`one_row_per_conversion`).
"""

from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
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

    `key` must be a row's full identity. A resolved conversion fans out to one
    row per candidate household under the SAME `conversion_id` (shared-IP
    fan-out), so `dedup_streams` keys resolved rows on `(conversion_id,
    household_id)` and counts only exact re-sends; the fan-out itself collapses
    later in `one_row_per_conversion`, which is not dedup and is not counted
    here. Batch mode keeps a full seen-set (no TTL): the seeded duplicate is
    timestamp-identical to its original, so there is nothing an event-time TTL
    could measure against (DECISIONS/SCALING Phase 5)."""
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
    Exposures key on `exposure_id` (globally unique); resolved conversions key
    on `(conversion_id, household_id)`, so the suppressed count is re-sends only
    (a shared-IP fan-out is collapsed separately, see `one_row_per_conversion`).
    Returns the deduped streams and the total suppressed count for the
    `engine_dedup_suppressed` counter. Pure, so the engine and any offline
    oracle dedup identically."""
    exp, exp_n = dedup_by_id(exposures, lambda e: e.exposure_id)
    res, res_n = dedup_by_id(resolved, lambda r: (r.conversion_id, r.household_id))
    return exp, res, exp_n + res_n


@dataclass(frozen=True)
class Candidate:
    """One attribution result. `last_touch_time` is the credited exposure's
    `event_time` (None if unattributed), carried so reconciliation's cross-
    household tiebreak (`reconcile.pick_household`) can compare recency without
    re-reading exposures. It is not part of the persisted schema."""

    row: AttributedConversion
    last_touch_time: datetime | None


def one_row_per_conversion(
    resolved: Iterable[ResolvedConversion],
) -> list[ResolvedConversion]:
    """Exactly one resolved row per `conversion_id`, in first-arrival order. A
    shared-IP fan-out (N rows, one per candidate household, same
    `conversion_id`) collapses to its lowest-`household_id` candidate — a
    PLACEHOLDER the hot path emits unattributed (`candidate_count > 1` →
    ambiguous_ip); reconciliation explodes the row's persisted
    `candidate_households` (Phase 17), so the placeholder is never credited.
    Exact re-sends that survived upstream (dedup off) collapse too (same bytes).
    This is what keeps `conversion_id` a safe ReplacingMergeTree sort key
    (DECISIONS Phase 3 (b))
    now that the `conversion_id`-keyed reduce is gone (Phase 16)."""
    by_conv: dict[str, ResolvedConversion] = {}
    for r in resolved:
        cur = by_conv.get(r.conversion_id)
        if cur is None or r.household_id < cur.household_id:
            by_conv[r.conversion_id] = r  # reassign keeps the first-seen slot
    return list(by_conv.values())


def candidate_households_by_conversion(
    rows: Iterable[ResolvedConversion],
) -> dict[str, list[str]]:
    """`conversion_id` → sorted candidate households, for the shared-IP fan-outs
    (`candidate_count > 1`) in `rows` — one row per candidate household, as
    `resolve_one` emits them (the engine, before `one_row_per_conversion`
    collapses the fan-out) and as `reconcile.expand_candidates` re-creates them.
    Certain conversions are absent. This is what the deferred row persists as
    `candidate_households` (Phase 17)."""
    seen: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        if r.candidate_count > 1:
            seen[r.conversion_id].add(r.household_id)
    return {cid: sorted(hhs) for cid, hhs in seen.items()}


def _candidates_of(
    conv: ResolvedConversion, candidates: Mapping[str, list[str]] | None
) -> list[str]:
    """The candidate set to persist on `conv`'s row: empty for a certain
    conversion; for an ambiguous one it MUST be present in `candidates` — a
    deferred row without its candidate set could never be reconciled."""
    if conv.candidate_count == 1:
        return []
    hhs = (candidates or {}).get(conv.conversion_id)
    if not hhs:
        raise ValueError(
            f"{conv.conversion_id}: ambiguous (candidate_count="
            f"{conv.candidate_count}) but no candidate_households were supplied"
        )
    return list(hhs)


def last_touch(
    exposures: list[Exposure],
    conv: ResolvedConversion,
    window: timedelta,
    candidate_households: Sequence[str] = (),
) -> Candidate:
    """The leaf. `exposures` are the rows of ONE household. Credit the eligible
    exposure with the latest `event_time` (ties broken by `exposure_id`); the
    others become assists. An exposure is eligible when `conv.event_time -
    window <= exp.event_time <= conv.event_time` (in-window and not after the
    conversion). No eligible exposure → an unattributed row. Household-local and
    ambiguity-blind: reconciliation scores each candidate household of an
    ambiguous conversion with this same function."""
    lo = conv.event_time - window
    hhs = list(candidate_households)
    eligible = [e for e in exposures if lo <= e.event_time <= conv.event_time]
    if not eligible:
        return Candidate(
            _attributed(conv, None, [], attributed=False, candidate_households=hhs),
            None,
        )
    winner = max(eligible, key=lambda e: (e.event_time, e.exposure_id))
    # Distinct assist ids, and never the credited exposure itself — set
    # difference by id, so a *resent* last-touch exposure (same id twice in
    # `eligible`) cannot survive into its own assists. Pure-function set
    # semantics, distinct from the Phase-5 seen-set stream dedup.
    assists = sorted({e.exposure_id for e in eligible} - {winner.exposure_id})
    return Candidate(
        _attributed(
            conv, winner.exposure_id, assists, attributed=True, candidate_households=hhs
        ),
        winner.event_time,
    )


def attribute_household(
    exposures: list[Exposure],
    resolved: list[ResolvedConversion],
    window: timedelta,
    candidates: Mapping[str, list[str]] | None = None,
) -> list[Candidate]:
    """The HOT-PATH rule over one household's rows. A conversion whose household
    is certain (`candidate_count == 1`: device hit or single-owner IP) gets
    `last_touch`. A shared-IP conversion (`candidate_count > 1`, reason
    ambiguous_ip) is emitted unattributed WITHOUT probing state — the hot path
    never guesses a household; reconciliation (Phase 6/16) owns it, alongside
    the state-miss rows (no in-window exposure). `candidates` supplies the
    candidate set each ambiguous row persists (`candidate_households_by_conversion`
    over the fan-out); required for every ambiguous row, unused otherwise."""
    return [
        Candidate(
            _attributed(
                conv,
                None,
                [],
                attributed=False,
                candidate_households=_candidates_of(conv, candidates),
            ),
            None,
        )
        if conv.candidate_count > 1
        else last_touch(exposures, conv, window)
        for conv in resolved
    ]


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


@dataclass(frozen=True)
class StreamState:
    """Join-state observability for one household's streaming pass. `peak` is the
    high-water exposure count held at once (the scaling constraint); `evicted` is
    how many exposures aged out; `final` is what remained at end-of-input. Not
    part of the persisted schema — it drives the engine_ join-state metrics."""

    peak: int
    evicted: int
    final: int


@dataclass(frozen=True)
class StreamResult:
    candidates: list[Candidate]
    state: StreamState


def attribute_household_streaming(
    events: list[tuple[str, Exposure | ResolvedConversion]],
    window: timedelta,
    allowed_lateness: timedelta,
    candidates: Mapping[str, list[str]] | None = None,
) -> StreamResult:
    """Stage 1, streaming form (features 2–3). Process ONE household's exposures
    and resolved conversions in arrival (ingest) order through a watermark-gated
    release and hot-window eviction.

    Per iteration, in this order:
    1. advance the watermark to `max(event_time) − allowed_lateness`;
    2. buffer the event (exposure → state, conversion → pending);
    3. **release** every pending conversion with `event_time ≤ watermark` (`≤`),
       guaranteeing its eligible exposures have arrived, and attribute it against
       state via `attribute_household`;
    4. **evict** exposures with `watermark > event_time + window` (strict `>`).

    Release before eviction, and `≥` release vs strict `>` eviction, are
    load-bearing: an eligible exposure of a just-released conversion satisfies
    `exp.event_time + window ≥ conv.event_time`, so at the boundary
    (`watermark = conv.event_time = exp.event_time + window`) release fires this
    tick while eviction waits until the next — the exposure is never dropped
    before its conversion probes (DECISIONS Phase 5). Eviction therefore removes
    only exposures no in-tolerance conversion can still match, so the output is
    byte-identical to the non-evicting `attribute_household` over the full
    household (gate 0 on tiny — which never evicts, span < window — and medium
    parity prove this).

    At end-of-input every still-pending conversion is released against the
    surviving state (the completeness backstop); a conversion whose release
    watermark is never reached mid-stream is still attributed against complete
    state (nothing matchable has been evicted, by the boundary rule above)."""
    ordered = sorted(events, key=_arrival_key)
    exposures: list[Exposure] = []
    pending: list[ResolvedConversion] = []
    out: list[Candidate] = []
    watermark: datetime | None = None
    peak = evicted = 0
    for kind, model in ordered:
        watermark = event_time_watermark(watermark, model.event_time, allowed_lateness)
        if kind == "exp":
            exposures.append(model)  # type: ignore[arg-type]
        else:
            pending.append(model)  # type: ignore[arg-type]
        if pending:  # release first (≥), so a boundary exposure is still present
            released = [c for c in pending if c.event_time <= watermark]
            if released:
                pending = [c for c in pending if c.event_time > watermark]
                for conv in released:
                    out.extend(
                        attribute_household(exposures, [conv], window, candidates)
                    )
        keep = [e for e in exposures if watermark <= e.event_time + window]  # evict >
        evicted += len(exposures) - len(keep)
        exposures = keep
        peak = max(peak, len(exposures))
    for conv in pending:  # EOF flush: surviving state, watermark → +∞
        out.extend(attribute_household(exposures, [conv], window, candidates))
    return StreamResult(
        out, StreamState(peak=peak, evicted=evicted, final=len(exposures))
    )


def attribute(
    exposures: Iterable[Exposure],
    resolved: Iterable[ResolvedConversion],
    window: timedelta = HOT_WINDOW,
) -> list[AttributedConversion]:
    """The non-evicting hot-path oracle (offline replay; Phase-5 parity
    baseline for the evicting engine). One row per `conversion_id`
    (`one_row_per_conversion`), grouped by `household_id`, the hot rule per
    household, sorted by `conversion_id` for byte-identical output."""
    exp_by_hh: dict[str, list[Exposure]] = defaultdict(list)
    for e in exposures:
        exp_by_hh[e.household_id].append(e)
    resolved = list(resolved)
    candidates = candidate_households_by_conversion(resolved)
    res_by_hh: dict[str, list[ResolvedConversion]] = defaultdict(list)
    for r in one_row_per_conversion(resolved):
        res_by_hh[r.household_id].append(r)

    rows = [
        cand.row
        for hid, res_rows in res_by_hh.items()
        for cand in attribute_household(
            exp_by_hh.get(hid, []), res_rows, window, candidates
        )
    ]
    return sorted(rows, key=lambda r: r.conversion_id)


def _attributed(
    conv: ResolvedConversion,
    exposure_id: str | None,
    assists: list[str],
    *,
    attributed: bool,
    candidate_households: list[str],
) -> AttributedConversion:
    if attributed:
        reason = None
    else:
        reason = "ambiguous_ip" if conv.candidate_count > 1 else "state_miss"
    return AttributedConversion(
        # ResolvedConversion fields only: a reconciliation candidate may arrive as
        # a hot AttributedConversion row (a subclass) — its old decision columns
        # must not leak through.
        **conv.model_dump(include=set(ResolvedConversion.model_fields)),
        exposure_id=exposure_id,
        assists=assists,
        attributed=attributed,
        path="hot",
        processed_at=conv.ingest_time,  # event-derived RMT version (DECISIONS Phase 3)
        reason=reason,
        candidate_households=candidate_households,
    )
