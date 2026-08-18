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
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from producer.models import AttributedConversion, Exposure, ResolvedConversion

# Default hot window: exposures up to 7 days before a conversion are eligible
# (ARCHITECTURE §3.3). tiny stays inside it, so every conversion is hot-path
# attributable without reconciliation (DECISIONS Phase 1).
HOT_WINDOW = timedelta(days=7)


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
