"""Pure device→household resolution. No I/O, no clock, no entropy: a function
of (conversion, graph) only, so the same input is byte-identical every run.

Priority: device-graph hit beats IP fallback. An IP owned by several
households fans out to one record per candidate (ambiguous); the hot path
defers such a conversion unattributed and reconciliation breaks the tie by
most-recent exposure across the candidates (Phase 16).
"""

from collections.abc import Iterable

from producer.models import Conversion, ResolvedConversion
from resolve.index import GraphIndex


def resolve_one(conv: Conversion, index: GraphIndex) -> list[ResolvedConversion]:
    """Zero, one, or (for an ambiguous shared IP) several resolved records."""
    hid = index.device_of.get(conv.device_id)
    if hid is not None:
        return [_resolved(conv, hid, "device", ambiguous=False, candidate_count=1)]

    owners = index.owners_of.get(conv.ip, [])
    if not owners:
        return []  # unknown device, IP owned by nobody → unresolvable
    n = len(owners)
    return [
        _resolved(conv, owner, "ip", ambiguous=n > 1, candidate_count=n)
        for owner in owners  # already sorted in GraphIndex
    ]


def resolve_stream(
    conversions: Iterable[Conversion], index: GraphIndex
) -> list[ResolvedConversion]:
    """Stateless map over the stream — duplicates in, duplicates out. Dedup is
    the engine's job (Phase 5), not the resolve step's."""
    out: list[ResolvedConversion] = []
    for conv in conversions:
        out.extend(resolve_one(conv, index))
    return out


def _resolved(
    conv: Conversion,
    household_id: str,
    resolution: str,
    *,
    ambiguous: bool,
    candidate_count: int,
) -> ResolvedConversion:
    return ResolvedConversion(
        **conv.model_dump(),
        household_id=household_id,
        resolution=resolution,
        ambiguous=ambiguous,
        candidate_count=candidate_count,
    )
