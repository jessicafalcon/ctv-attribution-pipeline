"""Seeded event generator. One rng, one pass, fixed call order → same seed
and profile give byte-identical output.

Emit order is arrival order (ingest_time, then duplicate re-sends), which is
how a real stream would hit the broker: late events appear late.
"""

import random
from datetime import datetime, timedelta
from typing import NamedTuple

from producer.config import Profile
from producer.graph import build_graph
from producer.models import (
    Conversion,
    Device,
    DeviceGraph,
    Exposure,
    Household,
    TruthLink,
)

# On-time events get up to this much ingest jitter; anything beyond is "late".
ON_TIME_MAX_JITTER_S = 60


class GeneratedStream(NamedTuple):
    graph: DeviceGraph
    exposures: list[Exposure]  # emit order, duplicates included
    conversions: list[Conversion]  # emit order, duplicates included
    truth_links: list[TruthLink]


def _ingest_time(
    event_time: datetime, rng: random.Random, profile: Profile
) -> datetime:
    late = profile.events.late
    if late.fraction > 0 and rng.random() < late.fraction:
        delay_s = rng.uniform(late.min_minutes * 60, late.max_minutes * 60)
    else:
        delay_s = rng.uniform(1, ON_TIME_MAX_JITTER_S)
    return event_time + timedelta(seconds=round(delay_s, 3))


def _conversion_device(household: Household, rng: random.Random) -> Device:
    personal = [d for d in household.devices if d.kind != "tv"]
    return rng.choice(personal or household.devices)


def _with_duplicates[E: (Exposure, Conversion)](
    events: list[E], rng: random.Random, fraction: float
) -> list[E]:
    """Assign an arrival slot per event; duplicates re-send the same payload later."""
    arrivals = [(e.ingest_time, 0, e) for e in events]
    for e in events:
        if fraction > 0 and rng.random() < fraction:
            arrivals.append(
                (e.ingest_time + timedelta(seconds=rng.uniform(10, 300)), 1, e)
            )
    arrivals.sort(key=lambda t: (t[0], _event_id(t[2]), t[1]))
    return [e for _, _, e in arrivals]


def _event_id(e: Exposure | Conversion) -> str:
    return e.exposure_id if isinstance(e, Exposure) else e.conversion_id


def generate(profile: Profile, seed: int) -> GeneratedStream:
    rng = random.Random(seed)
    graph = build_graph(profile.graph, rng)
    ev = profile.events
    spacing_s = 3600.0 / ev.events_per_hour
    span_s = ev.n_exposures * spacing_s

    exposures: list[Exposure] = []
    for i in range(ev.n_exposures):
        household = rng.choice(graph.households)
        event_time = profile.sim_start + timedelta(seconds=round(i * spacing_s, 3))
        exposures.append(
            Exposure(
                exposure_id=f"e-{i:06d}",
                event_time=event_time,
                ingest_time=_ingest_time(event_time, rng, profile),
                campaign_id=f"camp-{rng.randrange(profile.n_campaigns):02d}",
                household_id=household.household_id,
                ip=rng.choice(household.ips),
                app_id=rng.choice(profile.app_ids),
                program_genre=rng.choice(profile.genres),
                spend=round(rng.uniform(*profile.spend_range), 2),
            )
        )

    households_by_id = {h.household_id: h for h in graph.households}
    conversions: list[Conversion] = []
    truth_links: list[TruthLink] = []
    unknown_device_count = 0

    def make_conversion(
        household: Household, event_time: datetime, caused_by: str | None
    ) -> None:
        nonlocal unknown_device_count
        cid = f"c-{len(conversions):06d}"
        purchase = rng.random() < ev.purchase_fraction
        # Unknown device (guest/roommate/id churn): the graph never learned
        # it, so downstream resolution must fall back to the IP. The IP stays
        # the true household's (same home network), keeping shared IPs the
        # sole source of wrong-household matches. `u-` namespace is disjoint
        # from graph `d-` ids by construction.
        if rng.random() < ev.unknown_device_fraction:
            device_id = f"u-{unknown_device_count:06d}"
            unknown_device_count += 1
        else:
            device_id = _conversion_device(household, rng).device_id
        conversions.append(
            Conversion(
                conversion_id=cid,
                event_time=event_time,
                ingest_time=_ingest_time(event_time, rng, profile),
                device_id=device_id,
                ip=rng.choice(household.ips),
                conversion_type="purchase" if purchase else "site_visit",
                revenue=round(rng.uniform(*ev.revenue_range), 2) if purchase else 0.0,
                order_id=f"o-{cid}" if purchase else None,
            )
        )
        if caused_by is not None:
            truth_links.append(
                TruthLink(conversion_id=cid, truth_exposure_id=caused_by)
            )

    for exp in exposures:
        rate = ev.caused_conversion_rate * ev.co_view_multiplier.get(
            exp.program_genre, 1.0
        )
        if rng.random() < min(1.0, rate):
            delay_s = rng.uniform(
                ev.conversion_delay_minutes[0] * 60, ev.conversion_delay_minutes[1] * 60
            )
            make_conversion(
                households_by_id[exp.household_id],
                exp.event_time + timedelta(seconds=round(delay_s, 3)),
                caused_by=exp.exposure_id,
            )

    for _ in range(ev.organic_conversions):
        household = rng.choice(graph.households)
        event_time = profile.sim_start + timedelta(
            seconds=round(rng.uniform(0, span_s), 3)
        )
        make_conversion(household, event_time, caused_by=None)

    return GeneratedStream(
        graph=graph,
        exposures=_with_duplicates(exposures, rng, ev.duplicate_fraction),
        conversions=_with_duplicates(conversions, rng, ev.duplicate_fraction),
        truth_links=truth_links,
    )
