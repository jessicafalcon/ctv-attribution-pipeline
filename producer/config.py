"""Profile schema. Profiles live in producer/profiles/*.json; every knob is here."""

import json
from datetime import datetime
from pathlib import Path

from pydantic import Field

from producer.models import StrictModel

PROFILES_DIR = Path(__file__).parent / "profiles"


class GraphConfig(StrictModel):
    n_households: int = Field(gt=0)
    devices_per_household: tuple[int, int]
    ips_per_household: tuple[int, int]
    # Fraction of household-IP slots drawn from a shared pool (CGNAT/office).
    # The ONLY source of wrong-household matches — keep it that way.
    shared_ip_fraction: float = Field(ge=0, le=1)


class LateConfig(StrictModel):
    fraction: float = Field(ge=0, le=1)
    min_minutes: float = Field(ge=0)
    max_minutes: float = Field(ge=0)


class EventsConfig(StrictModel):
    n_exposures: int = Field(gt=0)
    events_per_hour: float = Field(gt=0)  # throughput knob: sets event_time spacing
    caused_conversion_rate: float = Field(ge=0, le=1)
    organic_conversions: int = Field(ge=0)
    conversion_delay_minutes: tuple[float, float]
    purchase_fraction: float = Field(ge=0, le=1)
    revenue_range: tuple[float, float]
    late: LateConfig
    duplicate_fraction: float = Field(ge=0, le=1)
    co_view_multiplier: dict[str, float]  # genre → factor on caused-conversion rate


class Profile(StrictModel):
    name: str
    seed: int
    sim_start: datetime
    n_campaigns: int = Field(gt=0)
    spend_range: tuple[float, float]
    app_ids: list[str]
    genres: list[str]
    graph: GraphConfig
    events: EventsConfig


def load_profile(name: str) -> Profile:
    path = PROFILES_DIR / f"{name}.json"
    return Profile.model_validate(json.loads(path.read_text()))
