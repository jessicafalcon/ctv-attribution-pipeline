"""Profile schema. Profiles live in producer/profiles/*.json; every knob is here."""

import json
from pathlib import Path

from pydantic import AwareDatetime, Field, model_validator

from producer.models import StrictModel

PROFILES_DIR = Path(__file__).parent / "profiles"


def _check_ordered(name: str, pair: tuple[float, float] | tuple[int, int]) -> None:
    if pair[0] > pair[1]:
        raise ValueError(f"{name}: min {pair[0]} > max {pair[1]}")


class GraphConfig(StrictModel):
    n_households: int = Field(gt=0)
    devices_per_household: tuple[int, int]
    ips_per_household: tuple[int, int]
    # Fraction of household-IP slots drawn from a shared pool (CGNAT/office).
    # The ONLY source of wrong-household matches — keep it that way.
    shared_ip_fraction: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def _ranges_ordered(self) -> "GraphConfig":
        _check_ordered("devices_per_household", self.devices_per_household)
        _check_ordered("ips_per_household", self.ips_per_household)
        return self


class LateConfig(StrictModel):
    fraction: float = Field(ge=0, le=1)
    min_minutes: float = Field(ge=0)
    max_minutes: float = Field(ge=0)

    @model_validator(mode="after")
    def _range_ordered(self) -> "LateConfig":
        _check_ordered("late minutes", (self.min_minutes, self.max_minutes))
        return self


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
    # Fraction of conversions fired from a device the graph hasn't learned
    # (guest/roommate/id churn) — forces IP-based resolution downstream.
    unknown_device_fraction: float = Field(ge=0, le=1)
    co_view_multiplier: dict[str, float]  # genre → factor on caused-conversion rate

    @model_validator(mode="after")
    def _ranges_ordered(self) -> "EventsConfig":
        _check_ordered("conversion_delay_minutes", self.conversion_delay_minutes)
        _check_ordered("revenue_range", self.revenue_range)
        return self


class Profile(StrictModel):
    name: str
    seed: int
    sim_start: AwareDatetime  # naive would serialize differently — break byte-identity
    n_campaigns: int = Field(gt=0)
    spend_range: tuple[float, float]
    app_ids: list[str]
    genres: list[str]
    graph: GraphConfig
    events: EventsConfig

    @model_validator(mode="after")
    def _ranges_ordered(self) -> "Profile":
        _check_ordered("spend_range", self.spend_range)
        return self


def load_profile(name: str) -> Profile:
    # Accept only names actually present in profiles/ — not paths.
    available = sorted(p.stem for p in PROFILES_DIR.glob("*.json"))
    if name not in available:
        raise ValueError(f"unknown profile {name!r}; available: {available}")
    path = PROFILES_DIR / f"{name}.json"
    return Profile.model_validate(json.loads(path.read_text()))
