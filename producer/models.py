"""Pydantic event models — the source of truth for all topic schemas.

JSON Schemas are generated from these models (producer/schemas.py) and
registered in the schema registry; never hand-edited.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Exposure(StrictModel):
    """A TV ad impression. Topic: exposures, keyed by household_id."""

    exposure_id: str
    event_time: datetime
    ingest_time: datetime
    campaign_id: str
    household_id: str
    ip: str
    app_id: str
    program_genre: str
    spend: float = Field(ge=0)


class Conversion(StrictModel):
    """A pixel fire on a personal device. Topic: conversions, keyed by device_id."""

    conversion_id: str
    event_time: datetime
    ingest_time: datetime
    device_id: str
    ip: str
    conversion_type: Literal["site_visit", "purchase"]
    revenue: float = Field(ge=0)
    order_id: str | None


class ResolvedConversion(Conversion):
    """A conversion mapped to a household. Topic: conversions_resolved, keyed
    by household_id. resolution=device is a device-graph hit; resolution=ip is
    the IP fallback. ambiguous/candidate_count flag shared-IP fan-out (one
    record per candidate household). Carries every Conversion field so the
    engine can attribute without re-reading conversions."""

    household_id: str
    resolution: Literal["device", "ip"]
    ambiguous: bool
    candidate_count: int = Field(ge=1)


class Device(StrictModel):
    device_id: str
    kind: Literal["tv", "phone", "laptop", "tablet"]


class Household(StrictModel):
    """One device-graph entry. Topic: device_graph (compacted), key household_id."""

    household_id: str
    devices: list[Device]
    ips: list[str]


class DeviceGraph(StrictModel):
    """Internal container only — the device_graph topic carries one Household
    message per key, never a single DeviceGraph message."""

    households: list[Household]


class TruthLink(StrictModel):
    """Hidden causal link, written to a side file the pipeline never reads."""

    conversion_id: str
    truth_exposure_id: str
