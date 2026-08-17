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


class Device(StrictModel):
    device_id: str
    kind: Literal["tv", "phone", "laptop", "tablet"]


class Household(StrictModel):
    """One device-graph entry. Topic: device_graph (compacted), key household_id."""

    household_id: str
    devices: list[Device]
    ips: list[str]


class DeviceGraph(StrictModel):
    households: list[Household]


class TruthLink(StrictModel):
    """Hidden causal link, written to a side file the pipeline never reads."""

    conversion_id: str
    truth_exposure_id: str
