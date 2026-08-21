"""Pydantic models — the single source of truth for every wire and row schema:
the Kafka topic schemas (`Exposure`, `Conversion`, `ResolvedConversion`, plus
the graph/truth records) AND the ClickHouse serving-table schema
(`AttributedConversion`). The topic models have JSON Schemas generated from them
(producer/schemas.py) and registered in the schema registry; never hand-edited.
`AttributedConversion` is a *table* schema, not a registered subject — its
columns live in clickhouse/ddl.sql and its insert order in streaming/sink.py.

Co-located deliberately so the output models can subclass `Conversion` without
a cross-package import cycle (see DECISIONS Phase 3). Split trigger: move the
engine models to a shared `schemas` package on the 4th output model, or the
first model producer/ has no reason to import.
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
    """A conversion mapped to a household — the in-process resolve step's
    output (no topic since Phase 16). resolution=device is a device-graph hit;
    resolution=ip is the IP fallback. ambiguous/candidate_count flag shared-IP
    fan-out (one record per candidate household). Carries every Conversion
    field so the engine can attribute without re-reading conversions."""

    household_id: str
    resolution: Literal["device", "ip"]
    ambiguous: bool
    candidate_count: int = Field(ge=1)


class AttributedConversion(ResolvedConversion):
    """A resolved conversion after the engine's last-touch attribution. Table:
    attributed_conversions (ReplacingMergeTree, key conversion_id, version
    processed_at). `exposure_id` is the credited last-touch exposure (null when
    unattributed); `assists` are the other in-window exposures in the household;
    `path` is `hot` for the streaming engine, `reconciled` for the Phase-6
    long-window job; `processed_at` is the RMT version, set to the conversion's
    `ingest_time` (event-derived, deterministic — DECISIONS Phase 3). `reason`
    says WHY a row is unattributed (Phase 16): `ambiguous_ip` — a shared-IP
    conversion the hot path refuses to guess (candidate_count > 1); `state_miss`
    — no in-window exposure in a certain household. Null when attributed."""

    exposure_id: str | None
    assists: list[str]
    attributed: bool
    path: Literal["hot", "reconciled"]
    processed_at: datetime
    reason: Literal["ambiguous_ip", "state_miss"] | None = None


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
