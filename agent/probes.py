"""The probe registry (ARCHITECTURE §4.2 "Test"; the probe contract, CLAUDE.md).

Each probe is `(name, parameterized SQL, params model, result model)` exposed to the
model as a TOOL. The model supplies parameters only — never SQL. The dispatcher
validates the params (pydantic), runs the FIXED SQL with clickhouse-connect
server-side parameter binding (`{name:Type}` placeholders — no f-string, no string
interpolation, so there is no SQL-injection surface even though `ip`/`campaign_id`/
`genre` are producer/attacker-influenceable), against the SELECT-only `agent_ro` user
(the query cannot write regardless), and maps rows to the typed result model.

Probes are per-hypothesis confirm/deny DRILL-DOWNS over handles the observe-step
`AttributionContext` already surfaced (e.g. `ip_clusters.top_clusters[].ip`), not
re-aggregations of the whole table — that keeps the observe/test boundary clean. All
reads are FINAL on both ReplacingMergeTree tables (DECISIONS Phase 4)."""

from dataclasses import dataclass

from clickhouse_connect.driver.client import Client
from pydantic import BaseModel


# ── parameter models (the tool input schemas) ────────────────────────────────
class IpParam(BaseModel):
    ip: str


class CampaignParam(BaseModel):
    campaign_id: str


class GenreParam(BaseModel):
    genre: str


class NoParam(BaseModel):
    pass


# ── result models (one per row) ──────────────────────────────────────────────
class IpClusterRow(BaseModel):
    household_id: str
    attributed_conversions: int
    max_candidate_count: int


class CampaignDayRow(BaseModel):
    day: str
    attributed: int
    revenue: float


class RestatementRow(BaseModel):
    campaign_id: str
    roas_as_reported: float | None
    roas_now: float | None
    roas_delta: float
    conversions_as_reported: int
    conversions_now: int
    revenue_delta: float


class WindowEdgeRow(BaseModel):
    day_bucket: int
    count: int


class GenreReachRow(BaseModel):
    exposures: int
    attributed_conversions: int


@dataclass(frozen=True)
class Probe:
    """A named probe: the confirm/deny question, the bound SQL, and the pydantic
    param/result types. `params` is `NoParam` for a parameter-free probe."""

    name: str
    description: str
    sql: str
    params: type[BaseModel]
    result: type[BaseModel]

    def tool_schema(self) -> dict:
        """The Anthropic tool definition. `input_schema` is the param model's JSON
        schema (an empty object for `NoParam`), so the model is constrained to the
        declared parameters — it cannot pass SQL."""
        schema = self.params.model_json_schema()
        schema.pop("title", None)
        schema.setdefault("type", "object")
        schema.setdefault("properties", {})
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": schema,
        }


# Per-household breakdown of the attributed conversions that resolved through one
# shared IP → confirm/deny DEVICE_GRAPH_MISMATCH (many households on one IP = the
# wrong-household fault; candidate_count is the fan-out width).
_IP_CLUSTER_DETAIL = """
select
    household_id as household_id,
    count() as attributed_conversions,
    max(candidate_count) as max_candidate_count
from attributed_conversions final
where attributed = 1
    and resolution = 'ip'
    and ip = {ip:String}
group by household_id
order by attributed_conversions desc, household_id
"""

# Attributed conversions + revenue per day for one campaign → REAL_PERFORMANCE_CHANGE
# (a gradual organic climb) vs UPSTREAM_DATA_CHANGE (a step). Campaign comes from the
# credited exposure, so this joins to exposures_landed on exposure_id. NOT a per-day
# match *rate* — a per-campaign `processed` denominator is undefined (an unattributed
# conversion carries no campaign), so this is the per-campaign attributed trend; the
# rate discrimination lives in the context's global match_rate_by_day (CR-1).
_CAMPAIGN_ATTRIBUTED_BY_DAY = """
select
    toString(toDate(a.event_time)) as day,
    count() as attributed,
    sum(a.revenue) as revenue
from attributed_conversions as a final
inner join exposures_landed as e final
    on a.exposure_id = e.exposure_id
where a.attributed = 1
    and e.campaign_id = {campaign_id:String}
group by day
order by day
"""

# One campaign's PRE-vs-now restatement (queries/restatement.sql, filtered) →
# LATE_ARRIVAL_DISTORTION: a material roas_delta means the period was restated after
# advertisers may have acted on it.
_CAMPAIGN_RESTATEMENT = """
select
    campaign_id as campaign_id,
    argMin(roas, reported_at) as roas_as_reported,
    argMax(roas, reported_at) as roas_now,
    round(argMax(roas, reported_at) - argMin(roas, reported_at), 4) as roas_delta,
    argMin(conversions, reported_at) as conversions_as_reported,
    argMax(conversions, reported_at) as conversions_now,
    round(
        argMax(revenue, reported_at) - argMin(revenue, reported_at), 2
    ) as revenue_delta
from report_snapshots final
where campaign_id = {campaign_id:String}
group by campaign_id, period
order by campaign_id
"""

# Attribution-lag histogram (day-wide buckets) over the 7d hot window for attributed
# HOT rows → WINDOW_EDGE_EFFECT: a pile-up in the last bucket is a boundary effect.
_WINDOW_EDGE_DISTRIBUTION = """
select
    intDiv(dateDiff('second', e.event_time, a.event_time), 86400) as day_bucket,
    count() as count
from attributed_conversions as a final
inner join exposures_landed as e final
    on a.exposure_id = e.exposure_id
where a.attributed = 1
    and a.path = 'hot'
    and a.event_time >= e.event_time
group by day_bucket
order by day_bucket
"""

# RAW exposures / attributed-conversions for one genre → the (caveated) co-view check.
# NOT co-view-adjusted (BACKLOG 26): a high ratio is suggestive, never CONFIDENT on
# its own — the loop routes an unexplained genre skew to AMBIGUOUS_NEEDS_HUMAN.
_GENRE_REACH_DETAIL = """
select
    (
        select count()
        from exposures_landed final
        where program_genre = {genre:String}
    ) as exposures,
    (
        select count()
        from attributed_conversions as a final
        inner join exposures_landed as e final
            on a.exposure_id = e.exposure_id
        where a.attributed = 1
            and e.program_genre = {genre:String}
    ) as attributed_conversions
"""


REGISTRY: dict[str, Probe] = {
    p.name: p
    for p in [
        Probe(
            name="ip_cluster_detail",
            description=(
                "Per-household breakdown of attributed conversions that resolved "
                "through one shared IP. Confirms/denies a wrong-household "
                "(device-graph-mismatch) cluster. Param: ip (from "
                "ip_clusters.top_clusters in the context)."
            ),
            sql=_IP_CLUSTER_DETAIL,
            params=IpParam,
            result=IpClusterRow,
        ),
        Probe(
            name="campaign_attributed_by_day",
            description=(
                "Attributed conversions and revenue per day for one campaign (the "
                "per-campaign attributed trend, not a rate). Tells a gradual organic "
                "lift (real-performance-change) from a step (upstream-data-change); "
                "read alongside the context's global match_rate_by_day. Param: "
                "campaign_id."
            ),
            sql=_CAMPAIGN_ATTRIBUTED_BY_DAY,
            params=CampaignParam,
            result=CampaignDayRow,
        ),
        Probe(
            name="campaign_restatement",
            description=(
                "One campaign's ROAS/conversions/revenue as reported before "
                "reconciliation vs now. A material roas_delta is late-arrival "
                "distortion. Param: campaign_id."
            ),
            sql=_CAMPAIGN_RESTATEMENT,
            params=CampaignParam,
            result=RestatementRow,
        ),
        Probe(
            name="window_edge_distribution",
            description=(
                "Attribution-lag histogram (day buckets) over the 7d hot window for "
                "attributed hot rows. A pile-up in the last bucket is a window-edge "
                "effect. No parameters."
            ),
            sql=_WINDOW_EDGE_DISTRIBUTION,
            params=NoParam,
            result=WindowEdgeRow,
        ),
        Probe(
            name="genre_reach_detail",
            description=(
                "RAW exposures and attributed conversions for one genre (NOT "
                "co-view-adjusted). Suggestive of co-view inflation but never "
                "confirms it alone. Param: genre."
            ),
            sql=_GENRE_REACH_DETAIL,
            params=GenreParam,
            result=GenreReachRow,
        ),
    ]
}


def tool_schemas() -> list[dict]:
    """The Anthropic tool list, in stable registry order — part of the cacheable
    prefix (Ruling B), so the order must not vary run to run."""
    return [p.tool_schema() for p in REGISTRY.values()]


def run(client: Client, name: str, params: dict) -> list[BaseModel]:
    """Validate `params` against the probe's model, run its FIXED SQL with
    server-side-bound parameters as `agent_ro`, and map rows to the typed result BY
    COLUMN NAME (CR-2). Every probe SQL aliases its columns to the result-model field
    names, so `zip(column_names, row)` is order-independent — a future SQL/field
    reorder can't silently swap two same-typed values (e.g. roas_as_reported /
    roas_now); a dropped/renamed alias surfaces as a loud pydantic ValidationError
    instead. A ValidationError (bad params or mismap) or KeyError (unknown probe) is
    caught by the loop, never silently swallowed."""
    probe = REGISTRY[name]
    validated = probe.params.model_validate(params)
    result = client.query(probe.sql, parameters=validated.model_dump())
    columns = result.column_names
    return [
        probe.result(**dict(zip(columns, row, strict=True)))
        for row in result.result_rows
    ]
