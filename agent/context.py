"""The typed `AttributionContext` (ARCHITECTURE §4.2) — the observation object
the Phase-9 agent reasons over. Every field is ClickHouse-derived (N1: the causal
side file is never read, never loaded into the DB) and maps to a named §4.1
hypothesis:

  match_rate / match_rate_by_day  → real-lift-vs-inflation, upstream data change
  campaigns                        → ROAS/CPA discontinuities, real change
  restatements                     → late-arrival distortion
  window_edge                      → attribution-window edge effects
  ip_clusters                      → wrong-household / shared-IP matches
  genre_reach                      → co-view inflation (RAW reach only)

FROZEN CONTRACT (DECISIONS Phase 8): this shape is what Phase 9 consumes. Phase 9
adds probe SQL + ranking over this model and MUST NOT add or rename fields — a
genuinely-needed new field is a STOP-and-report back-edit to this contract, not
routine churn. `genre_reach` is RAW exposures/conversions per genre, NOT the
co-view-*adjusted* factor (BACKLOG 26, deferred to Phase 10).
"""

from pydantic import BaseModel


class MatchRatePoint(BaseModel):
    """Attributed share of conversions for one calendar day (conversion
    event_time). A jump or drop over time is a match-rate anomaly (§4.1)."""

    day: str
    processed: int
    attributed: int
    match_rate: float | None


class CampaignMetrics(BaseModel):
    """The four advertiser metrics for one campaign, straight from report.sql
    (nullable where a denominator was zero). ROAS/CPA discontinuities live here."""

    campaign: str
    spend: float
    exposures: int
    conversions: int
    purchases: int
    revenue: float
    roas: float | None
    cpa: float | None
    cvr: float | None
    site_visit_rate: float | None


class CampaignRestatement(BaseModel):
    """A campaign's ROAS/conversions/revenue as reported before reconciliation vs
    now, from restatement.sql over report_snapshots. The late-arrival-distortion
    signal — a material `roas_delta` means a period was restated after advertisers
    may have acted on it."""

    campaign: str
    roas_as_reported: float | None
    roas_now: float | None
    roas_delta: float
    conversions_as_reported: int
    conversions_now: int
    revenue_delta: float


class WindowEdgeBucket(BaseModel):
    """Attributed hot conversions whose attribution lag (conversion event_time −
    credited exposure event_time) falls in one day-wide bucket of the hot window."""

    label: str
    count: int


class WindowEdgeStats(BaseModel):
    """Distribution of attribution lag across the 7d hot window for attributed
    HOT rows. Clustering in the last bucket (near_boundary) is a window-edge
    effect (§4.1) — conversions credited to an exposure right at the window edge."""

    attributed_hot: int
    buckets: list[WindowEdgeBucket]
    near_boundary_count: int
    near_boundary_fraction: float | None


class IpCluster(BaseModel):
    """One shared IP and how many attributed conversions resolved through it."""

    ip: str
    attributed_conversions: int
    max_candidate_count: int


class IpClusterStats(BaseModel):
    """Shares of attributed conversions that resolved via IP fallback / were
    ambiguous shared-IP fan-outs — the wrong-household signal (§4.1). This is the
    discriminator that tells the near-miss pair apart: a real lift raises match
    rate with `ip_resolved_fraction` flat; a shared-IP spike raises it with this
    fraction elevated (DECISIONS Phase 8). Counted over ATTRIBUTED rows: since
    Phase 16 the hot path never credits an ambiguous shared-IP row, so on a
    hot-only serving DB `ambiguous_attributed` is structurally 0 and the signal
    appears only after a reconcile pass has credited those rows (DECISIONS Phase
    16; `make run` / `make agent-eval` reconcile first)."""

    attributed: int
    ip_resolved_attributed: int
    ambiguous_attributed: int
    ip_resolved_fraction: float | None
    max_candidate_count: int
    top_clusters: list[IpCluster]


class GenreReach(BaseModel):
    """RAW per-genre reach: exposures, attributed conversions credited to an
    exposure of this genre, and their ratio. NOT co-view-adjusted (the adjusted
    factor is deferred — BACKLOG 26). An implausibly high ratio for one genre is
    the co-view-inflation signal (§4.1)."""

    genre: str
    exposures: int
    attributed_conversions: int
    conversions_per_exposure: float | None


class AttributionContext(BaseModel):
    """The full §4.2 observation snapshot, built by the deterministic collectors
    with zero LLM calls. Populated from ClickHouse serving tables + report
    snapshots only."""

    profile: str
    processed: int
    attributed: int
    match_rate: float | None
    match_rate_by_day: list[MatchRatePoint]
    campaigns: list[CampaignMetrics]
    restatements: list[CampaignRestatement]
    window_edge: WindowEdgeStats
    ip_clusters: IpClusterStats
    genre_reach: list[GenreReach]
