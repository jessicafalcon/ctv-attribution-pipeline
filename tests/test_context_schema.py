"""Freeze the AttributionContext contract (DECISIONS Phase 8, Ruling B). Phase 9
consumes this shape and must NOT add or rename fields — a genuinely-needed new
field is a STOP-and-report back-edit to Phase 8, which means changing THIS test in
the same deliberate change, never silently. The pins double as the field-coverage
proof that every §4.2 signal is present."""

from agent.context import (
    AttributionContext,
    CampaignMetrics,
    CampaignRestatement,
    GenreReach,
    IpCluster,
    IpClusterStats,
    MatchRatePoint,
    WindowEdgeBucket,
    WindowEdgeStats,
)

EXPECTED_FIELDS = {
    AttributionContext: {
        "profile",
        "processed",
        "attributed",
        "match_rate",
        "match_rate_by_day",
        "campaigns",
        "restatements",
        "window_edge",
        "ip_clusters",
        "genre_reach",
    },
    MatchRatePoint: {"day", "processed", "attributed", "match_rate"},
    CampaignMetrics: {
        "campaign",
        "spend",
        "exposures",
        "conversions",
        "purchases",
        "revenue",
        "roas",
        "cpa",
        "cvr",
        "site_visit_rate",
    },
    CampaignRestatement: {
        "campaign",
        "roas_as_reported",
        "roas_now",
        "roas_delta",
        "conversions_as_reported",
        "conversions_now",
        "revenue_delta",
    },
    WindowEdgeBucket: {"label", "count"},
    WindowEdgeStats: {
        "attributed_hot",
        "buckets",
        "near_boundary_count",
        "near_boundary_fraction",
    },
    IpCluster: {"ip", "attributed_conversions", "max_candidate_count"},
    IpClusterStats: {
        "attributed",
        "ip_resolved_attributed",
        "ambiguous_attributed",
        "ip_resolved_fraction",
        "max_candidate_count",
        "top_clusters",
    },
    GenreReach: {
        "genre",
        "exposures",
        "attributed_conversions",
        "conversions_per_exposure",
    },
}


def test_context_field_set_is_frozen() -> None:
    for model, expected in EXPECTED_FIELDS.items():
        assert set(model.model_fields) == expected, model.__name__


def test_every_section_4_2_signal_has_a_field() -> None:
    # Each §4.1 hypothesis the agent ranks needs a §4.2 field to read.
    f = AttributionContext.model_fields
    assert "match_rate_by_day" in f  # match-rate anomaly / real-vs-inflation
    assert "campaigns" in f  # ROAS/CPA discontinuity
    assert "restatements" in f  # late-arrival distortion
    assert "window_edge" in f  # window-edge effects
    assert "ip_clusters" in f  # wrong-household / shared-IP
    assert "genre_reach" in f  # co-view inflation (raw)
