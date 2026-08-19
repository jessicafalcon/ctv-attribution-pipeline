"""Pure collector aggregation — synthetic rows, no services, no LLM. Proves the
context math without a stack; the live read path is
tests/integration/test_context.py."""

from agent.collect import (
    build_context,
    campaign_metrics,
    campaign_restatements,
    genre_reach,
    ip_cluster_stats,
    match_rate_series,
    window_edge_stats,
)
from agent.context import AttributionContext

DAY = 86400


def test_match_rate_series_overall_and_per_day() -> None:
    processed, attributed, rate, points = match_rate_series(
        [("2026-08-01", 10, 8), ("2026-08-02", 10, 5)]
    )
    assert (processed, attributed) == (20, 13)
    assert rate == 13 / 20
    assert [p.match_rate for p in points] == [0.8, 0.5]
    assert points[0].day == "2026-08-01"


def test_match_rate_series_empty_is_none_not_crash() -> None:
    processed, attributed, rate, points = match_rate_series([])
    assert (processed, attributed, rate, points) == (0, 0, None, [])


def test_window_edge_buckets_and_near_boundary() -> None:
    # one lag in each of days 0..6, plus a second in the last (6-7d) bin.
    lags = [i * DAY + 100 for i in range(7)] + [6 * DAY + 500]
    stats = window_edge_stats(lags, window_days=7)
    assert [b.count for b in stats.buckets] == [1, 1, 1, 1, 1, 1, 2]
    assert [b.label for b in stats.buckets][0] == "0-1d"
    assert stats.attributed_hot == 8
    assert stats.near_boundary_count == 2
    assert stats.near_boundary_fraction == 2 / 8


def test_window_edge_exact_boundary_lag_lands_in_last_bin_not_overflow() -> None:
    # A lag of exactly the window (7d) is eligible (exp.event_time + window ==
    # conv.event_time) and must clamp into 6-7d, never a phantom 7-8d bin.
    stats = window_edge_stats([7 * DAY], window_days=7)
    assert len(stats.buckets) == 7
    assert stats.buckets[-1].count == 1
    assert stats.near_boundary_count == 1


def test_ip_cluster_stats_shares_and_top() -> None:
    stats = ip_cluster_stats(
        (100, 30, 12, 3),
        [("100.64.0.1", 18, 3), ("100.64.0.2", 12, 2)],
    )
    assert stats.attributed == 100
    assert stats.ip_resolved_attributed == 30
    assert stats.ambiguous_attributed == 12
    assert stats.ip_resolved_fraction == 0.30
    assert stats.max_candidate_count == 3
    assert stats.top_clusters[0].ip == "100.64.0.1"
    assert stats.top_clusters[0].attributed_conversions == 18


def test_ip_cluster_stats_zero_attributed_is_none_fraction() -> None:
    stats = ip_cluster_stats((0, 0, 0, None), [])
    assert stats.ip_resolved_fraction is None
    assert stats.max_candidate_count == 0
    assert stats.top_clusters == []


def test_genre_reach_joins_and_orders_by_genre() -> None:
    reach = genre_reach(
        [("sports", 90), ("news", 90), ("drama", 20)],
        [("sports", 69), ("news", 21)],
    )
    by_genre = {g.genre: g for g in reach}
    assert [g.genre for g in reach] == ["drama", "news", "sports"]  # sorted
    assert by_genre["sports"].conversions_per_exposure == 69 / 90
    assert by_genre["drama"].attributed_conversions == 0  # exposures, no conv
    assert by_genre["drama"].conversions_per_exposure == 0.0


def test_campaign_metrics_maps_nullable_denominators() -> None:
    rows = [("camp-00", 5.0, 100, 40, 10, 200.0, 40.0, 0.5, 0.4, 0.2)]
    (m,) = campaign_metrics(rows)
    assert (m.campaign, m.roas, m.cvr) == ("camp-00", 40.0, 0.4)
    (n,) = campaign_metrics([("camp-01", 0.0, 0, 0, 0, 0.0, None, None, None, None)])
    assert n.roas is None and n.cpa is None


def test_campaign_restatements_maps_and_empty_ok() -> None:
    assert campaign_restatements([]) == []
    (r,) = campaign_restatements([("camp-00", 10.0, 27.0, 17.0, 5, 12, 340.0)])
    assert r.roas_delta == 17.0 and r.conversions_now == 12


def _full_context() -> AttributionContext:
    return build_context(
        profile="synthetic",
        match_rate_rows=[("2026-08-01", 10, 9)],
        report_rows=[("camp-00", 5.0, 100, 40, 10, 200.0, 40.0, 0.5, 0.4, 0.2)],
        restatement_rows=[("camp-00", 10.0, 27.0, 17.0, 5, 12, 340.0)],
        window_edge_lags=[100, 6 * DAY + 1],
        ip_overall_row=(9, 3, 2, 3),
        ip_top_rows=[("100.64.0.1", 3, 3)],
        genre_exp_rows=[("sports", 50), ("news", 50)],
        genre_conv_rows=[("sports", 5), ("news", 4)],
    )


def test_build_context_assembles_full_object() -> None:
    ctx = _full_context()
    assert ctx.profile == "synthetic"
    assert ctx.processed == 10 and ctx.attributed == 9
    assert ctx.match_rate == 0.9
    assert len(ctx.campaigns) == 1
    assert len(ctx.restatements) == 1
    assert ctx.ip_clusters.ip_resolved_fraction == 3 / 9
    assert ctx.window_edge.near_boundary_count == 1
    assert len(ctx.genre_reach) == 2
