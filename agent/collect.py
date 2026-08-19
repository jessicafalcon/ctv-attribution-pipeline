"""Pure collector aggregation — a function of already-fetched rows only, no I/O,
no clock, no LLM. Deterministic and unit-testable with synthetic rows (mirrors
`accuracy/score.py`). `agent/readers.py` supplies the rows; `agent/run_context.py`
wires the two and prints. Building the context here calls nothing external, so
ARCHITECTURE's "collectors are deterministic and testable without an LLM" holds
by construction."""

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

_DAY_SECONDS = 86400


def _ratio(numerator: float, denominator: float) -> float | None:
    """Divide, or None on a zero denominator (never a ZeroDivisionError) — the
    same NULL-on-zero-denominator rule the report SQL uses."""
    return numerator / denominator if denominator else None


def match_rate_series(
    rows: list[tuple],
) -> tuple[int, int, float | None, list[MatchRatePoint]]:
    """`rows`: (day, processed, attributed) per calendar day. Returns overall
    processed, overall attributed, overall match rate, and the per-day series."""
    points = [
        MatchRatePoint(
            day=str(day),
            processed=int(processed),
            attributed=int(attributed),
            match_rate=_ratio(int(attributed), int(processed)),
        )
        for day, processed, attributed in rows
    ]
    processed = sum(p.processed for p in points)
    attributed = sum(p.attributed for p in points)
    return processed, attributed, _ratio(attributed, processed), points


def window_edge_stats(lag_seconds: list[int], window_days: int = 7) -> WindowEdgeStats:
    """Bucket attributed-hot attribution lags into day-wide bins across the hot
    window. The last bin (`(window-1)d–window d`) is the near-boundary count — a
    pile-up there is a window-edge effect. A lag of exactly `window` days lands in
    the last bin (clamped), never a phantom overflow bin."""
    counts = [0] * window_days
    for lag in lag_seconds:
        idx = min(int(lag) // _DAY_SECONDS, window_days - 1)
        idx = max(idx, 0)
        counts[idx] += 1
    buckets = [
        WindowEdgeBucket(label=f"{i}-{i + 1}d", count=c) for i, c in enumerate(counts)
    ]
    attributed_hot = len(lag_seconds)
    near = counts[-1] if counts else 0
    return WindowEdgeStats(
        attributed_hot=attributed_hot,
        buckets=buckets,
        near_boundary_count=near,
        near_boundary_fraction=_ratio(near, attributed_hot),
    )


def ip_cluster_stats(overall_row: tuple, top_rows: list[tuple]) -> IpClusterStats:
    """`overall_row`: (attributed, ip_resolved, ambiguous, max_candidate_count).
    `top_rows`: (ip, attributed_conversions, max_candidate_count) per IP."""
    attributed, ip_resolved, ambiguous, max_candidate = overall_row
    return IpClusterStats(
        attributed=int(attributed),
        ip_resolved_attributed=int(ip_resolved),
        ambiguous_attributed=int(ambiguous),
        ip_resolved_fraction=_ratio(int(ip_resolved), int(attributed)),
        max_candidate_count=int(max_candidate or 0),
        top_clusters=[
            IpCluster(
                ip=ip,
                attributed_conversions=int(n),
                max_candidate_count=int(maxc or 0),
            )
            for ip, n, maxc in top_rows
        ],
    )


def genre_reach(exp_rows: list[tuple], conv_rows: list[tuple]) -> list[GenreReach]:
    """Join raw exposures-per-genre to attributed-conversions-per-genre. RAW, NOT
    co-view-adjusted (BACKLOG 26). Genres with exposures but no attributed
    conversions still appear (ratio 0)."""
    conv_by_genre = {genre: int(n) for genre, n in conv_rows}
    out = []
    for genre, exposures in sorted(exp_rows):
        exposures = int(exposures)
        conversions = conv_by_genre.get(genre, 0)
        out.append(
            GenreReach(
                genre=genre,
                exposures=exposures,
                attributed_conversions=conversions,
                conversions_per_exposure=_ratio(conversions, exposures),
            )
        )
    return out


def campaign_metrics(report_rows: list[tuple]) -> list[CampaignMetrics]:
    """Map queries/report.sql rows (fixed column order) to the typed model."""
    return [
        CampaignMetrics(
            campaign=r[0],
            spend=float(r[1]),
            exposures=int(r[2]),
            conversions=int(r[3]),
            purchases=int(r[4]),
            revenue=float(r[5]),
            roas=None if r[6] is None else float(r[6]),
            cpa=None if r[7] is None else float(r[7]),
            cvr=None if r[8] is None else float(r[8]),
            site_visit_rate=None if r[9] is None else float(r[9]),
        )
        for r in report_rows
    ]


def campaign_restatements(restatement_rows: list[tuple]) -> list[CampaignRestatement]:
    """Map queries/restatement.sql rows to the typed model. Empty when
    report_snapshots is unpopulated (e.g. a hot-only run) — a valid empty list,
    not an error."""
    return [
        CampaignRestatement(
            campaign=r[0],
            roas_as_reported=None if r[1] is None else float(r[1]),
            roas_now=None if r[2] is None else float(r[2]),
            roas_delta=float(r[3]),
            conversions_as_reported=int(r[4]),
            conversions_now=int(r[5]),
            revenue_delta=float(r[6]),
        )
        for r in restatement_rows
    ]


def build_context(
    profile: str,
    match_rate_rows: list[tuple],
    report_rows: list[tuple],
    restatement_rows: list[tuple],
    window_edge_lags: list[int],
    ip_overall_row: tuple,
    ip_top_rows: list[tuple],
    genre_exp_rows: list[tuple],
    genre_conv_rows: list[tuple],
) -> AttributionContext:
    """Assemble the full §4.2 context from raw ClickHouse rows. Pure — every input
    is a plain row list, so the whole object is reproducible and LLM-free."""
    processed, attributed, match_rate, points = match_rate_series(match_rate_rows)
    return AttributionContext(
        profile=profile,
        processed=processed,
        attributed=attributed,
        match_rate=match_rate,
        match_rate_by_day=points,
        campaigns=campaign_metrics(report_rows),
        restatements=campaign_restatements(restatement_rows),
        window_edge=window_edge_stats(window_edge_lags),
        ip_clusters=ip_cluster_stats(ip_overall_row, ip_top_rows),
        genre_reach=genre_reach(genre_exp_rows, genre_conv_rows),
    )
