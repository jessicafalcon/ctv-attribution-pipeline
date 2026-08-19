"""Rollup refresh + report snapshot (Phase 6), both recomputed from ClickHouse
FINAL and stamped with a data-derived `reported_at` version.

`campaign_hourly` is a versioned-replace ReplacingMergeTree: each refresh rewrites
ALL (campaign_id, hour) keys with a higher `reported_at`, so FINAL is the latest
complete rollup — never an insert-triggered summing MV (a correction would
double-count) and never a TRUNCATE (CLAUDE.md). `report_snapshots` keeps one row
per (reported_at, campaign_id) so a period's number is queryable as of each pass.

Both read FINAL on the source ReplacingMergeTree tables (DECISIONS Phase 4), so
duplicate exposure landings and pre-reduction rows never inflate a denominator.
"""

from datetime import datetime

from clickhouse_connect.driver.client import Client

# Fixed period sentinel this phase (campaign-total grain). Day-grain periods slot
# in later by widening this column, no schema change (BACKLOG / agent phase).
PERIOD = "all"

# Rollup buckets by the credited exposure's event-time hour, so spend/exposures
# and the conversions credited against them share one hour axis and summing over
# hours equals the per-campaign report totals.
_REFRESH_CAMPAIGN_HOURLY = """
insert into campaign_hourly
select
    campaign_id,
    hour,
    sum(spend) as spend,
    sum(is_exposure) as exposures,
    sum(is_conversion) as attributed_conversions,
    sum(is_purchase) as purchases,
    sum(is_site_visit) as site_visits,
    sum(rev) as revenue,
    {reported_at:DateTime64(3)} as reported_at
from
(
    select
        campaign_id,
        toStartOfHour(event_time) as hour,
        spend as spend,
        1 as is_exposure,
        0 as is_conversion,
        0 as is_purchase,
        0 as is_site_visit,
        0.0 as rev
    from exposures_landed final
    union all
    select
        e.campaign_id as campaign_id,
        toStartOfHour(e.event_time) as hour,
        0 as spend,
        0 as is_exposure,
        1 as is_conversion,
        a.conversion_type = 'purchase' as is_purchase,
        a.conversion_type = 'site_visit' as is_site_visit,
        a.revenue as rev
    from attributed_conversions as a final
    inner join
    (
        select
            exposure_id,
            campaign_id,
            event_time
        from exposures_landed final
    ) as e
        on a.exposure_id = e.exposure_id
    where a.attributed = 1
)
group by
    campaign_id,
    hour
"""

# Per-campaign metrics + the raw counts behind them. Same shape as queries/
# report.sql (DECISIONS Phase 4 definitions), plus the snapshot key columns and
# the raw counts so a snapshot is self-contained. NULL on a zero denominator.
#
# {path_filter} makes the snapshot order-independent and re-run-safe: the PRE
# (hot) snapshot filters `and a.path = 'hot'` — the hot-attributed set is
# invariant under reconciliation (it only rewrites hot-UNattributed rows), so the
# pre snapshot recomputes the same hot-pass numbers no matter how many times the
# job runs. The POST snapshot has an empty filter (both paths).
_WRITE_REPORT_SNAPSHOT = """
insert into report_snapshots
with
exposures as
(
    select
        campaign_id,
        exposure_id,
        spend
    from exposures_landed final
),
spend_by_campaign as
(
    select
        campaign_id,
        sum(spend) as spend,
        count() as exposures
    from exposures
    group by campaign_id
),
credited as
(
    select
        e.campaign_id as campaign_id,
        a.conversion_type as conversion_type,
        a.revenue as revenue
    from attributed_conversions as a final
    inner join exposures as e
        on a.exposure_id = e.exposure_id
    where a.attributed = 1 /*path_filter*/
),
conv_by_campaign as
(
    select
        campaign_id,
        count() as conversions,
        countIf(conversion_type = 'purchase') as purchases,
        countIf(conversion_type = 'site_visit') as site_visits,
        sum(revenue) as revenue
    from credited
    group by campaign_id
)
select
    {reported_at:DateTime64(3)} as reported_at,
    s.campaign_id as campaign_id,
    {period:String} as period,
    s.spend as spend,
    coalesce(c.revenue, 0) as revenue,
    coalesce(c.conversions, 0) as conversions,
    coalesce(c.purchases, 0) as purchases,
    s.exposures as exposures,
    coalesce(c.revenue, 0) / nullIf(s.spend, 0) as roas,
    s.spend / nullIf(coalesce(c.purchases, 0), 0) as cpa,
    coalesce(c.conversions, 0) / nullIf(s.exposures, 0) as cvr,
    coalesce(c.site_visits, 0) / nullIf(s.exposures, 0) as site_visit_rate
from spend_by_campaign as s
left join conv_by_campaign as c
    on s.campaign_id = c.campaign_id
order by s.campaign_id
"""


def refresh_campaign_hourly(client: Client, reported_at: datetime) -> None:
    """Recompute the rollup from FINAL (current state, both paths) and insert it
    as a new version. FINAL keeps the highest reported_at per key, so this is the
    latest complete rollup."""
    client.command(_REFRESH_CAMPAIGN_HOURLY, parameters={"reported_at": reported_at})


def write_report_snapshot(
    client: Client, reported_at: datetime, *, hot_only: bool
) -> None:
    """Snapshot the four metrics per campaign as of `reported_at`. `hot_only`
    filters to path='hot' (the pre-reconciliation report — invariant under
    reconciliation); otherwise both paths (the post-reconciliation report)."""
    path_filter = "and a.path = 'hot'" if hot_only else ""
    # Plain replace, not str.format — the SQL keeps ClickHouse's own {name:Type}
    # server-side bindings, which str.format would try to interpolate.
    sql = _WRITE_REPORT_SNAPSHOT.replace("/*path_filter*/", path_filter)
    client.command(sql, parameters={"reported_at": reported_at, "period": PERIOD})
