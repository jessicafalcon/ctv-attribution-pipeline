"""Rollup refresh + report snapshot (Phase 6), both recomputed from ClickHouse
FINAL and stamped with a data-derived `reported_at` version.

`campaign_hourly` is a versioned-replace ReplacingMergeTree: each refresh rewrites
ALL (campaign_id, hour) keys with a higher `reported_at`, so FINAL is the latest
complete rollup — never an insert-triggered summing MV (a correction would
double-count) and never a TRUNCATE (CLAUDE.md). `report_snapshots` keeps one row
per (reported_at, campaign_id) so a period's number is queryable as of each pass.

Both read FINAL on the source ReplacingMergeTree tables (DECISIONS Phase 4), so
duplicate exposure landings and pre-reduction rows never inflate a denominator.

Monetary aggregates are summed in DECIMAL, never Float64 (fix/snapshot-float-
determinism, RUNBOOK incident 3): a Float64 `sum()` depends on the order
ClickHouse visits the parts, which differs between two passes over the same
rows, so two "identical" versioned rows could disagree in the 15th digit and
`argMax` over the ReplacingMergeTree twins picked either. Decimal addition is
exact in any order; the sums are cast to Float64 on write (`toFloat64` of an
identical Decimal is identical) and the ratios are divided as Float64 of those
exact sums — full precision, deterministic. The conversion goes THROUGH
`toString`: `toDecimal64(<Float64>, 4)` truncates the binary value (26.08 is
26.0799999… → 26.0799, found live by `make bench`), while the decimal string
("26.08") parses exactly. Exact because the producer quantizes money to cents
(`producer/generate.py` `round(…, 2)`; pinned by tests/test_money_domain.py);
scale 4 leaves headroom. Bridge, not destination: money stored as Float64 is the
root cause — Decimal64(4) end-to-end is a BACKLOG row for Phase 18a.
"""

from clickhouse_connect.driver.client import Client

# Fixed period sentinel this phase (campaign-total grain). Day-grain periods slot
# in later by widening this column, no schema change (BACKLOG / agent phase).
PERIOD = "all"


# reported_at is computed ENTIRELY server-side — `max(ingest_time) over the fixed
# state + offset_ms` — never round-tripped through a Python datetime. Reading a
# DateTime back into Python renders it in the client's local timezone, which
# differs between the `make run` subprocess and an in-process caller, stamping the
# same instant at different wall-clocks (a determinism break). Keeping it in SQL
# makes reported_at identical no matter which process writes the snapshot; the
# offset (0 for the pre/hot pass, RECONCILE_DELTA_MS for the post pass) keeps the
# two snapshots strictly ordered.


# Rollup buckets by the credited exposure's event-time hour, so spend/exposures
# and the conversions credited against them share one hour axis and summing over
# hours equals the per-campaign report totals.
_REFRESH_CAMPAIGN_HOURLY = """
insert into campaign_hourly
select
    campaign_id,
    hour,
    toFloat64(sum(toDecimal64(toString(spend), 4))) as spend,
    sum(is_exposure) as exposures,
    sum(is_conversion) as attributed_conversions,
    sum(is_purchase) as purchases,
    sum(is_site_visit) as site_visits,
    toFloat64(sum(toDecimal64(toString(rev), 4))) as revenue,
    (
        select max(t) from
        (
            select max(ingest_time) as t from exposures_landed final
            union all
            select max(ingest_time) as t from attributed_conversions final
        )
    ) + toIntervalMillisecond({offset_ms:Int64}) as reported_at
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
        toFloat64(sum(toDecimal64(toString(spend), 4))) as spend,
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
        toFloat64(sum(toDecimal64(toString(revenue), 4))) as revenue
    from credited
    group by campaign_id
)
select
    (
        select max(t) from
        (
            select max(ingest_time) as t from exposures_landed final
            union all
            select max(ingest_time) as t from attributed_conversions final
        )
    ) + toIntervalMillisecond({offset_ms:Int64}) as reported_at,
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


def refresh_campaign_hourly(client: Client, offset_ms: int) -> None:
    """Recompute the rollup from FINAL (current state, both paths) and insert it
    as a new version stamped `max(ingest_time) + offset_ms`. FINAL keeps the
    highest reported_at per key, so this is the latest complete rollup."""
    client.command(_REFRESH_CAMPAIGN_HOURLY, parameters={"offset_ms": offset_ms})


def write_report_snapshot(client: Client, offset_ms: int, *, hot_only: bool) -> None:
    """Snapshot the four metrics per campaign at `reported_at = max(ingest_time) +
    offset_ms`. `hot_only` filters to path='hot' (the pre-reconciliation report —
    invariant under reconciliation); otherwise both paths (the post report)."""
    path_filter = "and a.path = 'hot'" if hot_only else ""
    # Plain replace, not str.format — the SQL keeps ClickHouse's own {name:Type}
    # server-side bindings, which str.format would try to interpolate.
    sql = _WRITE_REPORT_SNAPSHOT.replace("/*path_filter*/", path_filter)
    client.command(sql, parameters={"offset_ms": offset_ms, "period": PERIOD})
