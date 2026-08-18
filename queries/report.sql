-- Reporting v1: the four advertiser metrics per campaign, from the raw serving
-- tables (the campaign_hourly rollup and the naive-vs-optimized benchmark are
-- Phase 6/7). Campaign comes from joining each credited conversion's exposure_id
-- to exposures_landed.
--
-- Reads FINAL on BOTH ReplacingMergeTree tables (DECISIONS Phase 4): a plain
-- sum/count over unmerged parts would count duplicate exposure landings and
-- pre-reduction rows, silently inflating the spend and exposure denominators.
-- Every ratio guards its denominator with nullIf(x, 0) so a zero denominator
-- yields NULL, never a divide-by-zero. Wrong-household (shared-IP) attributions
-- are kept in — they are the advertiser's reported number; the accuracy eval
-- measures the divergence separately.
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
    where a.attributed = 1
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
    s.campaign_id as campaign,
    s.spend as spend,
    s.exposures as exposures,
    coalesce(c.conversions, 0) as conversions,
    coalesce(c.purchases, 0) as purchases,
    coalesce(c.revenue, 0) as revenue,
    coalesce(c.revenue, 0) / nullIf(s.spend, 0) as roas,
    s.spend / nullIf(coalesce(c.purchases, 0), 0) as cpa,
    coalesce(c.conversions, 0) / nullIf(s.exposures, 0) as cvr,
    coalesce(c.site_visits, 0) / nullIf(s.exposures, 0) as site_visit_rate
from spend_by_campaign as s
left join conv_by_campaign as c
    on s.campaign_id = c.campaign_id
order by s.campaign_id
