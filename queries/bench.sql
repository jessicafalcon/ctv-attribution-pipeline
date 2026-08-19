-- Optimized reporting (Phase 7 benchmark): the four advertiser metrics from the
-- pre-aggregated campaign_hourly rollup FINAL, instead of the naive full FINAL
-- scan-and-join of attributed_conversions ⋈ exposures_landed (queries/report.sql).
--
-- Same metric DEFINITIONS as the naive query — verified, not assumed:
-- campaign_hourly is refreshed from the identical FINAL / both-paths /
-- `attributed = 1` / exposure-id-join logic (reconcile/rollup.py
-- _REFRESH_CAMPAIGN_HOURLY), so summing its components per campaign reproduces the
-- naive totals. Sum the components THEN divide (roas = Σrevenue/Σspend); never sum
-- hourly ratios. nullIf guards every denominator, same as report.sql. Column order
-- matches report.sql so the benchmark can assert row-for-row equality (rounded).
--
-- The win is scan size: campaign_hourly holds one small pre-aggregated row per
-- (campaign, hour), so the optimized read touches far fewer rows/bytes than the
-- full-scan FINAL merge of the two raw serving tables (make bench reports both).
-- Sum the pre-aggregated components in a CTE first, then divide in the outer
-- query, so a ratio never nests one aggregate inside another (and an alias never
-- shadows a source column into sum(sum(...))).
with rollup as
(
    select
        campaign_id,
        sum(spend) as spend,
        sum(exposures) as exposures,
        sum(attributed_conversions) as conversions,
        sum(purchases) as purchases,
        sum(site_visits) as site_visits,
        sum(revenue) as revenue
    from campaign_hourly final
    group by campaign_id
)
select
    campaign_id as campaign,
    spend,
    exposures,
    conversions,
    purchases,
    revenue,
    revenue / nullIf(spend, 0) as roas,
    spend / nullIf(purchases, 0) as cpa,
    conversions / nullIf(exposures, 0) as cvr,
    site_visits / nullIf(exposures, 0) as site_visit_rate
from rollup
order by campaign
