-- Restatement: a campaign's reported metric as of each pass, and the change the
-- latest pass caused. report_snapshots holds two rows per campaign this run — the
-- pre-reconciliation snapshot (earlier reported_at) and the post-reconciliation
-- one (later reported_at). argMin/argMax over reported_at collapse them to a
-- one-row-per-campaign before/after diff. Serving-layer only (report_snapshots),
-- never the accuracy side file.
--
-- Reconciliation only recovers (attributes) previously-missed conversions, so
-- conversions/revenue/ROAS can only rise between the two snapshots, never fall.
select
    campaign_id,
    argMin(roas, reported_at) as roas_as_reported,
    argMax(roas, reported_at) as roas_now,
    round(argMax(roas, reported_at) - argMin(roas, reported_at), 4) as roas_delta,
    argMin(conversions, reported_at) as conversions_as_reported,
    argMax(conversions, reported_at) as conversions_now,
    round(argMax(revenue, reported_at) - argMin(revenue, reported_at), 2) as revenue_delta
from report_snapshots final
-- group by (campaign_id, period): period is the fixed 'all' sentinel this phase,
-- so one row per campaign now; naming it here keeps the diff per-period once
-- day-grain periods land, instead of silently collapsing across them.
group by
    campaign_id,
    period
order by campaign_id
