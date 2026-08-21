-- Phase 3 serving-layer tables. Applied idempotently by clickhouse/apply.py.
--
-- ReplacingMergeTree keeps only the highest-version row per sort key, so a
-- replay from offset 0 or a Phase-6 reconciliation correction supersedes the
-- earlier row instead of duplicating it. Reads must use FINAL (or argMax) to
-- collapse superseded rows at query time.

create table if not exists attributed_conversions
(
    conversion_id   String,
    event_time      DateTime64(3, 'UTC'),
    ingest_time     DateTime64(3, 'UTC'),
    device_id       String,
    ip              String,
    conversion_type String,
    revenue         Float64,
    order_id        Nullable(String),
    household_id    String,
    resolution      String,
    ambiguous       UInt8,
    candidate_count UInt32,
    exposure_id     Nullable(String),
    assists         Array(String),
    attributed      UInt8,
    path            String,
    processed_at    DateTime64(3, 'UTC'),
    reason          Nullable(String),
    candidate_households Array(String)
)
engine = ReplacingMergeTree(processed_at)
order by conversion_id;

-- Phase 16 additive migration for volumes created before `reason` existed
-- (idempotent: no-op on a fresh table, which already has the column above).
-- reason: why a row is unattributed — 'ambiguous_ip' (shared-IP conversion the
-- hot path refuses to guess) or 'state_miss' (no in-window exposure); NULL when
-- attributed. An explicit contract for reconciliation / the agent / later
-- phases instead of re-deriving it from attributed=0 and candidate_count.
alter table attributed_conversions add column if not exists reason Nullable(String);

-- Phase 17 additive migration (same pattern). candidate_households: the FULL
-- candidate set the engine saw when it deferred a shared-IP conversion (sorted
-- owner households of the IP); empty when candidate_count = 1. Reconciliation
-- explodes this array — it no longer reads the device graph — and household_id
-- on an ambiguous row stays the min-candidate placeholder (the RMT key, never a
-- decision). An ambiguous row written before this column exists reads back as []
-- and is refused by the model validator until the next engine pass rewrites it.
alter table attributed_conversions
    add column if not exists candidate_households Array(String);

-- Raw exposures, for Phase-6 reconciliation lookups and the Phase-7 naive
-- benchmark. ReplacingMergeTree (not plain MergeTree) so re-landing on a replay
-- collapses on exposure_id instead of appending duplicates (DECISIONS Phase 3);
-- the leading (campaign_id, event_time) still serves the benchmark query.
create table if not exists exposures_landed
(
    exposure_id   String,
    event_time    DateTime64(3, 'UTC'),
    ingest_time   DateTime64(3, 'UTC'),
    campaign_id   String,
    household_id  String,
    ip            String,
    app_id        String,
    program_genre String,
    spend         Float64
)
engine = ReplacingMergeTree
order by (campaign_id, event_time, exposure_id);

-- Phase-6 rollup. Per (campaign_id, hour) aggregates, recomputed from FINAL on
-- each refresh and re-inserted with a higher `reported_at` version — a
-- versioned-replace ReplacingMergeTree, NOT an insert-triggered summing MV (a
-- reconciliation correction would double-count under summing) and NOT a TRUNCATE
-- (destructive-command rule). Every refresh rewrites ALL keys, so FINAL (highest
-- reported_at per key) is the latest complete rollup. `hour` is the credited
-- exposure's event-time hour, so summing campaign_hourly over hours equals the
-- per-campaign report totals.
create table if not exists campaign_hourly
(
    campaign_id            String,
    hour                   DateTime('UTC'),
    spend                  Float64,
    exposures              UInt64,
    attributed_conversions UInt64,
    purchases              UInt64,
    site_visits            UInt64,
    revenue                Float64,
    reported_at            DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(reported_at)
order by (campaign_id, hour);

-- Phase-6 restatement history. One row per (reported_at, campaign_id, period)
-- holding the four advertiser metrics (DECISIONS Phase 4) and the raw counts
-- behind them, so a period's reported number is queryable *as of* each pass.
-- EVERY column is serving-derived (N1 isolation): recall/precision are NOT here
-- — those are scored only in the eval harness against the side file. `period` is
-- a present-but-fixed sentinel this phase (campaign-total grain); day-grain slots
-- in later without a schema change. ReplacingMergeTree so re-running a pass
-- (byte-identical rows) converges.
create table if not exists report_snapshots
(
    reported_at     DateTime64(3, 'UTC'),
    campaign_id     String,
    period          String,
    spend           Float64,
    revenue         Float64,
    conversions     UInt64,
    purchases       UInt64,
    exposures       UInt64,
    roas            Nullable(Float64),
    cpa             Nullable(Float64),
    cvr             Nullable(Float64),
    site_visit_rate Nullable(Float64)
)
engine = ReplacingMergeTree
order by (reported_at, campaign_id, period);

-- Eval guard marker (BACKLOG 43). A single-row table naming which profile
-- populated the serving tables, so `make eval` can refuse to score one profile
-- against a DB populated from another (e.g. scoring tiny against a long_delay DB
-- → ~0.17, silently). OFF the golden-compared path (attributed_conversions /
-- exposures_landed FINAL), so gate-0 stays byte-identical. Keyed on the constant
-- k=0: the populate path's insert replaces the row, so a replay converges. No
-- timestamp — the marker is fully deterministic (only a profile string).
create table if not exists eval_meta
(
    k       UInt8,
    profile String
)
engine = ReplacingMergeTree
order by k;
