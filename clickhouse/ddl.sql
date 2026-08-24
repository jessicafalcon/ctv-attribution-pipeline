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
-- and is refused by the lake read (lake/read_attributed.py PrePhase17RowError,
-- naming the fix: re-populate from the engine) before any model is built.
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

-- Phase-18a incremental-rollup bookkeeping. The Dagster loader (lake/load_serving.py
-- — the ONE writer of the serving tables) records every (campaign_id, hour) rollup key
-- the day it loaded touches, stamped with a DATA-DERIVED version: max(processed_at)
-- over the credited conversions of that key, max(ingest_time) over its exposures (an
-- exposure has no processed_at; ingest_time is the same lineage `reported_at` is built
-- from). ReplacingMergeTree(version) keeps the HIGHEST stamp per key, so the two
-- contributions combine to "when did this key's data last move".
create table if not exists rollup_dirty
(
    campaign_id String,
    hour        DateTime('UTC'),
    version     DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(version)
order by (campaign_id, hour);

-- The refresh watermark, PER KEY: which version of each key the rollup was last
-- computed against. A key needs recomputing when its rollup_dirty version differs from
-- its row here — never a comparison against a global maximum, which left any key whose
-- own timestamps lagged the highest key's permanently unrefreshed (321 of 340 keys on
-- long_delay; review gate, Phase 18a). Written AFTER the refresh insert, so a crash
-- between the two re-refreshes the same keys next pass (idempotent) instead of
-- dropping them on the floor — the one failure the full-refresh oracle cannot see. No
-- deletes, no mutations. A key with no row here has never been refreshed and is dirty.
create table if not exists rollup_refreshed
(
    campaign_id String,
    hour        DateTime('UTC'),
    version     DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(version)
order by (campaign_id, hour);

-- Phase-6 restatement history. One row per (reported_at, campaign_id, period)
-- holding the four advertiser metrics (DECISIONS Phase 4) and the raw counts
-- behind them, so a period's reported number is queryable *as of* each pass.
-- EVERY column is serving-derived (N1 isolation): recall/precision are NOT here
-- — those are scored only in the eval harness against the side file. `period` is
-- a present-but-fixed sentinel this phase (campaign-total grain); day-grain slots
-- in later without a schema change. ReplacingMergeTree so re-running a pass
-- (byte-identical rows) converges.
--
-- Phase 18a: `snapshot_version` is the ReplacingMergeTree VERSION — max(processed_at)
-- over the rows the snapshot summarized (data-derived, no wall clock, no counter),
-- monotone across the two passes because reconciliation stamps processed_at =
-- reconciled_at, strictly greater than the hot max. Without it, which twin a merge
-- kept was undefined by construction (BACKLOG). The SORT KEY is unchanged and keeps
-- `reported_at`: `make restate` needs BOTH the pre- and post-reconciliation rows to
-- survive FINAL, so the version disambiguates twins WITHIN a key and never collapses
-- the pair. Existing volumes are migrated by clickhouse/apply.py (ClickHouse has no
-- `alter table ... modify engine`).
create table if not exists report_snapshots
(
    reported_at      DateTime64(3, 'UTC'),
    campaign_id      String,
    period           String,
    spend            Float64,
    revenue          Float64,
    conversions      UInt64,
    purchases        UInt64,
    exposures        UInt64,
    roas             Nullable(Float64),
    cpa              Nullable(Float64),
    cvr              Nullable(Float64),
    site_visit_rate  Nullable(Float64),
    snapshot_version DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(snapshot_version)
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

-- Per-query cost, from system.query_log (Phase 18b, Done-when 2). Keyed
-- (day, query_tag) — one row per tagged report/restate/bench query per day, filled
-- by `make cost-report` via the cost_rw writer. OUTSIDE the byte-identical guarantee
-- (Invariant 3): query_duration_ms / cpu_seconds / usd vary run to run, like Iceberg
-- metadata or a Dagster run id, and NO pipeline path reads this table. read_rows /
-- read_bytes are server-computed and stable; usd is an ILLUSTRATIVE conversion of a
-- measured cpu_seconds by a config rate, never a billed figure. Version = measured_at
-- (max event_time of the tag's query_log rows), so the latest measurement wins.
create table if not exists query_cost_daily
(
    day               Date,
    query_tag         String,
    query_duration_ms UInt64,
    read_rows         UInt64,
    read_bytes        UInt64,
    memory_usage      UInt64,
    cpu_seconds       Float64,
    usd               Float64,
    measured_at       DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(measured_at)
order by (day, query_tag);
