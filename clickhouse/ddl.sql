-- Phase 3 serving-layer tables. Applied idempotently by clickhouse/apply.py.
--
-- ReplacingMergeTree keeps only the highest-version row per sort key, so a
-- replay from offset 0 or a Phase-6 reconciliation correction supersedes the
-- earlier row instead of duplicating it. Reads must use FINAL (or argMax) to
-- collapse superseded rows at query time.

create table if not exists attributed_conversions
(
    conversion_id   String,
    event_time      DateTime64(6, 'UTC'),
    ingest_time     DateTime64(6, 'UTC'),
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
    processed_at    DateTime64(6, 'UTC')
)
engine = ReplacingMergeTree(processed_at)
order by conversion_id;

-- Raw exposures, for Phase-6 reconciliation lookups and the Phase-7 naive
-- benchmark. ReplacingMergeTree (not plain MergeTree) so re-landing on a replay
-- collapses on exposure_id instead of appending duplicates (DECISIONS Phase 3);
-- the leading (campaign_id, event_time) still serves the benchmark query.
create table if not exists exposures_landed
(
    exposure_id   String,
    event_time    DateTime64(6, 'UTC'),
    ingest_time   DateTime64(6, 'UTC'),
    campaign_id   String,
    household_id  String,
    ip            String,
    app_id        String,
    program_genre String,
    spend         Float64
)
engine = ReplacingMergeTree
order by (campaign_id, event_time, exposure_id);
