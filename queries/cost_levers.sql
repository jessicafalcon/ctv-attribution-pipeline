-- Phase 13 query cost levers: DDL + before/after query pairs, parsed by
-- queries/measure_levers.py. Each block is `-- >>> name` … up to the next marker.
--
-- Runs ONLY inside `make cost-levers` against a bench_large run — never on the
-- tiny golden DDL path (clickhouse/ddl.sql), so gate-0 stays byte-identical. The
-- date window below is the bench_large mid-span reporting slice (~60h total span
-- 2026-08-01→08-03; this 12h window holds ~5.4k of 25k attributed conversions),
-- deterministic under the profile's fixed seed.
--
-- Before/after is toggled by a ClickHouse SETTING, not by DDL ordering: the lever
-- object (projection / skip index) is materialized once, then the reader is asked
-- to ignore it (before) or use it (after). Same idiom as lever 3's
-- optimize_move_to_prewhere=0 (before) vs explicit PREWHERE (after) — an
-- apples-to-apples pair on one physical table, no re-load between sides.

-- ============================ setup DDL ============================
-- Idempotent: drop then add, so a re-run starts clean. A projection on a
-- ReplacingMergeTree needs deduplicate_merge_projection_mode set (ClickHouse
-- 24.8 refuses otherwise) — 'rebuild' keeps the projection valid across the
-- dedup merges FINAL relies on. All off clickhouse/ddl.sql (the golden schema).

-- >>> setup_projection
alter table attributed_conversions modify setting deduplicate_merge_projection_mode = 'rebuild'

-- >>> drop_projection
alter table attributed_conversions drop projection if exists proj_by_event_time

-- >>> add_projection
alter table attributed_conversions add projection proj_by_event_time (select * order by event_time)

-- >>> materialize_projection
alter table attributed_conversions materialize projection proj_by_event_time

-- >>> drop_idx_genre
alter table exposures_landed drop index if exists idx_genre

-- >>> add_idx_genre
alter table exposures_landed add index idx_genre program_genre type bloom_filter granularity 1

-- >>> materialize_idx_genre
alter table exposures_landed materialize index idx_genre

-- >>> drop_idx_ip
alter table exposures_landed drop index if exists idx_ip

-- >>> add_idx_ip
alter table exposures_landed add index idx_ip ip type bloom_filter granularity 1

-- >>> materialize_idx_ip
alter table exposures_landed materialize index idx_ip

-- ==================== LEVER 1: event_time projection ====================
-- The reporting slice: attributed conversions in one reporting window. The base
-- table is sorted by conversion_id, so event_time is scattered across every
-- granule and this range predicate prunes nothing today. The projection keeps an
-- alternate copy ordered by event_time; ClickHouse auto-picks it for the range
-- and reads only the window's granules. Non-FINAL because a projection cannot
-- serve a FINAL query (ClickHouse can't guarantee the projection copy is
-- deduplicated to the same latest-version rows) — valid here because the table is
-- single-version at merged steady state (canonicalized first), so FINAL and
-- non-FINAL return identical rows.

-- >>> lever1_query
select count(), round(sum(revenue), 2)
from attributed_conversions
where event_time >= '2026-08-02 06:00:00'
  and event_time < '2026-08-02 18:00:00'
  and attributed = 1

-- ============ LEVER 2: FINAL-avoidance / skip index (negative) ============
-- Two candidates, both measured, both lose on this schema — a documented
-- negative result (DECISIONS Phase 13). 2a: SELECT … FINAL vs an explicit
-- argMax(…) GROUP BY conversion_id doing the version-collapse by hand. 2b: a
-- bloom skip index on a non-leading column (genre, and the far-more-selective
-- ip) — measured to check whether the column physically clusters.

-- >>> lever2a_final
select count(), round(sum(revenue), 2)
from attributed_conversions final
where attributed = 1

-- >>> lever2a_argmax
select count(), round(sum(revenue), 2)
from
(
    select
        conversion_id,
        argMax(revenue, processed_at) as revenue,
        argMax(attributed, processed_at) as attributed
    from attributed_conversions
    group by conversion_id
)
where attributed = 1

-- >>> lever2b_genre
select count(), round(sum(spend), 4)
from exposures_landed
where program_genre = 'sports'

-- >>> lever2b_ip
-- `{ip}` is substituted by measure_levers.py with the highest-row-count shared-pool
-- IP, chosen deterministically (order by count() desc, ip) — no seed-pinned literal.
-- The producer's shared-IP pool is 100.64.0.{i+1} (producer/graph.py), a plain
-- counter that intentionally overflows the last octet past 255 (a sim convention,
-- not a real RFC IPv4 address), so the value is genuine generator output.
select count(), round(sum(spend), 4)
from exposures_landed
where ip = '{ip}'

-- ==================== LEVER 3: PREWHERE ====================
-- A wide-column read behind a selective window predicate. WHERE (with
-- optimize_move_to_prewhere=0, so ClickHouse does NOT auto-move it) reads every
-- selected column for all rows in the scanned granules, then filters. PREWHERE
-- reads the filter columns first, then fetches the wide columns (assists array,
-- ids) only for surviving rows — fewer bytes. Measuring against the auto-moved
-- default would show no delta, since ClickHouse already moves it.

-- >>> lever3_where
select
    count(),
    sum(length(device_id)),
    sum(length(ip)),
    sum(length(order_id)),
    sum(length(arrayStringConcat(assists)))
from attributed_conversions
where event_time >= '2026-08-02 06:00:00'
  and event_time < '2026-08-02 18:00:00'
  and attributed = 1

-- >>> lever3_prewhere
select
    count(),
    sum(length(device_id)),
    sum(length(ip)),
    sum(length(order_id)),
    sum(length(arrayStringConcat(assists)))
from attributed_conversions
prewhere event_time >= '2026-08-02 06:00:00'
  and event_time < '2026-08-02 18:00:00'
  and attributed = 1
