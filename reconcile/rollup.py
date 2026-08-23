"""Rollup refresh + report snapshot (Phase 6), both recomputed from ClickHouse
FINAL and stamped with a data-derived `reported_at` version.

`campaign_hourly` is a versioned-replace ReplacingMergeTree: each refresh rewrites
ALL (campaign_id, hour) keys with a higher `reported_at`, so FINAL is the latest
complete rollup — never an insert-triggered summing MV (a correction would
double-count) and never a TRUNCATE (CLAUDE.md). `report_snapshots` keeps one row
per (reported_at, campaign_id) so a period's number is queryable as of each pass,
versioned by `snapshot_version` (Phase 18a) so a twin on one sort key is decided
by the data, not by merge timing.

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

# The tables this module writes MONEY into. tests/test_rollup_decimal.py scans exactly
# these INSERTs for the Decimal path (fix/snapshot-float-determinism) — and DERIVES the
# expected set from clickhouse/ddl.sql (every table with a money column), asserting it
# equals this tuple. So a new money-bearing table fails the tripwire by omission
# instead of slipping past it: fail-closed by construction, not by remembering. The
# module's other INSERTs (rollup_dirty, rollup_refreshed) carry no money.
MONEY_TABLES = ("campaign_hourly", "report_snapshots")


# reported_at is computed ENTIRELY server-side, never round-tripped through a Python
# datetime: reading a DateTime back into Python renders it in the caller's local
# timezone, so the same instant would be stamped at different wall-clocks by `make
# run` and by an in-process caller (a determinism break).
#
# For `campaign_hourly` (Phase 18a, review-gate round 3) it is also DATA-DERIVED PER
# KEY: `max(stamp)` over the rows that key summarizes — an exposure's `ingest_time`,
# a credited conversion's `processed_at`. Same rule as `snapshot_version`. The
# consequence is the invariant the whole incremental path rests on: the SAME CONTENT
# always carries the SAME VERSION, whoever computed it. A hot load, a reconcile
# refresh, a replay and a full rebuild agree; a key whose data moved gets a strictly
# higher stamp (a reconciled `processed_at` exceeds every hot one) and supersedes;
# a key whose data did not move re-inserts an identical row the ReplacingMergeTree
# collapses. The earlier scheme stamped `max(ingest_time) + offset_ms` from the
# CALLER, which meant a replay (offset 0) wrote rows the RMT discarded as older than
# a reconcile pass's (offset 1000) while the dirty bookkeeping marked those keys
# clean — silently stale, exactly the class this phase exists to remove.
#
# `report_snapshots` keeps its caller offset: its pre/post pair is a deliberate TWO
# ROW history per campaign (the restatement view reads both), not a version race.


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
    max(stamp) as reported_at
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
        0.0 as rev,
        ingest_time as stamp
    from exposures_landed final
    /*dirty_exposures*/
    union all
    select
        e.campaign_id as campaign_id,
        toStartOfHour(e.event_time) as hour,
        0 as spend,
        0 as is_exposure,
        1 as is_conversion,
        a.conversion_type = 'purchase' as is_purchase,
        a.conversion_type = 'site_visit' as is_site_visit,
        a.revenue as rev,
        a.processed_at as stamp
    from attributed_conversions as a final
    inner join
    (
        select
            exposure_id,
            campaign_id,
            event_time
        from exposures_landed final
        /*dirty_exposures*/
    ) as e
        on a.exposure_id = e.exposure_id
    where a.attributed = 1 /*path_filter*/
)
group by
    campaign_id,
    hour
"""

# Per-campaign metrics + the raw counts behind them. Same shape as queries/
# report.sql (DECISIONS Phase 4 definitions), plus the snapshot key columns and
# the raw counts so a snapshot is self-contained. NULL on a zero denominator.
#
# snapshot_version (Phase 18a) is the table's ReplacingMergeTree VERSION:
# max(processed_at) over the CREDITED rows this snapshot summarizes — the same
# {path_filter} set, so the pre (hot-only) snapshot versions on the hot rows and
# the post one on the reconciled stamp, which is strictly greater. Computed
# server-side for the same reason as reported_at (a Python datetime round-trip
# renders in the caller's local timezone). It disambiguates twins WITHIN a sort
# key; the pre/post pair sits on different reported_at values and is never
# collapsed, which `make restate` depends on. A pass that credited nothing has no
# processed_at to take a max of and falls back to max(processed_at) over ALL
# current rows — NOT to reported_at, which is measured to be SMALLER than a prior
# pass's version: reported_at is max(ingest_time) + offset, while a reconciled
# row's processed_at is max(ingest_time) + RECONCILE_DELTA_MS, so the pre pass's
# reported_at (offset 0) sits a second BELOW it. The all-rows max is a superset of
# every credited set, so it can never be lower than a version already written.
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
        a.revenue as revenue,
        a.processed_at as processed_at
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
    coalesce(c.site_visits, 0) / nullIf(s.exposures, 0) as site_visit_rate,
    if
    (
        (select count() from credited) = 0,
        (select max(processed_at) from attributed_conversions final),
        (select max(processed_at) from credited)
    ) as snapshot_version
from spend_by_campaign as s
left join conv_by_campaign as c
    on s.campaign_id = c.campaign_id
order by s.campaign_id
"""


# Phase 18a, per-key watermark. `rollup_dirty` says when each key's data last moved
# (a data-derived stamp the loader writes); `rollup_refreshed` says which version of a
# key the rollup was last computed against. A key needs recomputing when those differ.
#
# The comparison is PER KEY and never against a global maximum. The first cut compared
# every key's version to one scalar watermark (the max over the whole dirty table), so
# any key whose own timestamps lagged the highest key's sat permanently below it and
# was never refreshed again — measured at 321 of 340 keys on long_delay, and
# reproduced as a stale served rollup (review gate, Phase 18a).
#
# `!=` rather than `>`: "the rollup was computed against a different version of this
# key" is the condition that matters, and it does not depend on the two stamps being
# ordered. (The earlier justification — "a version can move DOWN on a re-seed" — was
# WRONG and is corrected here: `rollup_dirty` is a ReplacingMergeTree keyed on the
# version, so a lower version is discarded on read and `d.version` never moves down.
# The re-seed hazard is real and is closed elsewhere: `make replay-serving` truncates
# this bookkeeping alongside the serving tables, and a fresh stack is `make down`.)
#
# `isNull(...) or ...`: a LEFT JOIN with no match yields the epoch under ClickHouse's
# default `join_use_nulls = 0`, but if that setting is ever enabled the comparison
# would evaluate NULL and every never-refreshed key would silently drop out of the
# dirty set — the exact silent-wrong class this gate exists for. The predicate states
# the intent instead of relying on a default nothing here sets.
_DIRTY_KEYS = """
select
    d.campaign_id,
    toUnixTimestamp(d.hour) as hour_s,
    toUnixTimestamp64Milli(d.version) as version_ms
from rollup_dirty as d final
left join rollup_refreshed as r final
    on d.campaign_id = r.campaign_id and d.hour = r.hour
where isNull(r.version) or d.version != r.version
order by
    d.campaign_id,
    hour_s
"""

# Keys cross the process boundary as (String, UInt32 epoch seconds, Int64 epoch
# millis) — never as Python datetimes. Reading a DateTime back into Python renders it
# in the caller's local timezone and rebinding it would compare a shifted instant
# (the determinism break `_max_ingest` documents); an integer carries no timezone.
_DIRTY_FILTER = (
    "where (campaign_id, toUnixTimestamp(toStartOfHour(event_time))) "
    "in {keys:Array(Tuple(String, UInt32))}"
)

# Stamp exactly the versions the refresh computed against — never `now()`, never a max
# over other keys. Written AFTER the refresh insert: a crash in between leaves the old
# stamps, so the next pass recomputes the same keys (idempotent) rather than skipping
# them, which is the one failure the full-refresh oracle cannot see. Never a delete or
# a mutation.
_STAMP_REFRESHED = """
insert into rollup_refreshed
select
    t.1 as campaign_id,
    toDateTime(t.2, 'UTC') as hour,
    fromUnixTimestamp64Milli(t.3, 'UTC') as version
from
(
    select arrayJoin({keys:Array(Tuple(String, UInt32, Int64))}) as t
)
"""

# A whole-table rebuild covers every key the loader has recorded, so it stamps all of
# them. Used by `queries/rollup_bench.py` after its scratch-table oracle run, never by
# the pipeline (which only ever refreshes what moved).
_STAMP_ALL_REFRESHED = """
insert into rollup_refreshed
select
    campaign_id,
    hour,
    version
from rollup_dirty final
"""


def dirty_keys(client: Client) -> list[tuple[str, int, int]]:
    """(campaign_id, hour epoch-seconds, version epoch-millis) for every key whose data
    has moved since the rollup was last computed against it."""
    return [tuple(row) for row in client.query(_DIRTY_KEYS).result_rows]


def campaign_hourly_rows(client: Client, *, hot_only: bool = False) -> list[tuple]:
    """The rollup aggregate as a SELECT — the same expression `refresh_campaign_hourly`
    inserts, read without writing. `hot_only` restricts the credit join to `path =
    'hot'`, i.e. the rollup as it stood BEFORE reconciliation (the hot-attributed set
    is invariant under reconciliation, the same seam the PRE report snapshot uses). Two
    calls therefore give the pre- and post-reconciliation FULL rebuilds the dirty-set
    gate compares (`queries/rollup_bench.py`)."""
    sql = _REFRESH_CAMPAIGN_HOURLY.replace("insert into campaign_hourly", "", 1)
    sql = sql.replace("/*dirty_exposures*/", "")
    sql = sql.replace("/*path_filter*/", "and a.path = 'hot'" if hot_only else "")
    # reported_at is dropped: it is the pass stamp, not part of the aggregate being
    # compared (and it differs between a pre and a post rebuild by construction).
    rows = client.query(sql).result_rows
    return [r[:-1] for r in rows]


def refresh_campaign_hourly(client: Client) -> int:
    """Recompute the rollup for exactly the keys whose data has moved since the rollup
    was last computed against them, then stamp those keys in `rollup_refreshed`.
    Returns the number of keys recomputed.

    No caller-supplied version: each row carries `max(stamp)` over the source rows it
    summarizes, so identical content always carries an identical version and the
    ReplacingMergeTree resolves every re-computation for free — including a replay's.
    No `full` branch either: a whole-table rebuild is a measurement tool
    (`queries/rollup_bench.py` runs `refresh_sql(full=True)` into a scratch table),
    not a pipeline path, and the live gate proves the incremental result equals it."""
    keys = dirty_keys(client)
    if not keys:
        return 0
    client.command(
        refresh_sql(full=False),
        parameters={"keys": [(c, h) for c, h, _ in keys]},
    )
    client.command(_STAMP_REFRESHED, parameters={"keys": keys})
    return len(keys)


def refresh_sql(*, full: bool) -> str:
    """The refresh statement, full or dirty-filtered. Public so `make rollup-bench`
    measures the SAME SQL the pipeline runs, never a copy of it."""
    return _REFRESH_CAMPAIGN_HOURLY.replace(
        "/*dirty_exposures*/", "" if full else _DIRTY_FILTER
    ).replace("/*path_filter*/", "")


def write_report_snapshot(client: Client, offset_ms: int, *, hot_only: bool) -> None:
    """Snapshot the four metrics per campaign at `reported_at = max(ingest_time) +
    offset_ms`. `hot_only` filters to path='hot' (the pre-reconciliation report —
    invariant under reconciliation); otherwise both paths (the post report)."""
    path_filter = "and a.path = 'hot'" if hot_only else ""
    # Plain replace, not str.format — the SQL keeps ClickHouse's own {name:Type}
    # server-side bindings, which str.format would try to interpolate.
    sql = _WRITE_REPORT_SNAPSHOT.replace("/*path_filter*/", path_filter)
    client.command(sql, parameters={"offset_ms": offset_ms, "period": PERIOD})
