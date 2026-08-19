"""ClickHouse read helpers for the collectors — the only I/O in the collector
path. Each is fixed, parameter-free SQL over the serving tables (N1: the causal
side file is never read, never in the DB); the pure aggregation lives in
`agent/collect.py`, so these can be swapped for fakes in unit tests. Campaign
metrics and restatements reuse the existing report.sql / restatement.sql so the
context can never drift from `make report` / `make restate`.

Every read is FINAL on both ReplacingMergeTree tables (DECISIONS Phase 4) — a
plain scan of unmerged parts would count superseded and duplicate rows."""

from clickhouse_connect.driver.client import Client

# Attributed share of conversions per calendar day (conversion event_time). The
# aggregate aliases must NOT shadow the `attributed` column (they are referenced by
# the countIf condition and, in IP_CLUSTER_OVERALL, the WHERE — ClickHouse raises
# ILLEGAL_AGGREGATION on a name clash). collect.py unpacks positionally.
MATCH_RATE_BY_DAY = """
select
    toDate(event_time) as day,
    count() as processed,
    countIf(attributed = 1) as attributed_count
from attributed_conversions final
group by day
order by day
"""

# Attribution lag in seconds for each attributed HOT row: conversion event_time
# minus its credited exposure's event_time. Hot only — reconciled rows span the
# 90d long window, a different edge from the 7d hot boundary this measures.
WINDOW_EDGE_LAGS = """
select
    dateDiff('second', e.event_time, a.event_time) as lag_seconds
from attributed_conversions as a final
inner join exposures_landed as e final
    on a.exposure_id = e.exposure_id
where a.attributed = 1
    and a.path = 'hot'
    and a.event_time >= e.event_time
"""

# Overall attributed counts + the two shared-IP shares (resolution / ambiguity).
IP_CLUSTER_OVERALL = """
select
    count() as attributed_count,
    countIf(resolution = 'ip') as ip_resolved,
    countIf(ambiguous = 1) as ambiguous_count,
    max(candidate_count) as max_candidate_count
from attributed_conversions final
where attributed = 1
"""

# Top shared IPs by attributed-conversion count (IP-resolved rows only — these are
# the shared-IP fan-outs that can land on the wrong household).
IP_CLUSTER_TOP = """
select
    ip,
    count() as attributed_conversions,
    max(candidate_count) as max_candidate_count
from attributed_conversions final
where attributed = 1
    and resolution = 'ip'
group by ip
order by attributed_conversions desc, ip
limit 10
"""

# Raw exposures per genre (denominator for genre reach).
GENRE_EXPOSURES = """
select
    program_genre as genre,
    count() as exposures
from exposures_landed final
group by genre
order by genre
"""

# Attributed conversions credited to an exposure of each genre (numerator).
GENRE_CONVERSIONS = """
select
    e.program_genre as genre,
    count() as attributed_conversions
from attributed_conversions as a final
inner join exposures_landed as e final
    on a.exposure_id = e.exposure_id
where a.attributed = 1
group by genre
order by genre
"""


def query(client: Client, sql: str) -> list[tuple]:
    return client.query(sql).result_rows
