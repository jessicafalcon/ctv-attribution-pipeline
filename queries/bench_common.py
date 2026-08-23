"""Shared measurement harness for the `queries/` benchmarks (Phase 18a).

Public since three benchmarks need it — `bench.py` (naive vs rollup),
`measure_levers.py` (the Phase-13 cost levers) and `rollup_bench.py` (full vs
incremental refresh). It was `bench.py`'s private `_canonicalize` / `_measure` /
`_round_row`, imported across modules by their underscore names (BACKLOG: a
private-symbol import across modules is the signal they should be public).

The two things it exists to keep single-sourced:
  canonicalize — OPTIMIZE ... FINAL every read table before measuring, so
                 read_rows is the merged steady state and not un-merged
                 part-bloat (RUNBOOK incident #1, the determinism fix).
  measure      — median wall-clock over RUNS with the query cache forced off,
                 plus the server's own read_rows / read_bytes.
"""

import time

from clickhouse_connect.driver.client import Client

RUNS = 5  # median wall-clock over this many runs (rows/bytes are deterministic)
ROUND = 6  # decimals for the metric-equality gate

# Every ReplacingMergeTree the benchmarks read. Canonicalized to merged steady state
# before measuring, so read_rows reflects logical table size on every side (not
# un-merged part-bloat) — the only apples-to-apples comparison, and the only
# deterministic one.
READ_TABLES = (
    "attributed_conversions",
    "exposures_landed",
    "campaign_hourly",
    # Phase 18a: the incremental refresh reads the dirty-set bookkeeping too, so it is
    # canonicalized like every other measured table — an un-merged rollup_dirty was
    # counted as read-side cost and made the measurement drift between runs.
    "rollup_dirty",
    "rollup_refreshed",
)


def canonicalize(client: Client) -> None:
    """Collapse each read table to its merged steady state before measuring.

    A versioned-replace ReplacingMergeTree keeps every superseded version as physical
    rows in un-merged parts until a background merge fires; a FINAL scan physically
    reads all of them, so read_rows counts part-bloat that grows per refresh /
    correction and drifts with merge timing. `campaign_hourly` gains a full copy per
    rollup refresh; `attributed_conversions` gains a row per reconciled conversion_id
    (a higher-processed_at version of the hot row). `OPTIMIZE ... FINAL` forces the
    merge — synchronous on single-node (alter_sync=1), a no-op if already merged
    (optimize_throw_if_noop=0) — so read_rows measures the steady state a scheduled
    rollup serves in production: deterministic and identical on re-run. Both settings
    are relied-upon ClickHouse DEFAULTS (not overridden in `clickhouse/`): overriding
    alter_sync or optimize_throw_if_noop would silently reintroduce the very non-
    determinism this collapses (a re-run could throw, or measure before the merge)."""
    for table in READ_TABLES:
        client.command(f"optimize table {table} final")


def round_row(row: tuple) -> tuple:
    """Round floats to ROUND decimals for equality; leave ints/str/None as-is."""
    return tuple(round(v, ROUND) if isinstance(v, float) else v for v in row)


def measure(client: Client, sql: str, settings: dict | None = None) -> dict:
    """Run `sql` RUNS times (query cache off), return median latency plus the
    server's read_rows/read_bytes and the result rows (deterministic across runs).

    `settings` adds ClickHouse query settings (e.g. optimize_use_projections=0)
    for the cost-lever before/after pairs (queries/measure_levers.py); the query
    cache is always forced off and cannot be overridden here."""
    latencies = []
    result = None
    q_settings = {**(settings or {}), "use_query_cache": 0}
    for _ in range(RUNS):
        start = time.perf_counter()
        result = client.query(sql, settings=q_settings)
        latencies.append((time.perf_counter() - start) * 1000.0)
    summary = result.summary
    return {
        "latency_ms": sorted(latencies)[len(latencies) // 2],
        "read_rows": int(summary.get("read_rows", 0)),
        "read_bytes": int(summary.get("read_bytes", 0)),
        "rows": result.result_rows,
    }
