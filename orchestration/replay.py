"""`make replay-serving` = `python -m lake.destructive replay` (Phase 17, Done-when
5): rebuild the ClickHouse serving tables FROM THE LAKE with no Kafka involvement.
Library code — the validate/confirm/act sequence lives in `lake.destructive`.

Truncates the six derived tables (SERVING_TABLES: `exposures_landed`,
`attributed_conversions`, `eval_meta` — a stale marker over a half-loaded DB is what
the guard refuses — plus `campaign_hourly` and the `rollup_dirty` /
`rollup_refreshed` bookkeeping, Phase 18a), then materializes the two load assets for
EVERY day the lake holds — the days come from the data (distinct event_time days in
both raw tables), never from the wall clock — so the current row per conversion_id
(hot, or its later reconciled version) and every distinct exposure are back, and the
loader rebuilds the rollup as it reloads. `make eval` afterwards reproduces the pins.
`report_snapshots` is NOT truncated: it is the restatement history, the one derived
table a reload cannot reconstruct (BACKLOG).
"""

import duckdb

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from lake.iceberg_catalog import (
    catalog_exists,
    ensure_attributed,
    ensure_exposures,
    metadata_path,
)

# eval_meta is truncated too: a stale marker over a half-loaded DB is exactly what
# the BACKLOG-43 guard exists to refuse; the replay re-stamps it after the load.
#
# Since Phase 18a the ROLLUP and its bookkeeping go with them (review-gate round 3).
# All three are derived state the reload reproduces: `campaign_hourly` is recomputed
# from the reloaded rows, and `rollup_dirty` / `rollup_refreshed` describe a state of
# the serving tables that no longer exists once they are truncated. Keeping them was
# the re-seed hazard: bookkeeping from the PREVIOUS data marked keys clean, so the
# refresh after a replay skipped them and the old rollup was served on top of new
# rows — invisible to every gate, because the rows themselves were right.
SERVING_TABLES = (
    "exposures_landed",
    "attributed_conversions",
    "eval_meta",
    "campaign_hourly",
    "rollup_dirty",
    "rollup_refreshed",
)


def lake_days() -> set[str]:
    """Every event_time day (YYYY-MM-DD) present in either raw table. Empty when
    this root holds no catalog at all — checked BEFORE `ensure_*`, so asking
    never creates an empty lake as a side effect (review gate)."""
    if not catalog_exists():
        return set()
    con = duckdb.connect()
    con.execute("load iceberg")
    con.execute("set timezone='UTC'")
    days: set[str] = set()
    for table in (ensure_exposures(), ensure_attributed()):
        rows = con.execute(
            "select distinct strftime(event_time, '%Y-%m-%d') from iceberg_scan(?)",
            [metadata_path(table)],
        ).fetchall()
        days |= {r[0] for r in rows}
    return days


class EmptyLakeError(ValueError):
    """Refuse to TRUNCATE the serving tables when the lake holds nothing to
    reload — that would be data loss with a green exit code (review gate). A
    normal exception (not SystemExit) so library callers can catch it; the
    destructive CLI turns it into the exit code."""


def truncate_and_reload(days: set[str]) -> dict[str, int]:
    """The act: TRUNCATE the six derived tables (SERVING_TABLES — the three loaded
    ones plus the rollup and its dirty-set bookkeeping), reload `days`. Callers (the
    destructive CLI) have already refused an empty or out-of-calendar lake and
    confirmed."""
    if not days:
        raise EmptyLakeError("replay-serving: no days to reload")
    apply_ddl()
    client = connect()
    for table in SERVING_TABLES:
        client.command(f"truncate table {table}")
    # Lazy for the same reason as the engine and the reconcile job: the loader
    # pulls in the Dagster stack, which the offline suites must not pay for. (No
    # import cycle — orchestration.run does not import this module.)
    from orchestration.run import materialize_load

    return materialize_load(days)
