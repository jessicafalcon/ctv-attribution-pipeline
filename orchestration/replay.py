"""`make replay-serving` = `python -m lake.destructive replay` (Phase 17, Done-when
5): rebuild the ClickHouse serving tables FROM THE LAKE with no Kafka involvement.
Library code — the validate/confirm/act sequence lives in `lake.destructive`.

Truncates `exposures_landed` and `attributed_conversions`, then materializes the
two load assets for EVERY day the lake holds — the days
come from the data (distinct event_time days in both raw tables), never from the
wall clock — so the current row per conversion_id (hot, or its later reconciled
version) and every distinct exposure are back. `make eval` afterwards reproduces
the pins. The rollup/snapshot tables are derived state and are not touched here;
a reconcile pass (`make run`'s second step / `make reconcile-dagster`) refreshes
them.
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

SERVING_TABLES = ("exposures_landed", "attributed_conversions")


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


class EmptyLakeError(RuntimeError):
    """Refuse to TRUNCATE the serving tables when the lake holds nothing to
    reload — that would be data loss with a green exit code (review gate). A
    normal exception (not SystemExit) so library callers can catch it; the
    destructive CLI turns it into the exit code."""


def truncate_and_reload(days: set[str]) -> dict[str, int]:
    """The act: TRUNCATE both serving tables, reload `days`. Callers (the
    destructive CLI) have already refused an empty lake and confirmed."""
    if not days:
        raise EmptyLakeError("replay-serving: no days to reload")
    apply_ddl()
    client = connect()
    for table in SERVING_TABLES:
        client.command(f"truncate table {table}")
    from orchestration.run import materialize_load  # the one CLI owns the loader

    return materialize_load(days)
