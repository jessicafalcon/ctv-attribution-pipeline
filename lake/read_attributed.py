"""Read attributed rows back from the lake via DuckDB (Phase 17).

The lake is an append-only log (spec D4): a conversion_id may have a hot row and
a later reconciled row, and exact re-lands of the same row. `read_current` gives
the CURRENT row per conversion_id — the highest `processed_at`, i.e. what the
ClickHouse ReplacingMergeTree(processed_at) FINAL would keep — computed in SQL
(`distinct` then `row_number() over (partition by conversion_id order by
processed_at desc, path desc) = 1`, the argMax form; `path` breaks a tie on an
equal `processed_at` — 'reconciled' > 'hot' — so the pick is a total order, never
the scan's whim). Never assume one row per key.

Same naive-UTC-ms round-trip as lake.read_exposures: `set timezone='UTC'`, then
drop tzinfo, so a row read back compares equal to the clickhouse-connect
representation and to the engine's in-memory row.
"""

from datetime import UTC, datetime, timedelta

import duckdb

from lake.iceberg_catalog import ensure_attributed, metadata_path
from producer.models import AttributedConversion

_COLS = list(AttributedConversion.model_fields)
_SELECT = ", ".join(_COLS)


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("load iceberg")  # LOAD only — installed once at setup (Phase 12)
    con.execute("set timezone='UTC'")
    return con


def _row(r: tuple) -> AttributedConversion:
    d = dict(zip(_COLS, r, strict=True))
    for k in ("event_time", "ingest_time", "processed_at"):
        d[k] = d[k].replace(tzinfo=None)
    d["assists"] = list(d["assists"])
    d["candidate_households"] = list(d["candidate_households"])
    return AttributedConversion(**d)


def day_ranges_predicate(days: list[str], params: list[object]) -> str:
    """`(event_time >= ? and event_time < ?) or (...)` for each day — partition-
    prunable; appends the bound parameters to `params`. No days → `false`."""
    if not days:
        return "false"
    clauses = []
    for day in days:
        start = datetime.fromisoformat(day).replace(tzinfo=UTC)
        clauses.append("(event_time >= ? and event_time < ?)")
        params += [start, start + timedelta(days=1)]
    return "(" + " or ".join(clauses) + ")"


def read_current(days: list[str] | None = None) -> list[AttributedConversion]:
    """Current row per conversion_id, ordered by conversion_id. `days`
    (YYYY-MM-DD event_time days) prunes day partitions — an output-invariant
    filter on the conversion's own event_time. The day
    filter is written as raw `event_time` RANGES (`>= d and < d+1`, one per day),
    the form DuckDB's iceberg_scan can push down to the `day(event_time)`
    partition — an expression over the column (`strftime(...) = ...`) would filter
    correctly but scan every file (review gate; the FINAL `read_rows` lesson)."""
    predicates: list[str] = []
    params: list[object] = [metadata_path(ensure_attributed())]
    if days is not None:
        predicates.append(day_ranges_predicate(sorted(days), params))
    where = ("where " + " and ".join(predicates)) if predicates else ""
    rows = (
        _connect()
        .execute(
            f"select {_SELECT} from ("
            f"select distinct {_SELECT} from iceberg_scan(?) {where}) "
            "qualify row_number() over ("
            "partition by conversion_id order by processed_at desc, path desc) = 1 "
            "order by conversion_id",
            params,
        )
        .fetchall()
    )
    return [_row(r) for r in rows]
