"""Read exposures back from the Iceberg lake via DuckDB (Phase 12).

DuckDB's `iceberg_scan` reads an Iceberg table directly from its current metadata
file — the same table pyiceberg wrote. Since Phase 17 the lake is the record: this
is THE exposure read of the reconcile pass (`recover_day`, one call per (day,
bucket)) and of the ClickHouse loader (`read_exposures_for_days`), feeding the
UNCHANGED pure `last_touch` leaf (Phase 16 name; `attribute_household` until then).

Two properties make this byte-identical to the ClickHouse-sourced read:
  * dedup-on-read — `select distinct` collapses the append-accumulated re-sends to
    one row per exposure (rows are byte-identical replays), reproducing the
    exposures_landed ReplacingMergeTree FINAL collapse (invariant: exposure_id is
    unique — DECISIONS Phase 12).
  * naive-UTC-ms round-trip — clickhouse-connect hands the DateTime64(3,'UTC')
    column back as a NAIVE datetime holding the UTC wall-clock (tzinfo=None). We
    match that exactly: `SET TimeZone='UTC'` renders the timestamptz at the correct
    UTC wall-clock (the §8 defense — a default local session would shift it), then
    we drop tzinfo so the matcher compares like-with-like and the recovered rows
    are identical to the ClickHouse-sourced pass.

The optional `min_event_time` / `max_event_time` bounds prune whole
day-partitions: `recover_day` passes `[day − long window, day + 1d)`, so every
pruned exposure is outside every candidate's window and the leaf would discard it
anyway — the prune is output-invariant, not a filter that could drop a real match.
"""

from collections import defaultdict
from datetime import datetime

import duckdb

from lake.iceberg_catalog import ensure_exposures, metadata_path
from lake.read_attributed import day_ranges_predicate
from producer.models import Exposure

_SELECT_COLS = (
    "exposure_id, event_time, ingest_time, campaign_id, household_id, "
    "ip, app_id, program_genre, spend"
)


def read_exposures_for_days(days: list[str]) -> list[Exposure]:
    """Every distinct exposure whose event_time falls on one of `days`
    (YYYY-MM-DD) — the loader's per-day-partition read (Phase 17). Dedup-on-read
    as above, ordered by the exposures_landed sort key for a stable insert."""
    if not days:
        return []

    con = duckdb.connect()
    con.execute("load iceberg")
    con.execute("set timezone='UTC'")
    params: list[object] = [metadata_path(ensure_exposures())]
    where = day_ranges_predicate(sorted(days), params)  # prunable ranges, not strftime
    rows = con.execute(
        f"select distinct {_SELECT_COLS} from iceberg_scan(?) where {where} "
        "order by campaign_id, event_time, exposure_id",
        params,
    ).fetchall()
    return [_exposure(r) for r in rows]


def _exposure(r: tuple) -> Exposure:
    return Exposure(
        exposure_id=r[0],
        # Drop tzinfo → naive UTC, matching clickhouse-connect (the UTC
        # wall-clock is already correct via SET TimeZone='UTC').
        event_time=r[1].replace(tzinfo=None),
        ingest_time=r[2].replace(tzinfo=None),
        campaign_id=r[3],
        household_id=r[4],
        ip=r[5],
        app_id=r[6],
        program_genre=r[7],
        spend=r[8],
    )


def read_exposures_by_household(
    household_ids: set[str],
    min_event_time: datetime | None = None,
    max_event_time: datetime | None = None,
) -> dict[str, list[Exposure]]:
    """Bulk-load the given households' exposures from the lake in one scan,
    deduped on exposure_id, grouped by household (the Phase-6 ClickHouse read's
    shape, `dict[household_id -> list[Exposure]]`). `min_event_time` /
    `max_event_time` (optional, half-open) day-partition-prune exposures provably
    outside every candidate's window; with the households all in ONE bucket this
    is the bucket-aligned partitioned read of the Phase-17 reconcile (DuckDB
    prunes on both transforms — ARCHITECTURE §8)."""
    if not household_ids:
        return {}
    con = duckdb.connect()
    # LOAD only — never install. The extension is installed once at setup
    # (`make setup` / CI run `python -m lake.install_extension`, network allowed
    # there); loading here reaches no network, so the offline unit suite stays
    # offline and fails loud if setup was skipped (DECISIONS Phase 12).
    con.execute("load iceberg")
    # Force UTC rendering: DuckDB returns a TIMESTAMPTZ in the session timezone, so
    # a default (local) session would hand back the same instant with a local-tz
    # tzinfo — the ARCHITECTURE §8 gotcha (clickhouse-connect had the mirror bug).
    # Pinning UTC makes the datetime tz-aware UTC, matching DateTime64(3,'UTC').
    con.execute("set timezone='UTC'")
    predicates = ["household_id = any(?)"]
    params: list[object] = [sorted(household_ids)]
    if min_event_time is not None:
        predicates.append("event_time >= ?")
        params.append(min_event_time)
    if max_event_time is not None:
        predicates.append("event_time < ?")
        params.append(max_event_time)
    where = " and ".join(predicates)
    rows = con.execute(
        f"select distinct {_SELECT_COLS} "
        f"from iceberg_scan(?) where {where} order by exposure_id",
        [metadata_path(ensure_exposures()), *params],
    ).fetchall()
    by_household: dict[str, list[Exposure]] = defaultdict(list)
    for r in rows:
        by_household[r[4]].append(_exposure(r))
    return by_household
