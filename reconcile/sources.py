"""Exposure sources for the reconciliation matcher (Phase 12).

The long-window matcher needs, per candidate household, that household's exposures
to re-run the last-touch leaf. Phase 6 read them from ClickHouse `exposures_landed
final`; Phase 12 adds a second source that reads the SAME rows from the Iceberg
lake via DuckDB. Both return an identical `dict[household_id -> list[Exposure]]`,
so the matcher — and its byte-identical output — is source-agnostic. Only the
exposure *source* swaps; candidates, snapshots, rollup, and the insert stay on
ClickHouse (spec pinned decision).
"""

from collections import defaultdict
from datetime import timedelta
from typing import Protocol

from clickhouse_connect.driver.client import Client

from producer.models import Exposure, ResolvedConversion

_EXPOSURE_COLS = (
    "exposure_id, event_time, ingest_time, campaign_id, household_id, ip, "
    "app_id, program_genre, spend"
)


class ExposureSource(Protocol):
    """Read the candidate households' exposures, grouped by household. `window`
    is the long window — a source may use it to prune provably-out-of-window rows
    (output-invariant, since the leaf filters to the window anyway) but must never
    drop an in-window exposure."""

    def read_for(
        self, candidates: list[ResolvedConversion], window: timedelta
    ) -> dict[str, list[Exposure]]: ...


class ClickHouseExposureSource:
    """Phase-6 read, unchanged: one `exposures_landed final` query per pass,
    grouped in memory. `make run` uses this by default, so its output is
    byte-identical to before Phase 12. Ignores `window` (the leaf window-filters);
    loads all of each candidate household's exposures."""

    def __init__(self, client: Client):
        self._client = client

    def read_for(
        self, candidates: list[ResolvedConversion], window: timedelta
    ) -> dict[str, list[Exposure]]:
        household_ids = {c.household_id for c in candidates}
        if not household_ids:
            return {}
        rows = self._client.query(
            f"select {_EXPOSURE_COLS} from exposures_landed final "
            "where household_id in {hhs:Array(String)} order by exposure_id",
            parameters={"hhs": sorted(household_ids)},
        ).result_rows
        by_household: dict[str, list[Exposure]] = defaultdict(list)
        for r in rows:
            by_household[r[4]].append(
                Exposure(
                    exposure_id=r[0],
                    event_time=r[1],
                    ingest_time=r[2],
                    campaign_id=r[3],
                    household_id=r[4],
                    ip=r[5],
                    app_id=r[6],
                    program_genre=r[7],
                    spend=r[8],
                )
            )
        return by_household


class IcebergExposureSource:
    """Read the same exposures from the Iceberg lake via DuckDB. Prunes day-
    partitions older than (earliest candidate event_time − window): every pruned
    row is older than every candidate's window, so the leaf would discard it
    anyway — the output is byte-identical to the ClickHouse source (Done-when #2).
    Dedup-on-exposure_id inside the read reproduces the exposures_landed FINAL
    collapse (invariant: exposure_id unique — DECISIONS Phase 12)."""

    def read_for(
        self, candidates: list[ResolvedConversion], window: timedelta
    ) -> dict[str, list[Exposure]]:
        # Imported lazily so ClickHouse-only paths (make run, CI) never import the
        # lake stack.
        from lake.read_exposures import read_exposures_by_household

        if not candidates:
            return {}
        household_ids = {c.household_id for c in candidates}
        min_event_time = min(c.event_time for c in candidates) - window
        return read_exposures_by_household(household_ids, min_event_time=min_event_time)
