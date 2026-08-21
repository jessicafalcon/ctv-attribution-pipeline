"""Headless lake → ClickHouse load (Phase 17): materialize the two load assets for
exactly the day partitions a landing touched (spec D6). Called in-process by the
engine and the reconcile job after they land, and by `make replay-serving`.

Ephemeral Dagster instance (nothing persists, no DAGSTER_HOME); one materialize
per (asset, day) — laptop-scale. The load is idempotent per day (ReplacingMergeTree
collapses re-inserts), so re-running for the same days converges.
"""

from dagster import DagsterInstance, materialize

from orchestration.assets import (
    attributed_iceberg,
    clickhouse_attributed_conversions,
    clickhouse_exposures_landed,
    exposures_iceberg,
)


def materialize_load(days: set[str]) -> dict[str, int]:
    """Load `days` (YYYY-MM-DD event_time days) from the lake into ClickHouse;
    return the row counts inserted per table."""
    instance = DagsterInstance.ephemeral()
    totals = {"exposures": 0, "attributed": 0}
    if not days:
        return totals
    observed = materialize([exposures_iceberg, attributed_iceberg], instance=instance)
    if not observed.success:
        raise RuntimeError("lake observe assets failed")
    for day in sorted(days):
        for key, asset_def in (
            ("exposures", clickhouse_exposures_landed),
            ("attributed", clickhouse_attributed_conversions),
        ):
            result = materialize([asset_def], partition_key=day, instance=instance)
            if not result.success:
                raise RuntimeError(f"{asset_def.key.to_user_string()}[{day}] failed")
            (event,) = result.get_asset_materialization_events()
            totals[key] += int(event.materialization.metadata["rows"].value)
    return totals
