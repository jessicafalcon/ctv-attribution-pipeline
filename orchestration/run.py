"""Headless runner for `make reconcile-dagster` (Phase 12; Phase 17 lake-of-record).

Materializes the reconciliation asset graph WITHOUT the webserver: observe the
lake, recover each candidate day (a backfill — or a single day with --partition)
appending the corrections to the lake, reload the touched days into ClickHouse,
then finalize once. Uses an ephemeral Dagster instance so no DAGSTER_HOME is
needed. The pipeline output is identical to `make run`'s reconcile pass; Dagster
only orchestrates (DECISIONS Phase 12).
"""

import argparse

from dagster import DagsterInstance, materialize

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from lake.iceberg_catalog import configure
from orchestration.assets import (
    attributed_iceberg,
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)
from orchestration.load import materialize_load
from reconcile.reconcile import candidate_days


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Orchestrated reconciliation")
    parser.add_argument(
        "--profile", required=True, help="binds the lake of record: data/lake/<profile>"
    )
    parser.add_argument(
        "--partition", default=None, help="materialize only this day (YYYY-MM-DD)"
    )
    args = parser.parse_args(argv)

    configure(args.profile)
    apply_ddl()
    connect()  # fail early if the serving layer is down
    instance = DagsterInstance.ephemeral()

    observed = materialize([exposures_iceberg, attributed_iceberg], instance=instance)

    if not observed.success:
        raise RuntimeError("lake observe assets failed")

    days = [args.partition] if args.partition else candidate_days()
    touched: set[str] = set()
    for day in days:
        result = materialize(
            [reconciled_conversions],
            partition_key=day,
            instance=instance,
        )
        if not result.success:
            raise RuntimeError(f"reconciled_conversions[{day}] failed")
        (event,) = result.get_asset_materialization_events()
        touched |= set(event.materialization.metadata["touched_days"].value)
        print(f"reconciled_conversions[{day}] materialized")

    loaded = materialize_load(touched)
    if not materialize([reconciled_report], instance=instance).success:
        raise RuntimeError("reconciled_report failed")
    print(
        f"reconcile-dagster: {len(days)} day-partition(s) recovered, "
        f"{loaded['attributed']} corrected rows reloaded over {len(touched)} "
        "day(s) + finalize"
    )


if __name__ == "__main__":
    main()
