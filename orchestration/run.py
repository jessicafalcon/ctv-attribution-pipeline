"""The ONE headless orchestration CLI (Phase 17 review gate — four near-identical
runners became one): `python -m orchestration.run <load|reconcile|replay|maintain>
--profile <p>`. `--profile` is required on every subcommand and binds the lake of
record (`lake.iceberg_catalog.configure` → data/lake/<profile>); nothing here has
a default root.

Every Dagster materialization goes through `_materialize` on an ephemeral
instance (nothing persists, no DAGSTER_HOME, no webserver). The pipeline output
is identical to the in-process paths — Dagster only orchestrates (DECISIONS
Phase 12): `load` is what the engine and the reconcile job call after landing,
`reconcile` is `make run`'s pass one day-partition at a time, `replay` rebuilds
the serving tables from the lake, `maintain` runs the `lake_maintenance` job.
"""

import argparse
import sys
from collections.abc import Sequence

from dagster import (
    AssetsDefinition,
    DagsterInstance,
    ExecuteInProcessResult,
    materialize,
)

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from lake.iceberg_catalog import configure
from orchestration.assets import (
    attributed_iceberg,
    clickhouse_attributed_conversions,
    clickhouse_exposures_landed,
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)
from orchestration.maintenance import lake_maintenance
from orchestration.replay import EmptyLakeError, replay
from reconcile.reconcile import candidate_days

_LOAD_ASSETS = (
    ("exposures", clickhouse_exposures_landed),
    ("attributed", clickhouse_attributed_conversions),
)


def _materialize(
    assets: Sequence[AssetsDefinition],
    instance: DagsterInstance,
    partition_key: str | None = None,
) -> ExecuteInProcessResult:
    """Materialize `assets` (one partition if given) and fail loud — a Dagster
    step failure is a pipeline failure, never a green exit code."""
    result = materialize(list(assets), partition_key=partition_key, instance=instance)
    if not result.success:
        names = ", ".join(a.key.to_user_string() for a in assets)
        suffix = f"[{partition_key}]" if partition_key else ""
        raise RuntimeError(f"materialization failed: {names}{suffix}")
    return result


def _rows(result: ExecuteInProcessResult, key: str) -> int:
    (event,) = result.get_asset_materialization_events()
    return int(event.materialization.metadata[key].value)


def materialize_load(days: set[str]) -> dict[str, int]:
    """Load exactly `days` (YYYY-MM-DD event_time days — the days a landing
    TOUCHED, spec D6) from the lake into ClickHouse; return rows inserted per
    table. Idempotent per day (the ReplacingMergeTree collapses re-inserts).
    Called in-process by the engine and the reconcile job after they land."""
    totals = {"exposures": 0, "attributed": 0}
    if not days:
        return totals
    instance = DagsterInstance.ephemeral()
    _materialize([exposures_iceberg, attributed_iceberg], instance)
    for day in sorted(days):
        for key, asset_def in _LOAD_ASSETS:
            totals[key] += _rows(_materialize([asset_def], instance, day), "rows")
    return totals


def run_reconcile(partition: str | None) -> None:
    """Observe the lake, recover each candidate day (a backfill — or one day with
    `partition`) appending corrections to the lake, reload the touched days, then
    finalize once. Byte-identical to `make run`'s pass."""
    apply_ddl()
    connect()  # fail early if the serving layer is down
    instance = DagsterInstance.ephemeral()
    _materialize([exposures_iceberg, attributed_iceberg], instance)
    days = [partition] if partition else candidate_days()
    touched: set[str] = set()
    for day in days:
        result = _materialize([reconciled_conversions], instance, day)
        (event,) = result.get_asset_materialization_events()
        touched |= set(event.materialization.metadata["touched_days"].value)
        print(f"reconciled_conversions[{day}] materialized")
    loaded = materialize_load(touched)
    _materialize([reconciled_report], instance)
    print(
        f"reconcile-dagster: {len(days)} day-partition(s) recovered, "
        f"{loaded['attributed']} corrected rows reloaded over {len(touched)} "
        "day(s) + finalize"
    )


def run_maintain() -> None:
    result = lake_maintenance.execute_in_process()
    if not result.success:
        raise RuntimeError("lake_maintenance job failed")
    for name in ("maintain_exposures", "maintain_attributed"):
        print(f"lake-maintain {name}: {result.output_for_node(name)}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="headless orchestration")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_ in (
        ("load", "load the given event_time days from the lake into ClickHouse"),
        ("reconcile", "orchestrated reconciliation (make reconcile-dagster)"),
        ("replay", "truncate + reload the serving tables from the lake (no broker)"),
        ("maintain", "lake hygiene job: compact day partitions, expire snapshots"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument(
            "--profile",
            required=True,
            help="binds the lake of record: data/lake/<profile>",
        )
    sub.choices["load"].add_argument("--days", nargs="+", required=True)
    sub.choices["reconcile"].add_argument(
        "--partition", default=None, help="materialize only this day (YYYY-MM-DD)"
    )
    sub.choices["replay"].add_argument(
        "--confirm", action="store_true", help="skip the prompt"
    )
    args = parser.parse_args(argv)
    configure(args.profile)
    if args.command == "load":
        loaded = materialize_load(set(args.days))
        print(f"load: {loaded}")
    elif args.command == "reconcile":
        run_reconcile(args.partition)
    elif args.command == "replay":
        try:
            loaded = replay(args.confirm)
        except EmptyLakeError as e:
            sys.exit(str(e))
        print(
            f"replay-serving: {loaded['exposures']} exposures, "
            f"{loaded['attributed']} attributed rows reloaded from the lake (no broker)"
        )
    else:
        run_maintain()


if __name__ == "__main__":
    main()
