"""Headless runner for `make reconcile-dagster` (Phase 12).

Materializes the reconciliation asset graph WITHOUT the webserver: observe the
lake, recover each candidate day (a backfill — or a single day with --partition),
then finalize once. Uses an ephemeral Dagster instance so no DAGSTER_HOME is
needed. The pipeline output is identical to `make run`'s reconcile pass; Dagster
only orchestrates (DECISIONS Phase 12).
"""

import argparse

from clickhouse_connect.driver.client import Client
from dagster import DagsterInstance, materialize

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from orchestration.assets import (
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)
from reconcile.reconcile import _read_candidates


def _candidate_days(client: Client) -> list[str]:
    """The distinct conversion-event-time days that hold hot-miss candidates —
    the only partitions worth materializing."""
    return sorted({c.event_time.date().isoformat() for c in _read_candidates(client)})


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Orchestrated reconciliation")
    parser.add_argument("--profile", default="long_delay", help="informational")
    parser.add_argument(
        "--partition", default=None, help="materialize only this day (YYYY-MM-DD)"
    )
    args = parser.parse_args(argv)

    apply_ddl()
    client = connect()
    instance = DagsterInstance.ephemeral()

    assert materialize([exposures_iceberg], instance=instance).success

    days = [args.partition] if args.partition else _candidate_days(client)
    for day in days:
        result = materialize(
            [reconciled_conversions],
            partition_key=day,
            instance=instance,
        )
        assert result.success
        print(f"reconciled_conversions[{day}] materialized")

    assert materialize([reconciled_report], instance=instance).success
    print(f"reconcile-dagster: {len(days)} day-partition(s) recovered + finalize")


if __name__ == "__main__":
    main()
