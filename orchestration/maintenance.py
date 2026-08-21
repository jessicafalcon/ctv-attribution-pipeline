"""Dagster job `lake_maintenance` (Phase 17, spec D10) + its headless runner
(`make lake-maintain`). Two ops, one per raw table, each running
`lake.maintenance.maintain`: compact the day partitions that accumulated small
files, then expire snapshots older than LAKE_SNAPSHOT_MAX_AGE_DAYS (default 7).
Not part of `make run` — hygiene on a schedule, never on the write path.
"""

import argparse
import os
from datetime import UTC, datetime, timedelta

from dagster import OpExecutionContext, job, op

from lake.iceberg_catalog import ensure_attributed, ensure_exposures
from lake.maintenance import maintain


def _max_age() -> timedelta:
    return timedelta(days=float(os.environ.get("LAKE_SNAPSHOT_MAX_AGE_DAYS", "7")))


@op
def maintain_exposures(context: OpExecutionContext) -> dict[str, int]:
    out = maintain(ensure_exposures(), _max_age(), datetime.now(UTC))
    context.log.info(f"raw.exposures: {out}")
    return out


@op
def maintain_attributed(context: OpExecutionContext) -> dict[str, int]:
    out = maintain(ensure_attributed(), _max_age(), datetime.now(UTC))
    context.log.info(f"raw.attributed_conversions: {out}")
    return out


@job
def lake_maintenance() -> None:
    maintain_exposures()
    maintain_attributed()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="lake hygiene (Dagster job)")
    parser.add_argument("--profile", default="tiny", help="informational")
    parser.parse_args(argv)
    result = lake_maintenance.execute_in_process()
    assert result.success
    for name in ("maintain_exposures", "maintain_attributed"):
        print(f"lake-maintain {name}: {result.output_for_node(name)}")


if __name__ == "__main__":
    main()
