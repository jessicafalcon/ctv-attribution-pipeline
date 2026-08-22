"""Lake hygiene (Phase 17, spec D10): snapshot expiry + small-file rewrite.

An Iceberg append creates a new snapshot AND at least one new Parquet file per
partition it touches, so an append-only lake grows two ways: the snapshot
history (every old metadata + manifest stays readable for time-travel) and the
file count (a 55-row tiny run lands 3 days × up to 8 buckets = 24 tiny files,
and every re-land doubles them). Both are hygiene, not correctness: dedup-on-read
makes the rows right regardless (BACKLOG "data/lake accumulates unboundedly").

- `expire_snapshots`: pyiceberg's `table.maintenance.expire_snapshots()
  .older_than(ts)` (verified on 0.11.1 — ARCHITECTURE §8). The current
  snapshot is always kept.
- `compact_days`: pyiceberg 0.11.1 has no `rewrite_data_files` (and its
  `dynamic_partition_overwrite` refuses non-identity transforms such as `day`),
  so a day whose partitions hold >1 file is rewritten with `overwrite(rows,
  overwrite_filter=<that day's event_time range>)` — delete the day's files,
  write the day once → one file per (day, bucket), same rows (physical
  duplicates included: compaction does not dedup; the read does).

Compaction is a REWRITE of the record's data files (row content unchanged —
asserted on both raw tables offline), so `make lake-maintain` prompts like the
other destructive paths (lake/destructive.py). Expiry is metadata-only on
pyiceberg 0.11.1 (no `remove_orphan_files`): the directory does not shrink.
Non-deterministic by nature (wall-clock age, snapshot ids) and therefore OFF the
`make run` path and outside the byte-identical guarantee like all lake
metadata (DECISIONS Phase 12/17); every asserted check reads rows back.
"""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table


def expire_snapshots(table: Table, older_than: datetime) -> int:
    """Expire snapshots committed before `older_than`; return how many went."""
    before = len(table.metadata.snapshots)
    table.maintenance.expire_snapshots().older_than(older_than).commit()
    table.refresh()
    return before - len(table.metadata.snapshots)


def days_with_small_files(table: Table) -> list[str]:
    """Day partitions (YYYY-MM-DD) where some bucket holds more than one file."""
    days = {
        p["partition"]["event_time_day"].isoformat()
        for p in table.inspect.partitions().to_pylist()
        if p["file_count"] > 1
    }
    return sorted(days)


def compact_days(table: Table, days: list[str]) -> int:
    """Rewrite each of `days` so every (day, bucket) partition is one file.
    Returns the number of days rewritten. Rows are unchanged."""
    for day in days:
        start = datetime.fromisoformat(day).replace(tzinfo=UTC)
        end = start + timedelta(days=1)
        day_filter = And(
            GreaterThanOrEqual("event_time", start.isoformat()),
            LessThan("event_time", end.isoformat()),
        )
        rows: pa.Table = table.scan(row_filter=day_filter).to_arrow()
        if rows.num_rows:
            table.overwrite(rows, overwrite_filter=day_filter)
    return len(days)


def maintain(table: Table, max_age: timedelta, now: datetime) -> dict[str, int]:
    """One maintenance pass over `table`: compact first (so the rewrite's own
    snapshot is the one kept), then expire snapshots older than `now − max_age`."""
    compacted = compact_days(table, days_with_small_files(table))
    expired = expire_snapshots(table, now - max_age)
    return {"compacted_days": compacted, "expired_snapshots": expired}
