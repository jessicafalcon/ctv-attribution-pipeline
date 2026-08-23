"""Apply the Phase 3 DDL against the running ClickHouse. Idempotent
(`create ... if not exists`); called by `make run` and the integration setup.
No migration framework yet — one flat DDL file, executed statement by
statement (DECISIONS: simplest standard solution now).

The one exception is the Phase-18a `report_snapshots` version column: ClickHouse
has no `alter table ... modify engine` (verified on the pinned 24.8 image,
ARCHITECTURE §8), so an existing table is migrated by create → backfill →
`exchange tables` → drop, below.
"""

from pathlib import Path

from clickhouse_connect.driver.client import Client

from clickhouse.client import connect

DDL = Path(__file__).parent / "ddl.sql"


# The scratch table the rebuild copies into before the atomic EXCHANGE (`<table>_v2`).
# Dropped at the START of every apply: after a post-exchange crash the leftover holds
# the OLD table, whose rows are already in the live one.
def _scratch(table: str) -> str:
    return f"{table}_v2"


class MigrationError(RuntimeError):
    """A migration refused to complete rather than losing rows. Loud on purpose:
    the serving tables are rebuildable from the lake, but report_snapshots is not
    (`orchestration/replay.py` SERVING_TABLES omits it — BACKLOG)."""


def _statements(ddl: str) -> list[str]:
    """Split into executable statements. Strip `--` line comments first so a
    semicolon inside a comment cannot split a statement (or leave a comment-only
    chunk that ClickHouse rejects as an empty query)."""
    uncommented = "\n".join(line.split("--", 1)[0] for line in ddl.splitlines())
    return [s.strip() for s in uncommented.split(";") if s.strip()]


def _engine_full(client: Client, table: str) -> str:
    rows = client.query(
        "select engine_full from system.tables "
        "where database = currentDatabase() and name = {t:String}",
        parameters={"t": table},
    ).result_rows
    return rows[0][0] if rows else ""


def migrate_report_snapshots(client: Client, table: str = "report_snapshots") -> bool:
    """Give `table` a ReplacingMergeTree VERSION column (Phase 18a). Returns True
    if it migrated, False if it was already versioned (or absent).

    ClickHouse has no `alter table ... modify engine`, so the rebuild is
    create-backfill-exchange-drop. Every row is preserved: nothing is dropped
    that has not already been copied, and `EXCHANGE TABLES` is atomic on the
    Atomic database engine (the default here). Crash-safe in both directions —
    before the exchange, the live table is untouched and the scratch copy is
    dropped by the next apply; after it, the leftover scratch table holds the old
    rows, which the live table now also holds.

    Backfill: legacy rows get `snapshot_version = reported_at`. The choice is
    almost inert — `reported_at` is part of the sort key, so a legacy row only
    ever competes with a twin of its OWN pass, never with a newer pass. (0 would
    flatten those twins back to "undefined", which is the bug being fixed.)
    """
    scratch = _scratch(table)
    client.command(f"drop table if exists {scratch}")
    engine = _engine_full(client, table)
    if not engine or "ReplacingMergeTree(" in engine:
        return False

    client.command(
        f"alter table {table} "
        "add column if not exists snapshot_version DateTime64(3, 'UTC')"
    )
    # `as {table}` copies the column list (now including snapshot_version), so the
    # scratch table cannot drift from the DDL; only the engine is overridden.
    client.command(
        f"create table {scratch} as {table} "
        "engine = ReplacingMergeTree(snapshot_version) "
        "order by (reported_at, campaign_id, period)"
    )
    # `* replace` keeps this column-count-agnostic: whatever the table holds is
    # copied, with the backfill substituted for the (all-zero) new column.
    client.command(
        f"insert into {scratch} "
        f"select * replace (reported_at as snapshot_version) from {table}"
    )

    old_rows = client.query(f"select count() from {table}").result_rows[0][0]
    new_rows = client.query(f"select count() from {scratch}").result_rows[0][0]
    if old_rows != new_rows:
        # Refuse rather than exchange a short copy. A background merge collapsing
        # the un-versioned original between the copy and the count lands here too:
        # the scratch table is dropped by the next apply, which then re-copies the
        # collapsed state and succeeds. Loud and retryable beats silent loss.
        raise MigrationError(
            f"{table}: copied {new_rows} of {old_rows} rows — refusing to exchange"
        )

    client.command(f"exchange tables {table} and {scratch}")
    client.command(f"drop table if exists {scratch}")
    return True


def apply(client: Client | None = None) -> None:
    client = client or connect()
    for statement in _statements(DDL.read_text()):
        client.command(statement)
    migrate_report_snapshots(client)


def main() -> None:
    apply()
    print("clickhouse: DDL applied")


if __name__ == "__main__":
    main()
