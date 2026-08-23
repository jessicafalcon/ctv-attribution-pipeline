"""`report_snapshots` has a defined ReplacingMergeTree version (Phase 18a).

Offline half: the SQL shape of the writer and the statement shape of the
migration. The live halves — a real re-run's twins, and the migration against a
probe table — are `tests/integration/test_snapshot_version.py`.

Why it exists: a ReplacingMergeTree with no version keeps an ARBITRARY twin, so
which pass's numbers survive a background merge was undefined by construction
(BACKLOG). It had worked only because Decimal-summed money makes the twins
identical (RUNBOOK incident 3).
"""

import re

import pytest

from clickhouse import apply as apply_mod
from clickhouse.apply import MigrationError, migrate_report_snapshots
from reconcile import rollup


class _Client:
    """Records commands; answers `select count()` / `engine_full` from a script."""

    def __init__(self, engine: str, counts: list[int] | None = None):
        self.commands: list[str] = []
        self.queries: list[str] = []
        self._engine = engine
        self._counts = list(counts or [])

    def command(self, sql: str, parameters: dict | None = None) -> None:
        self.commands.append(" ".join(sql.split()))

    def query(self, sql: str, parameters: dict | None = None):
        self.queries.append(" ".join(sql.split()))

        class _R:
            pass

        r = _R()
        if "engine_full" in sql:
            r.result_rows = [(self._engine,)] if self._engine else []
        else:
            r.result_rows = [(self._counts.pop(0),)]
        return r


def _ddl_block() -> str:
    ddl = (apply_mod.DDL).read_text()
    start = ddl.index("create table if not exists report_snapshots")
    return ddl[start : ddl.index(";", start)]


def test_ddl_declares_the_version_column_and_keeps_the_sort_key() -> None:
    block = _ddl_block()
    assert "engine = ReplacingMergeTree(snapshot_version)" in block
    # The sort key must KEEP reported_at: `make restate` reads both the pre- and
    # post-reconciliation rows through FINAL, so the pair must never collapse.
    assert "order by (reported_at, campaign_id, period)" in block


def test_snapshot_version_is_the_credited_max_processed_at_not_a_clock() -> None:
    sql = rollup._WRITE_REPORT_SNAPSHOT
    assert "(select max(processed_at) from credited)" in " ".join(sql.split())
    assert "as snapshot_version" in sql
    # No wall clock anywhere in the write (the determinism policy); the version is
    # data-derived like reported_at.
    assert not re.search(r"\bnow\(|\bnow64\(|\btoday\(", sql)


def test_snapshot_version_is_the_last_column_of_the_insert() -> None:
    # Positional INSERT: the version must sit where the DDL puts it, or every
    # column after it silently shifts.
    select_list = rollup._WRITE_REPORT_SNAPSHOT.split("from spend_by_campaign")[0]
    assert select_list.rstrip().endswith("as snapshot_version")
    columns = _ddl_block().split("\n)\nengine =")[0]
    assert columns.rstrip().endswith("snapshot_version DateTime64(3, 'UTC')")


def test_migration_is_a_no_op_once_the_table_is_versioned() -> None:
    client = _Client("ReplacingMergeTree(snapshot_version) ORDER BY (reported_at)")
    assert migrate_report_snapshots(client) is False
    # …and it still clears an orphan scratch table left by a post-exchange crash.
    assert client.commands == ["drop table if exists report_snapshots_v2"]


def test_migration_copies_before_it_exchanges_and_never_drops_the_original() -> None:
    client = _Client("ReplacingMergeTree ORDER BY (reported_at)", counts=[6, 6])
    assert migrate_report_snapshots(client) is True
    seq = client.commands
    assert seq[0] == "drop table if exists report_snapshots_v2"
    order = [i for i, c in enumerate(seq) if c.startswith(("insert into", "exchange"))]
    assert seq[order[0]].startswith("insert into report_snapshots_v2")
    assert seq[order[1]].startswith("exchange tables")
    # The only drops are of the scratch table — the original is exchanged, never
    # dropped while it holds the sole copy.
    assert all("drop table" not in c or "report_snapshots_v2" in c for c in seq)


def test_migration_refuses_to_exchange_a_short_copy() -> None:
    client = _Client("ReplacingMergeTree ORDER BY (reported_at)", counts=[6, 5])
    with pytest.raises(MigrationError, match="copied 5 of 6"):
        migrate_report_snapshots(client)
    assert not any(c.startswith("exchange") for c in client.commands)
