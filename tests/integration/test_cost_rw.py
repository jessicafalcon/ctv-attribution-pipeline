"""LIVE: the cost writer's principal (`cost_rw`) can read system.query_log and write
`query_cost_daily` and NOTHING else (Phase 18b) — the mirror of test_metrics_ro.py /
test_agent_readonly.py. A writer, so its own principal; it still cannot read a single
pipeline row.

Runs under any `make test-int*` target; asserts grants, never a profile's numbers.
"""

from datetime import UTC, date, datetime

import pytest

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect, connect_cost

ACCESS_DENIED = "ACCESS_DENIED"

# Reads and writes that must ALL be refused at the database, not by convention.
FORBIDDEN = [
    "select count() from attributed_conversions",
    "select * from exposures_landed limit 1",
    "select count() from query_cost_daily",  # INSERT-only: its own table is unreadable
    "insert into rollup_dirty values ('c', now(), now())",
    "alter table query_cost_daily delete where 1",
    "drop table query_cost_daily",
    "create table pwned (x Int8) engine = Memory",
]

_COLS = [
    "day",
    "query_tag",
    "query_duration_ms",
    "read_rows",
    "read_bytes",
    "memory_usage",
    "cpu_seconds",
    "usd",
    "measured_at",
]


def test_cost_rw_reads_query_log_writes_its_table_and_nothing_else() -> None:
    apply_ddl(connect())  # ensure query_cost_daily exists
    cost = connect_cost()

    # Allowed: SELECT system.query_log, INSERT query_cost_daily — its only two grants.
    cost.query("select count() from system.query_log")
    probe = [
        date(2026, 8, 1),
        "__probe__",
        1,
        1,
        1,
        1,
        0.0,
        0.0,
        datetime(2026, 8, 1, tzinfo=UTC),
    ]
    cost.insert("query_cost_daily", [probe], column_names=_COLS)

    # Denied: every read of a pipeline table, every write beyond its one table, DDL.
    for stmt in FORBIDDEN:
        run = cost.query if stmt.lower().startswith("select") else cost.command
        with pytest.raises(Exception) as excinfo:
            run(stmt)
        assert ACCESS_DENIED in str(excinfo.value), stmt

    connect().command(
        "alter table query_cost_daily delete where query_tag = '__probe__'"
    )
