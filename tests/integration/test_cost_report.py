"""LIVE (Phase 18b, Done-when 2): each tagged report / restate / bench query lands
ONE `query_cost_daily` row keyed by its tag, written by `cost_rw` (Invariant 4); and
the DDL is a no-op on re-run. Runs on whatever populated stack the `make test-int*`
target left; asserts the per-query keying, not a magnitude (cost is non-deterministic,
Invariant 3)."""

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect, connect_cost
from queries.cost_report import QUERIES, measure_costs


def test_each_measured_query_lands_one_row_keyed_by_its_tag() -> None:
    runner = connect()
    apply_ddl(runner)
    runner.command("truncate table query_cost_daily")
    rows = measure_costs(runner, connect_cost())
    assert {r["query_tag"] for r in rows} == set(QUERIES)

    got = runner.query(
        "select query_tag, read_rows from query_cost_daily final order by query_tag"
    ).result_rows
    assert [g[0] for g in got] == sorted(QUERIES)  # exactly one current row per tag
    assert all(g[1] >= 0 for g in got)  # server-computed read_rows present


def test_query_cost_daily_ddl_is_a_no_op_on_re_run() -> None:
    runner = connect()
    apply_ddl(runner)
    before = runner.query("select count() from query_cost_daily").result_rows[0][0]
    apply_ddl(runner)  # create … if not exists → no error, no row change
    after = runner.query("select count() from query_cost_daily").result_rows[0][0]
    assert after == before
