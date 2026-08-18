"""Offline tests for reporting v1: the formatter (incl. the NULL path that tiny
never exercises — every tiny campaign has purchases), and text guards on the
load-bearing SQL rules (FINAL on both tables, nullIf on every ratio). The metric
math is SQL-only by design (no Python core to drift from it); its VALUES are
pinned live in tests/integration/test_eval_report.py."""

import re
from pathlib import Path

from queries.report import format_table

SQL = (Path(__file__).parent.parent / "queries" / "report.sql").read_text().lower()


def test_format_table_renders_null_denominator() -> None:
    # a campaign with spend but zero purchases → cpa NULL (the zero-denominator
    # path tiny cannot reach)
    rows = [
        ("camp-00", 4.48, 47, 16, 7, 431.99, 96.4263, 0.64, 0.3404, 0.1915),
        ("camp-09", 1.00, 10, 0, 0, 0.0, 0.0, None, 0.0, 0.0),
    ]
    text = format_table(rows)
    assert "NULL" in text
    assert "camp-09" in text
    assert "96.4263" in text  # floats rendered at 4dp


def test_sql_reads_final_on_both_tables() -> None:
    # FINAL must apply to both RMT tables; tolerate an optional `as <alias>`
    # between the table and FINAL (ClickHouse requires the alias before FINAL)
    assert re.search(r"exposures_landed\s+(as\s+\w+\s+)?final", SQL)
    assert re.search(r"attributed_conversions\s+(as\s+\w+\s+)?final", SQL)


def test_sql_guards_every_denominator_with_nullif() -> None:
    # roas, cpa, cvr, site_visit_rate — four ratios, four nullIf guards
    assert SQL.count("nullif(") >= 4


def test_sql_credits_only_attributed_rows() -> None:
    assert "where a.attributed = 1" in SQL
