"""Every monetary `sum()` written to a versioned table goes through Decimal
(fix/snapshot-float-determinism, RUNBOOK incident 3). A Float64 sum's value
depends on the order the parts are visited, so two passes write twins that
differ in the 15th digit and `argMax` picks either. This pins the SQL shape
offline; the live two-pass byte-identity is pinned in
tests/integration/test_reconcile.py.
"""

import re
from pathlib import Path

from reconcile import rollup

REPO_ROOT = Path(__file__).parent.parent

MONEY = ("spend", "rev", "revenue")


def _money_sums(sql: str) -> list[str]:
    """Every sum(...) / sumIf(...) call (balanced parens) that mentions a money
    column."""
    out = []
    for m in re.finditer(r"\bsum(?:If)?\(", sql):
        depth, k = 1, m.end()
        while depth:
            depth += {"(": 1, ")": -1}.get(sql[k], 0)
            k += 1
        call = sql[m.start() : k]
        if any(re.search(rf"\b{c}\b", call) for c in MONEY):
            out.append(call)
    return out


def test_versioned_writes_sum_money_in_decimal_via_tostring() -> None:
    # every INSERT statement in rollup.py, not a hand-enumerated pair
    # every string constant holding an INSERT, whatever its name (a public
    # `REFRESH_SQL` counts; `__doc__` does not hold "insert into"); a tripwire
    # over the module's constants — an INSERT built inside a function body
    # escapes it (BACKLOG: AST scan). The behavioural money pins are the proof.
    # Scoped to the MONEY tables — and the scope is DERIVED from the DDL, not trusted:
    # test_money_tables_matches_the_ddl below asserts rollup.MONEY_TABLES equals every
    # table in clickhouse/ddl.sql that has a money column. So a new money-bearing table
    # fails the tripwire by omission (fail-closed) instead of slipping past it, which
    # is what a hand-maintained list or a per-INSERT escape comment would allow.
    money_tables = rollup.MONEY_TABLES
    inserts = [
        v
        for k, v in vars(rollup).items()
        if isinstance(v, str)
        and k != "__doc__"
        and any(f"insert into {t}" in v for t in money_tables)
    ]
    assert len(inserts) >= 2
    for sql in inserts:
        sums = _money_sums(sql)
        assert sums, "a versioned write without money sums?"
        for s in sums:
            # exact path only: toDecimal64(toString(<col>), 4) — a bare Float64 or a
            # toDecimal64(<Float64>) truncates the binary value (RUNBOOK incident 3)
            pat = r"toDecimal64\(toString\((?:[a-z]\.)?(spend|rev|revenue)\), 4\)"
            assert re.search(pat, s), s


# Money columns are Float64 today (the Decimal64-end-to-end BACKLOG row); a table that
# stores one is a table whose sums must go through the Decimal path.
MONEY_COLUMNS = ("spend", "revenue")


def _ddl_tables_with_money() -> set[str]:
    """Every `create table` in clickhouse/ddl.sql that declares a money column. The
    column list is everything up to the `)` that closes it (not the first `)` in the
    text — `DateTime64(3, 'UTC')` closes earlier)."""
    ddl = (REPO_ROOT / "clickhouse" / "ddl.sql").read_text()
    found = set()
    for block in ddl.split("create table if not exists ")[1:]:
        name = block.split("\n", 1)[0].strip()
        columns = block[block.index("(") + 1 : block.index("\n)")]
        if any(re.search(rf"^\s*{c}\s", columns, re.M) for c in MONEY_COLUMNS):
            found.add(name)
    return found


def _tables_this_module_inserts_into() -> set[str]:
    return {
        m
        for v in vars(rollup).values()
        if isinstance(v, str)
        for m in re.findall(r"insert into (\w+)", v)
    }


def test_money_tables_matches_the_ddl() -> None:
    """The tripwire's scope is DERIVED, not remembered: every table this module
    inserts into that the DDL gives a money column must be in `rollup.MONEY_TABLES`.
    Add a money-bearing INSERT here and this fails until the tuple names it — at which
    point the Decimal-path assertion above starts covering it. Fail-closed by
    construction — the first cut would have let a future `query_cost_daily` through
    (review gate).

    Raw tables with money columns (`attributed_conversions`, `exposures_landed`) are
    outside the scope on purpose: this module never inserts into them, the loader
    moves those rows unchanged, and their money is pinned by the column contract.
    """
    expected = _ddl_tables_with_money() & _tables_this_module_inserts_into()
    assert set(rollup.MONEY_TABLES) == expected
