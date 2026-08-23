"""Every monetary `sum()` written to a versioned table goes through Decimal
(fix/snapshot-float-determinism, RUNBOOK incident 3). A Float64 sum's value
depends on the order the parts are visited, so two passes write twins that
differ in the 15th digit and `argMax` picks either. This pins the SQL shape
offline; the live two-pass byte-identity is pinned in
tests/integration/test_reconcile.py.
"""

import re

from reconcile import rollup

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
    # Scoped to the MONEY tables, named ONCE in the module under test
    # (`rollup.MONEY_TABLES`) — Phase 18a added money-free INSERT constants here
    # (rollup_dirty, rollup_refresh_marker). A new money-bearing table is covered
    # by adding it to that constant and is NOT covered if you forget: there is no
    # per-INSERT escape comment, which is how a tripwire stops firing.
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
