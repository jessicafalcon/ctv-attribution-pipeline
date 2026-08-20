"""`make bench` — naive-vs-optimized reporting benchmark (Phase 7).

Runs the SAME four-metric question two ways and reports latency, rows read, and
bytes read for each:
  naive     — full FINAL scan-and-join of the raw serving tables (queries/
              report.sql: attributed_conversions ⋈ exposures_landed).
  optimized — the pre-aggregated campaign_hourly rollup (queries/bench.sql).

Rows/bytes read come from ClickHouse's own X-ClickHouse-Summary
(clickhouse-connect `QueryResult.summary`) and are cache-independent, the honest
structural signal. They are deterministic ONLY because `run()` first canonicalizes
every read table to its merged steady state (`OPTIMIZE ... FINAL`, see
`_canonicalize`): both `campaign_hourly` and `attributed_conversions` are
versioned-replace ReplacingMergeTrees, and a FINAL scan physically READS every
un-merged version-part before collapsing it — so without the OPTIMIZE, read_rows
counts transient part-bloat (one extra full copy per rollup refresh; ≥1 extra row
per reconciled conversion_id) and drifts with background-merge timing, not the
logical table size. Merged steady state is also the honest comparison: a scheduled
rollup serves its collapsed form in production. Latency is wall-clock median over a
few runs; at profile scale it is noisy and can even favour the naive scan on tiny
tables (the rollup's win is scan size, which pays off at volume — SCALING.md). Both
queries run with the query cache off so neither is served a cached result.

Two gates on the result:
  equality  — the two must return the SAME metric rows, rounded to 6 decimals
              (sum-of-hourly-sums vs a single sum differ at ulp scale; raw float
              `==` would flake). An optimization that changed the answer is a bug.
  direction — the optimized query must read FEWER rows than the naive scan
              (magnitude-free; pins the "rollup wins" claim without pinning any
              tunable number). Catches bench.sql silently reverting to a raw scan.
"""

import time
from pathlib import Path

from clickhouse_connect.driver.client import Client

from clickhouse.client import connect

NAIVE_SQL = Path(__file__).parent / "report.sql"
OPT_SQL = Path(__file__).parent / "bench.sql"
_RUNS = 5  # median wall-clock over this many runs (rows/bytes are deterministic)
_ROUND = 6  # decimals for the metric-equality gate

# Every ReplacingMergeTree read by the two queries. Both sides are canonicalized to
# merged steady state before measuring, so read_rows reflects logical table size on
# BOTH sides (not un-merged part-bloat) — the only apples-to-apples comparison, and
# the only deterministic one (see module docstring / _canonicalize).
_READ_TABLES = ("attributed_conversions", "exposures_landed", "campaign_hourly")


def _canonicalize(client: Client) -> None:
    """Collapse each read table to its merged steady state before measuring.

    A versioned-replace ReplacingMergeTree keeps every superseded version as physical
    rows in un-merged parts until a background merge fires; a FINAL scan physically
    reads all of them, so read_rows counts part-bloat that grows per refresh /
    correction and drifts with merge timing. `campaign_hourly` gains a full copy per
    rollup refresh; `attributed_conversions` gains a row per reconciled conversion_id
    (a higher-processed_at version of the hot row). `OPTIMIZE ... FINAL` forces the
    merge — synchronous on single-node (alter_sync=1), a no-op if already merged
    (optimize_throw_if_noop=0) — so read_rows measures the steady state a scheduled
    rollup serves in production: deterministic and identical on re-run. Both settings
    are relied-upon ClickHouse DEFAULTS (not overridden in `clickhouse/`): overriding
    alter_sync or optimize_throw_if_noop would silently reintroduce the very non-
    determinism this collapses (a re-run could throw, or measure before the merge)."""
    for table in _READ_TABLES:
        client.command(f"optimize table {table} final")


def _round_row(row: tuple) -> tuple:
    """Round floats to _ROUND decimals for equality; leave ints/str/None as-is."""
    return tuple(round(v, _ROUND) if isinstance(v, float) else v for v in row)


def _measure(client: Client, sql: str) -> dict:
    """Run `sql` _RUNS times (query cache off), return median latency plus the
    server's read_rows/read_bytes and the result rows (deterministic across runs)."""
    latencies = []
    result = None
    for _ in range(_RUNS):
        start = time.perf_counter()
        result = client.query(sql, settings={"use_query_cache": 0})
        latencies.append((time.perf_counter() - start) * 1000.0)
    summary = result.summary
    return {
        "latency_ms": sorted(latencies)[len(latencies) // 2],
        "read_rows": int(summary.get("read_rows", 0)),
        "read_bytes": int(summary.get("read_bytes", 0)),
        "rows": result.result_rows,
    }


def run(client: Client | None = None) -> tuple[dict, dict]:
    """Measure the naive and optimized queries; assert they agree and that the
    optimized query reads fewer rows. Both sides are canonicalized to merged steady
    state first, so read_rows is deterministic and the comparison is apples-to-apples.
    """
    client = client or connect()
    _canonicalize(client)
    naive = _measure(client, NAIVE_SQL.read_text())
    optimized = _measure(client, OPT_SQL.read_text())

    # NOTE: this proves equality on the loaded profile's data, not that the rollup
    # refresh is structurally identical to report.sql for every profile — verify
    # before reusing bench on another profile (BACKLOG; Phase-8 shared-IP is where
    # wrong-household rows could diverge).
    naive_rows = [_round_row(r) for r in naive["rows"]]
    opt_rows = [_round_row(r) for r in optimized["rows"]]
    if not naive_rows:
        raise AssertionError(
            "benchmark ran against an empty rollup — seed a profile and `make run` "
            "first (equality on two empty result sets is a vacuous false-green)"
        )
    if naive_rows != opt_rows:
        raise AssertionError(
            "benchmark mismatch: optimized rollup disagrees with the naive scan "
            f"(rounded to {_ROUND} dp)\n  naive={naive_rows}\n  opt  ={opt_rows}"
        )
    # Direction guard: equal metric rows alone don't prove the rollup is still the
    # thing being read — bench.sql could revert to scanning the raw tables and still
    # return equal rows, silently voiding the "rollup reads less" claim while green.
    # A magnitude-free assert pins the optimization direction (the actual claim)
    # without pinning any tunable number (long_delay steady state: 340 < 835). Valid
    # only because _canonicalize merged both sides first — otherwise un-merged
    # part-bloat can push the rollup's read ABOVE the naive scan (CI once saw 1020).
    if optimized["read_rows"] >= naive["read_rows"]:
        raise AssertionError(
            "benchmark lost its optimization: the optimized query read no fewer rows "
            f"than the naive scan (opt={optimized['read_rows']} >= "
            f"naive={naive['read_rows']}) — is bench.sql still reading campaign_hourly?"
        )
    return naive, optimized


def _ratio(naive: float, optimized: float) -> str:
    return f"{naive / optimized:.1f}x" if optimized else "n/a"


def format_table(naive: dict, optimized: dict) -> str:
    rows = [
        ("metric", "naive (full scan)", "optimized (rollup)", "naive/opt"),
        (
            "rows read",
            str(naive["read_rows"]),
            str(optimized["read_rows"]),
            _ratio(naive["read_rows"], optimized["read_rows"]),
        ),
        (
            "bytes read",
            str(naive["read_bytes"]),
            str(optimized["read_bytes"]),
            _ratio(naive["read_bytes"], optimized["read_bytes"]),
        ),
        (
            "latency (ms, median)",
            f"{naive['latency_ms']:.2f}",
            f"{optimized['latency_ms']:.2f}",
            _ratio(naive["latency_ms"], optimized["latency_ms"]),
        ),
    ]
    widths = [max(len(r[i]) for r in rows) for i in range(4)]
    lines = ["  ".join(c.rjust(widths[i]) for i, c in enumerate(r)) for r in rows]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def main() -> None:
    naive, optimized = run()
    print("benchmark — naive full scan vs optimized campaign_hourly rollup")
    print("(same four-metric answer, verified equal to 6 dp)\n")
    print(format_table(naive, optimized))


if __name__ == "__main__":
    main()
