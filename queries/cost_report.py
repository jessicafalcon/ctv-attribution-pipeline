"""Per-query cost from system.query_log (Phase 18b, Done-when 2).

`make cost-report PROFILE=<p>` runs each report / restate / bench query TAGGED with a
distinct `log_comment`, reads its cost back from system.query_log (a per-query row,
not per-session — Invariant 4), writes it to `query_cost_daily` as the `cost_rw`
writer, and rewrites the "Cost per report query" block in docs/RESULTS.md.

Non-determinism is quarantined (Invariant 3): `query_duration_ms` and `cpu_seconds`
vary run to run, so `query_cost_daily` is OUTSIDE the byte-identical guarantee (like
Iceberg metadata / Dagster run ids) and NO pipeline path reads it. `read_rows` /
`read_bytes` are server-computed and stable; the `usd` column is an ILLUSTRATIVE
conversion of a MEASURED `cpu_seconds` by a config rate — never a billed figure, and
never a SQL literal (it is computed in Python, in `to_dollars`).

Reuses `queries/bench_common.py`'s public `canonicalize` / `measure` (the `settings=`
seam carries the `log_comment` tag). The tagged queries run as the DEFAULT user; only
the cost read-back (system.query_log) and the `query_cost_daily` write go through
`cost_rw`, whose only two grants are exactly those.
"""

import argparse
import os
from datetime import UTC
from pathlib import Path

from clickhouse_connect.driver.client import Client
from prometheus_client import CollectorRegistry, Gauge

from accuracy.guard import assert_profile_marker, db_profile_marker
from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect, connect_cost
from lake.iceberg_catalog import validate_profile
from observability.push import push_registry
from queries.bench_common import canonicalize, measure

RESULTS_PATH = Path(__file__).parent.parent / "docs" / "RESULTS.md"
_START = "<!-- COST_REPORT_START -->"
_END = "<!-- COST_REPORT_END -->"

# The queries this tool costs, each tagged with a CONSTANT log_comment (never user- or
# payload-derived — threat model). The tag is the query_cost_daily key, so cost is
# per-query, not per-session (Invariant 4).
QUERIES: dict[str, Path] = {
    "report": Path(__file__).parent / "report.sql",
    "restate": Path(__file__).parent / "restatement.sql",
    "bench": Path(__file__).parent / "bench.sql",
}
_TAG_PREFIX = "cost-report:"

# Illustrative ClickHouse-Cloud-style rate — NOT a measurement, NOT a billed figure.
# Config, overridable via COST_USD_PER_CPU_SECOND. Kept OUT of SQL: `usd` is computed
# in Python (to_dollars), never multiplied inside an INSERT.
DEFAULT_USD_PER_CPU_SECOND = 3.61e-5  # ≈ $0.13 / cpu-hour, illustrative only

_COST_COLS = [
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

# The latest QueryFinish row for a tag. read_rows/read_bytes are stable; duration/cpu
# are the most recent measurement (this run's last execution of the tagged query).
_READ_COST = """
select
    toDate(event_time) as day,
    query_duration_ms,
    read_rows,
    read_bytes,
    memory_usage,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] as cpu_us,
    event_time as measured_at
from system.query_log
where log_comment = {tag:String}
    and type = 'QueryFinish'
order by event_time desc
limit 1
"""


def usd_per_cpu_second() -> float:
    return float(os.environ.get("COST_USD_PER_CPU_SECOND", DEFAULT_USD_PER_CPU_SECOND))


def cpu_seconds(profile_events_us: int) -> float:
    """CPU-seconds = OSCPUVirtualTimeMicroseconds / 1e6 — the measured ProfileEvents
    value. The mutation sentinel (`constant-return:0.0`) is killed by
    test_cpu_seconds_is_the_profileevents_value."""
    return profile_events_us / 1_000_000.0


def to_dollars(cpu_secs: float, rate: float | None = None) -> float:
    """Illustrative $ = cpu_seconds × rate, the rate read from config (never a SQL
    literal). The mutation sentinel (`constant-return:0.0`) is killed by
    test_dollars_come_from_the_config_rate_not_a_hardcoded_sql_literal."""
    return cpu_secs * (usd_per_cpu_second() if rate is None else rate)


def measure_costs(runner: Client, cost: Client) -> list[dict]:
    """Run each tagged query as `runner` (default user), read its cost back as `cost`
    (cost_rw) from system.query_log, and INSERT one query_cost_daily row per tag.
    Returns the rows for the RESULTS writer."""
    canonicalize(runner)
    for tag, sql in QUERIES.items():
        measure(runner, sql.read_text(), settings={"log_comment": _TAG_PREFIX + tag})
    runner.command("system flush logs")  # make the QueryFinish rows visible now

    rows: list[dict] = []
    for tag in QUERIES:
        result = cost.query(
            _READ_COST, parameters={"tag": _TAG_PREFIX + tag}
        ).result_rows
        if not result:
            raise RuntimeError(f"no query_log row for tag {tag!r} — was the query run?")
        day, dur, read_rows, read_bytes, mem, cpu_us, measured_at = result[0]
        if measured_at.tzinfo is None:  # naive-UTC read → tz-aware before the write
            measured_at = measured_at.replace(tzinfo=UTC)
        cpu_s = cpu_seconds(cpu_us)
        row = {
            "day": day,
            "query_tag": tag,
            "query_duration_ms": dur,
            "read_rows": read_rows,
            "read_bytes": read_bytes,
            "memory_usage": mem,
            "cpu_seconds": cpu_s,
            "usd": to_dollars(cpu_s),
            "measured_at": measured_at,
        }
        cost.insert(
            "query_cost_daily",
            [[row[c] for c in _COST_COLS]],
            column_names=_COST_COLS,
        )
        rows.append(row)
    return rows


def render(rows: list[dict]) -> str:
    """The docs/RESULTS.md block, regenerated by this command (the cost-levers /
    scale-curve marker pattern, so `make check-docs` guards its presence). The numbers
    are one run's measurement — cpu_seconds / usd are illustrative and vary run to run
    (Invariant 3); read_rows / read_bytes are stable. Prose lives outside markers."""
    lines = [
        _START,
        "",
        "| query | read_rows | read_bytes | cpu_seconds | usd (illustrative) |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['query_tag']} | {r['read_rows']} | {r['read_bytes']} "
            f"| {r['cpu_seconds']:.6f} | {r['usd']:.8f} |"
        )
    lines += [
        "",
        f"_cpu_seconds = ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6; "
        f"usd = cpu_seconds × ${usd_per_cpu_second():.2e}/cpu-s (illustrative, not "
        f"billed). One run's measurement — regenerated by `make cost-report`._",
        "",
        _END,
    ]
    return "\n".join(lines)


def write_results(section: str) -> None:
    text = RESULTS_PATH.read_text()
    if _START not in text or _END not in text:
        raise SystemExit(
            f"{RESULTS_PATH} is missing the {_START} / {_END} markers — add the "
            "block skeleton before running cost-report."
        )
    head = text[: text.index(_START)]
    tail = text[text.index(_END) + len(_END) :]
    RESULTS_PATH.write_text(head + section + tail)


def push_cost_metrics(rows: list[dict]) -> None:
    """Push per-query cost to the Pushgateway as `clickhouse_query_cost_*{query_tag}`
    gauges (job "cost"), so the Grafana cost panel can graph it — no-op unless
    PUSHGATEWAY_URL is set. Grafana's only datasource is Prometheus, so cost reaches it
    through the push path, not by reading query_cost_daily directly."""
    registry = CollectorRegistry()
    cpu = Gauge(
        "clickhouse_query_cost_cpu_seconds",
        "cpu-seconds for the tagged query (illustrative, one run)",
        ["query_tag"],
        registry=registry,
    )
    usd = Gauge(
        "clickhouse_query_cost_usd",
        "illustrative $ for the tagged query (cpu_seconds × config rate)",
        ["query_tag"],
        registry=registry,
    )
    for r in rows:
        cpu.labels(query_tag=r["query_tag"]).set(r["cpu_seconds"])
        usd.labels(query_tag=r["query_tag"]).set(r["usd"])
    push_registry(registry, "cost")


def format_table(rows: list[dict]) -> str:
    out = ["cost per report query (this run):"]
    for r in rows:
        out.append(
            f"  {r['query_tag']:8s} read_rows={r['read_rows']:>10} "
            f"cpu_s={r['cpu_seconds']:.6f} usd={r['usd']:.8f}"
        )
    return "\n".join(out)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    args = parser.parse_args(argv)
    # Same shape as `make eval` / `make rollup-bench`: validate the profile in-process
    # (nothing is derived from it but the guard) and refuse a DB populated from a
    # different profile (BACKLOG 43 marker) before touching anything.
    profile = validate_profile(args.profile)
    runner = connect()
    assert_profile_marker(db_profile_marker(runner), profile)
    apply_ddl(runner)
    rows = measure_costs(runner, connect_cost())
    write_results(render(rows))
    push_cost_metrics(rows)  # no-op unless PUSHGATEWAY_URL is set (Grafana cost panel)
    print(format_table(rows))
    print("\nwrote docs/RESULTS.md → 'Cost per report query'")


if __name__ == "__main__":
    main()
