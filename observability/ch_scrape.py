"""One-shot ClickHouse storage scrape (Phase 18a). Stage prefix `clickhouse_`.

Not a daemon and not a compose service: every stage in this pipeline is a finite
drain (ARCHITECTURE §8), so there is nothing for a pull-scrape to reach after the
process exits. This runs at the END of `make run`, `make run-hot` and `make
metrics-capture`, exactly like the terminal registry dumps beside it — the live
scrape path is 18b's Pushgateway item.

What it reports is how ClickHouse is STORING the data, never what the data says:

  clickhouse_active_parts{table}   — active parts per table. `SELECT ... FINAL`
      physically reads every un-merged version-part before collapsing it, so part
      count IS the operating cost of FINAL (RUNBOOK incident #1). A rollup refresh
      adds a part; a reconciled conversion adds one.
  clickhouse_unmerged_parts{table} — active parts at merge level 0, i.e. parts no
      merge has touched yet: the work the background merger still owes. State, not
      timing.
  clickhouse_merge_backlog_seconds — the longest merge RUNNING at scrape time
      (`system.merges.elapsed`). Read the caveat in its HELP text and in DECISIONS
      18a: at profile scale merges finish in microseconds, so this is 0 in every
      capture on this stack. It is a point-in-time observation of in-flight work,
      not a backlog that accumulates.

Reads as `metrics_ro`, a user granted SELECT on `system.parts` and `system.merges`
and nothing else (clickhouse/users.d/metrics-ro.xml) — it cannot read a pipeline row.
"""

import argparse
import os
import time

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

ACTIVE_PARTS = Gauge(
    "clickhouse_active_parts",
    "Active data parts per table. A FINAL scan physically reads every un-merged "
    "version-part, so this is the operating cost of FINAL (RUNBOOK incident #1).",
    ["table"],
)

UNMERGED_PARTS = Gauge(
    "clickhouse_unmerged_parts",
    "Active parts at merge level 0 — parts no background merge has touched yet, "
    "i.e. the collapse work still owed. State, not timing: deterministic for a "
    "given load, unlike the wall-clock of a merge in flight.",
    ["table"],
)

MERGE_BACKLOG = Gauge(
    "clickhouse_merge_backlog_seconds",
    "Elapsed seconds of the longest merge RUNNING at scrape time "
    "(system.merges.elapsed); 0 when none is running. CAVEAT: this is a "
    "point-in-time sample of in-flight work, not an accumulating backlog — at "
    "profile scale merges finish in microseconds, so a capture reads 0. The "
    "deterministic view of pending merge work is clickhouse_unmerged_parts.",
)

# The scraper's own tables of interest: everything this pipeline writes. Named, not
# discovered, so a stray table (a probe, a bench scratch) cannot appear in a captured
# fixture and move a threshold.
TABLES = (
    "attributed_conversions",
    "exposures_landed",
    "campaign_hourly",
    "report_snapshots",
    "rollup_dirty",
)

_PARTS = """
select
    table,
    countIf(active) as parts,
    countIf(active and level = 0) as unmerged
from system.parts
where database = {db:String}
    and table in {tables:Array(String)}
group by table
order by table
"""

_MERGES = """
select coalesce(max(elapsed), 0)
from system.merges
where database = {db:String}
"""


def connect_metrics() -> Client:
    """The SELECT-only handle for the scrape. `metrics_ro` is granted SELECT on
    `system.parts` and `system.merges` only, so a scrape cannot read pipeline data
    even by accident; overridable for a hardened deploy."""
    return get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_METRICS_USER", "metrics_ro"),
        password=os.environ.get("CLICKHOUSE_METRICS_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "default"),
    )


# Settling. Part counts right after a load are IN FLUX: the background merger is
# still collapsing the parts the inserts just created (measured on a clean tiny
# stack: 13 active parts at the end of `make run`, 9 a few seconds later, then
# stable). A capture taken at an arbitrary moment of that decay is not reproducible,
# and the promtool fixtures are built from captures — so the scrape WAITS for
# quiescence first: no merge running, and the part counts unchanged across two
# consecutive samples. The wait is bounded; how long it took is printed, never
# recorded (wall clock must not enter a .prom the fixtures are baked from).
_SETTLE_INTERVAL_S = 0.5
_SETTLE_TIMEOUT_S = 60.0


def _sample(client: Client, db: str) -> tuple[dict[str, tuple[int, int]], float]:
    rows = client.query(
        _PARTS, parameters={"db": db, "tables": list(TABLES)}
    ).result_rows
    counts = {table: (int(parts), int(unmerged)) for table, parts, unmerged in rows}
    backlog = float(client.query(_MERGES, parameters={"db": db}).result_rows[0][0])
    return counts, backlog


class UnsettledError(RuntimeError):
    """The storage state never stopped moving inside the cap. Loud on purpose: a
    capture taken mid-decay is not reproducible, and the promtool fixtures are baked
    from captures — a silently unsettled .prom would put an invented number in a
    committed fixture. The cap is a backstop against hanging a run, not a fallback
    value."""


def settle(client: Client, db: str) -> tuple[dict[str, tuple[int, int]], float, float]:
    """Sample until the storage STATE stops moving — no merge running and the part
    counts unchanged across two consecutive reads — and return (counts, backlog,
    waited). The condition is state, not elapsed time; the cap only bounds how long a
    scrape may hold a pipeline run open, and reaching it raises."""
    deadline = time.monotonic() + _SETTLE_TIMEOUT_S
    start = time.monotonic()
    previous, backlog = _sample(client, db)
    while time.monotonic() < deadline:
        time.sleep(_SETTLE_INTERVAL_S)
        counts, backlog = _sample(client, db)
        if counts == previous and backlog == 0.0:
            return counts, backlog, time.monotonic() - start
        previous = counts
    raise UnsettledError(
        f"ClickHouse storage state still moving after {_SETTLE_TIMEOUT_S:.0f}s "
        f"(last sample {previous}, merge backlog {backlog}s) — a capture taken now "
        "would not reproduce. Re-run the scrape once the stack is idle."
    )


def scrape(client: Client | None = None) -> dict[str, float]:
    """Fill the gauges from ClickHouse's own SETTLED storage state (see `settle`).
    Returns the values for the caller's log line, plus how long settling took —
    which is printed and never written to the .prom. Tables absent from
    `system.parts` (never written on this stack) are reported as 0, not omitted — a
    missing series and a zero series are different claims, and an alert must see the
    zero."""
    client = client or connect_metrics()
    db = os.environ.get("CLICKHOUSE_DB", "default")
    counts, backlog, waited = settle(client, db)
    out: dict[str, float] = {"settle_seconds": waited}
    for table in TABLES:
        parts, unmerged = counts.get(table, (0, 0))
        ACTIVE_PARTS.labels(table=table).set(parts)
        UNMERGED_PARTS.labels(table=table).set(unmerged)
        out[f"parts:{table}"] = parts
        out[f"unmerged:{table}"] = unmerged
    MERGE_BACKLOG.set(backlog)
    out["merge_backlog_seconds"] = backlog
    return out


def _registry() -> CollectorRegistry:
    """A registry holding only this scrape's gauges — the .prom this writes is the
    ClickHouse view, kept beside (never mixed into) the stage registries."""
    registry = CollectorRegistry()
    for gauge in (ACTIVE_PARTS, UNMERGED_PARTS, MERGE_BACKLOG):
        registry.register(gauge)
    return registry


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="one-shot ClickHouse storage scrape")
    parser.add_argument(
        "--metrics-out",
        help="write the registry to this .prom file (make metrics-capture)",
    )
    args = parser.parse_args(argv)
    values = scrape()
    if args.metrics_out:
        write_to_textfile(args.metrics_out, _registry())
    parts = sum(v for k, v in values.items() if k.startswith("parts:"))
    unmerged = sum(v for k, v in values.items() if k.startswith("unmerged:"))
    print(
        f"clickhouse: {int(parts)} active parts ({int(unmerged)} unmerged), "
        f"merge backlog {values['merge_backlog_seconds']:.0f}s "
        f"(settled after {values['settle_seconds']:.1f}s)"
    )


if __name__ == "__main__":
    main()
