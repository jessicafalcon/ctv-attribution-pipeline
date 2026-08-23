"""`make rollup-bench` — full rollup rebuild vs dirty-set refresh (Phase 18a).

Run after `make run PROFILE=<p>` on a profile whose reconcile pass changed something
(`long_delay` is the gate profile). Three things come out of one pass:

  equality  — the dirty-set refresh leaves `campaign_hourly FINAL` identical to what a
              full rebuild leaves, to 6 dp. The full rebuild is the ORACLE; an
              incremental refresh that changed an answer is a bug, not a saving.
  cost      — rows read and rows written, full vs incremental, as measured by
              ClickHouse's own summary. The direction ASSERT is on rows WRITTEN
              (magnitude-free, Phase 7/13 precedent): rewriting only the changed keys
              is what an incremental rollup buys, and it is what keeps campaign_hourly
              from gaining a full copy per refresh (RUNBOOK incident #1's real cost).
              Rows read are PRINTED, not asserted, and the printout says why: at
              profile scale the source tables are a SINGLE granule, so a dirty-key
              predicate has nothing to prune and costs more than it saves (the
              dirty-key lookup itself reads). A read-side win needs a multi-granule
              table — measured on bench_large when 18b's query-cost work runs it
              (BACKLOG). Asserting it here would be claiming scale we do not run.
  the gate  — the dirty set is the contract between the loader and the rollup, and a
              wrong one is SILENTLY wrong while the equality oracle still passes. The
              rule: every key whose aggregate changed is in the dirty set
              (changed ⊆ dirty). A key the refresh would not recompute is the
              silent-wrong case; an extra key is only wasted work, and the dirty set
              is a LAWFUL superset — reloading a touched day re-records that day's
              exposure hours at the new version whether or not their aggregate moved.
              The over-refresh count |dirty − changed| is printed, not asserted.

Reconstructing "before" and "after" without a time machine: the hot-attributed set is
invariant under reconciliation (it only rewrites hot-UNattributed rows), so the
pre-reconciliation rollup is the same expression restricted to `path = 'hot'` — the
seam the PRE report snapshot already uses. The dirty side is reconstructed the same
way: the keys whose `rollup_dirty` version rose above the watermark the HOT load would
have left (`max(ingest_time)` over exposures, `max(processed_at)` over hot rows).

Nothing here changes a served number. The measured refreshes run at the pipeline's own
offset, so the rows they insert are byte-identical twins of the ones already there, and
the watermark they advance is this tool's own marker row, never the pipeline's.
"""

import argparse
from pathlib import Path

from clickhouse_connect.driver.client import Client

from accuracy.guard import assert_profile_marker, db_profile_marker
from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from lake.iceberg_catalog import validate_profile
from queries.bench_common import ROUND, canonicalize, round_row
from reconcile import rollup
from reconcile.reconcile import RECONCILE_DELTA_MS

# This tool's own watermark row: measuring must never advance the pipeline's.
BENCH_MARKER = "rollup_bench"

RESULTS_PATH = Path(__file__).parent.parent / "docs" / "RESULTS.md"
_START = "<!-- ROLLUP_BENCH_START -->"
_END = "<!-- ROLLUP_BENCH_END -->"

_HOT_WATERMARK = """
select max(t) from
(
    select max(ingest_time) as t from exposures_landed final
    union all
    select max(processed_at) as t from attributed_conversions final where path = 'hot'
)
"""

_DIRTY_ABOVE = """
select
    campaign_id,
    hour
from rollup_dirty final
where version > {watermark:DateTime64(3)}
"""


def _keyed(rows: list[tuple]) -> dict[tuple, tuple]:
    """(campaign_id, hour) → the rounded aggregate for that key."""
    return {(r[0], r[1]): round_row(r[2:]) for r in rows}


def _measure_refresh(client: Client, sql: str, params: dict) -> dict:
    """Run ONE refresh and report what ClickHouse says it read and wrote.

    Single-shot, unlike `bench_common.measure`: this statement WRITES, so running it
    five times for a median would write five times. Latency is not the claim here —
    rows read and rows written are, and both are deterministic on a canonicalized
    table.
    """
    result = client.query(sql, parameters=params, settings={"use_query_cache": 0})
    summary = result.summary
    return {
        "read_rows": int(summary.get("read_rows", 0)),
        "written_rows": int(summary.get("written_rows", 0)),
    }


def run(client: Client | None = None) -> dict:
    """Measure and gate. Returns the numbers the printer formats."""
    client = client or connect()
    canonicalize(client)

    total_keys = len(rollup.campaign_hourly_rows(client))
    if not total_keys:
        raise AssertionError(
            "rollup-bench ran against an empty rollup — `make seed` and `make run` "
            "a profile first (comparing two empty rebuilds is a vacuous false-green)"
        )

    # --- the gate ---------------------------------------------------------------
    before = _keyed(rollup.campaign_hourly_rows(client, hot_only=True))
    after = _keyed(rollup.campaign_hourly_rows(client))
    changed = {k for k in before.keys() | after.keys() if before.get(k) != after.get(k)}

    watermark = client.query(_HOT_WATERMARK).result_rows[0][0]
    dirty = {
        (r[0], r[1])
        for r in client.query(
            _DIRTY_ABOVE, parameters={"watermark": watermark}
        ).result_rows
    }
    if not changed:
        raise AssertionError(
            "rollup-bench: the reconcile pass changed no rollup key, so the dirty-set "
            "gate would pass vacuously — run it on a profile whose reconciliation "
            "restates something (long_delay)"
        )
    missed = changed - dirty
    if missed:
        raise AssertionError(
            "DIRTY SET MISSED A CHANGED KEY — the incremental refresh would serve a "
            f"stale rollup for {sorted(missed)[:5]} ({len(missed)} keys). The "
            "full-refresh oracle cannot see this; that is why this gate exists."
        )
    if len(dirty) >= total_keys:
        raise AssertionError(
            f"dirty set is not a saving: {len(dirty)} keys of {total_keys} total"
        )

    # --- equality + cost --------------------------------------------------------
    # Our own watermark row, seeded where the hot load would have left it, so the
    # incremental refresh below recomputes exactly the reconcile-affected keys.
    client.command(
        "insert into rollup_refresh_marker values ({m:String}, {w:DateTime64(3)})",
        parameters={"m": BENCH_MARKER, "w": watermark},
    )
    params = {"offset_ms": RECONCILE_DELTA_MS, "marker": BENCH_MARKER}
    full = _measure_refresh(client, rollup.refresh_sql(full=True), params)
    full_rows = _keyed(_final_rows(client))
    incremental = _measure_refresh(client, rollup.refresh_sql(full=False), params)
    incremental_rows = _keyed(_final_rows(client))

    if full_rows != incremental_rows:
        differing = [
            k for k in full_rows if full_rows.get(k) != incremental_rows.get(k)
        ]
        raise AssertionError(
            "incremental refresh disagrees with the full rebuild "
            f"(rounded to {ROUND} dp) on {sorted(differing)[:5]}"
        )
    if incremental["written_rows"] >= full["written_rows"]:
        raise AssertionError(
            "incremental refresh wrote no fewer rows than the full rebuild "
            f"(incremental={incremental['written_rows']} >= "
            f"full={full['written_rows']}) — is the dirty-key filter still on both "
            "exposures_landed reads, and is the marker still being written after it?"
        )

    return {
        "granules": _granules(client),
        "total_keys": total_keys,
        "changed": len(changed),
        "dirty": len(dirty),
        "over_refresh": len(dirty - changed),
        "equal_sets": dirty == changed,
        "full": full,
        "incremental": incremental,
    }


_FINAL_ROWS = """
select
    campaign_id,
    hour,
    spend,
    exposures,
    attributed_conversions,
    purchases,
    site_visits,
    revenue
from campaign_hourly final
order by campaign_id, hour
"""


def _final_rows(client: Client) -> list[tuple]:
    return client.query(_FINAL_ROWS).result_rows


def _granules(client: Client) -> list[tuple]:
    """rows and marks per source table — the printout's evidence for why rows read
    cannot fall here. One granule is 8192 rows; a table inside one mark range has
    nothing for a key predicate to skip."""
    return client.query(
        "select table, sum(rows), sum(marks) from system.parts "
        "where active and table in ('exposures_landed', 'attributed_conversions') "
        "group by table order by table"
    ).result_rows


def format_report(m: dict) -> str:
    write_ratio = (
        f"{m['full']['written_rows'] / m['incremental']['written_rows']:.1f}x"
        if m["incremental"]["written_rows"]
        else "n/a"
    )
    lines = [
        "rollup refresh — full rebuild vs dirty-set refresh",
        "(same campaign_hourly FINAL rows, verified equal to 6 dp)",
        "",
        f"campaign_hourly FINAL rows identical (6dp): {m['total_keys']} keys",
        f"dirty set == changed set ({m['dirty']} keys)"
        if m["equal_sets"]
        else f"changed set ({m['changed']}) ⊆ dirty set ({m['dirty']})",
        f"over-refresh (dirty − changed): {m['over_refresh']} keys",
        "",
        f"rows written  full={m['full']['written_rows']}  "
        f"incremental={m['incremental']['written_rows']}  ({write_ratio} fewer)",
        "rows written incremental < full",
        "",
        f"rows read     full={m['full']['read_rows']}  "
        f"incremental={m['incremental']['read_rows']}  (printed, NOT asserted)",
        "  "
        + "; ".join(
            f"{t}: {rows} rows in {marks} marks" for t, rows, marks in m["granules"]
        ),
        "  a granule is 8192 rows — inside one, a dirty-key predicate has nothing to",
        "  prune and the dirty-key lookup itself reads, so the incremental refresh",
        "  reads MORE here. The read side needs a multi-granule table (bench_large,",
        "  BACKLOG); the write saving above is the structural one.",
    ]
    return "\n".join(lines)


def render(m: dict) -> str:
    """The docs/RESULTS.md block, regenerated verbatim by this command (the
    `cost-levers` / `scale-curve` pattern, so `make check-docs` guards it). Both
    numbers are stated — the write saving AND the read cost — with the mark counts
    that explain the second, so a reader cannot take the headline for a read win."""
    granules = "; ".join(
        f"`{t}` {rows:,} rows in {marks} marks" for t, rows, marks in m["granules"]
    )
    write_ratio = m["full"]["written_rows"] / max(m["incremental"]["written_rows"], 1)
    sets = (
        f"identical ({m['dirty']} keys)"
        if m["equal_sets"]
        else f"{m['changed']} changed ⊆ {m['dirty']} dirty"
    )
    return "\n".join(
        [
            _START,
            "",
            "_Measured by `make rollup-bench PROFILE=long_delay` after `make run`, on "
            f"a rollup of {m['total_keys']} `(campaign_id, hour)` keys of which the "
            f"reconcile pass changed {m['changed']}. Rows read/written are "
            "ClickHouse's own `X-ClickHouse-Summary`, both tables canonicalized to "
            "merged steady state first._",
            "",
            "| measure | full rebuild | dirty-set refresh | |",
            "|---|---|---|---|",
            f"| rows written | {m['full']['written_rows']:,} | "
            f"{m['incremental']['written_rows']:,} | "
            f"{write_ratio:.1f}× fewer — **asserted** (direction only) |",
            f"| rows read | {m['full']['read_rows']:,} | "
            f"{m['incremental']['read_rows']:,} | MORE — printed, **not** asserted |",
            "",
            f"**The write saving is the structural one.** Rewriting only the "
            f"{m['incremental']['written_rows']} changed keys instead of all "
            f"{m['total_keys']} is what an incremental rollup buys, and it is what "
            "stops `campaign_hourly` gaining a full copy per refresh — the un-merged "
            "part growth RUNBOOK incident #1 is about.",
            "",
            f"**The read side gets worse here, and the reason is size:** {granules}. A "
            "granule is 8,192 rows, so both tables sit inside ONE — a dirty-key "
            "predicate has nothing to skip, while the predicate's own subquery reads "
            "`rollup_dirty` once per `exposures_landed` branch. A read-side win needs "
            "a multi-granule table (`bench_large`, ≈7 granules of exposures); that "
            "measurement is a BACKLOG row for 18b, which already runs that profile. "
            "Asserting a read win from this profile would be claiming scale we do not "
            "run.",
            "",
            "_Unlike every other measured block in this file, the incremental **rows "
            "read** cell is NOT re-run stable (2,004 and 2,685 observed on the same "
            "data): it counts the dirty-key lookup, and `rollup_dirty` grows a row "
            "per key per refresh, so each run of this command makes the next one read "
            "a little more. The write, key and gate numbers above ARE stable across "
            "runs. Stated rather than smoothed — the cell is printed, never asserted, "
            "for this reason as well as the granule one._",
            "",
            f"**Dirty-set gate:** changed vs dirty above the refresh watermark — "
            f"{sets}, over-refresh {m['over_refresh']} keys. The contract is "
            "`changed ⊆ dirty` (a missed key serves a stale rollup while the "
            "full-refresh oracle still passes); equality is evidence, not the rule.",
            "",
            _END,
        ]
    )


def write_results(section: str) -> None:
    text = RESULTS_PATH.read_text()
    if _START not in text or _END not in text:
        raise AssertionError(
            f"{RESULTS_PATH} is missing the {_START} / {_END} markers — add the "
            "'Rollup refresh' section skeleton first."
        )
    head = text[: text.index(_START)]
    tail = text[text.index(_END) + len(_END) :]
    RESULTS_PATH.write_text(head + section + tail)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    args = parser.parse_args(argv)
    # Same shape as the destructive paths and `make eval`: validate the profile in the
    # process (it is never a path here — nothing is derived from it but the guard), then
    # refuse a database populated from a DIFFERENT profile (BACKLOG 43 marker).
    profile = validate_profile(args.profile)
    client = connect()
    apply_ddl(client)
    assert_profile_marker(db_profile_marker(client), profile)
    measured = run(client)
    write_results(render(measured))
    print(format_report(measured))
    print("\nwrote docs/RESULTS.md → 'Rollup refresh: full rebuild vs dirty set'")


if __name__ == "__main__":
    main()
