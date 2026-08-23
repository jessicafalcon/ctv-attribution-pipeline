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
    print(format_report(run(client)))


if __name__ == "__main__":
    main()
