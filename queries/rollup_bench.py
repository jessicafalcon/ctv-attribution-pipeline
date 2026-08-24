"""`make rollup-bench` — full rollup rebuild vs dirty-set refresh (Phase 18a).

Run after `make run PROFILE=<p>` on a profile whose reconcile pass changed something
(`long_delay` is the lighter gate profile; `bench_large` is the recorded
multi-granule profile — the committed RESULTS block is its measurement). Three
things come out of one pass:

  equality  — the dirty-set refresh leaves `campaign_hourly FINAL` identical to what a
              full rebuild leaves, to 6 dp. The full rebuild is the ORACLE; an
              incremental refresh that changed an answer is a bug, not a saving.
  cost      — rows read and rows written, full vs incremental, as measured by
              ClickHouse's own summary. The direction ASSERT is on rows WRITTEN
              (magnitude-free, Phase 7/13 precedent): rewriting only the changed keys
              is what an incremental rollup buys, and it is what keeps campaign_hourly
              from gaining a full copy per refresh (RUNBOOK incident #1's real cost).
              Rows read are PRINTED, not asserted, and the printout derives its own
              read-side verdict from the measured granule counts. On a single-granule
              table (`long_delay`, exposures_landed in one 8192-row granule) a
              dirty-key predicate has nothing to prune. On a multi-granule table
              (`bench_large`, ≈7 granules of exposures) it CAN prune only if the dirty
              keys leave whole granules untouched — measured NOT to: reconciliation's
              shared-IP deferrals spread across every campaign and hour, so the dirty
              set covers almost every `(campaign_id, hour)` key and at least one falls
              in every granule's range (BACKLOG 73, DECISIONS fix/rollup-bench-read).
              A read assert would claim a saving the data does not show.
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
seam the PRE report snapshot already uses. The keys that differ between those two
rebuilds are the keys the reconcile pass changed, and they are the key set the
pipeline's own second-pass refresh runs over (`orchestration.run.materialize_load`
refreshes the keys each load touched — Phase 18a).

The cost half therefore measures `reconcile.rollup.refresh_sql` — the pipeline's own
statement — twice: once full, once with exactly those keys bound. No marker is faked
and no watermark is moved; earlier revisions of this tool seeded their own marker row
to construct a scenario the pipeline did not run, which made the saving look like a
pipeline result when it was not (review gate, Phase 18a). Nothing here changes a served
number: both refreshes run at the pipeline's own offset, so the rows they insert are
byte-identical twins of the ones already there.
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

RESULTS_PATH = Path(__file__).parent.parent / "docs" / "RESULTS.md"
_START = "<!-- ROLLUP_BENCH_START -->"
_END = "<!-- ROLLUP_BENCH_END -->"

# The keys the reconcile pass's reload refreshed: rollup_refreshed rows stamped at a
# version that only a reconciled row carries (path='reconciled' processed_at). Read
# from the pipeline's own bookkeeping — this tool never writes it.
_RECONCILE_TOUCHED = """
select
    campaign_id,
    hour,
    toUnixTimestamp(hour) as hour_s
from rollup_refreshed final
where version >=
(
    select min(processed_at) from attributed_conversions final where path = 'reconciled'
)
order by
    campaign_id,
    hour
"""


def _keyed(rows: list[tuple]) -> dict[tuple, tuple]:
    """(campaign_id, hour) → the rounded aggregate for that key."""
    return {(r[0], r[1]): round_row(r[2:]) for r in rows}


def _merged(incremental: dict, served: dict) -> dict:
    """The served rollup with the incremental refresh's rows applied — what
    `campaign_hourly FINAL` would hold after that refresh. Comparing THIS to the full
    rebuild is the honest equality check: the incremental run only recomputes its own
    keys, so comparing its output alone to a whole-table rebuild compares two
    different questions."""
    return {**served, **incremental}


def _measure_into_scratch(client: Client, table: str, sql: str, params: dict) -> dict:
    """Run a refresh into a throwaway table with `campaign_hourly`'s exact structure
    and report what it read, wrote and produced. The live rollup is never written by
    this tool: the oracle and the thing under test must not share a medium."""
    client.command(f"drop table if exists {table}")
    client.command(f"create table {table} as campaign_hourly")
    try:
        measured = _measure_refresh(
            client,
            sql.replace("insert into campaign_hourly", f"insert into {table}", 1),
            params,
        )
        measured["rows"] = _keyed(_final_rows(client, table))
        return measured
    finally:
        client.command(f"drop table if exists {table}")


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
    if not changed:
        raise AssertionError(
            "rollup-bench: the reconcile pass changed no rollup key, so the dirty-set "
            "gate would pass vacuously — run it on a profile whose reconciliation "
            "restates something (long_delay)"
        )

    # The dirty set the pipeline's own second-pass refresh ran over: the keys the
    # reconcile load stamped in rollup_refreshed at the reconciled version. Read from
    # the tables, never reconstructed from a fake watermark.
    touched = client.query(_RECONCILE_TOUCHED).result_rows
    dirty = {(r[0], r[1]) for r in touched}
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
    # Both refreshes write to their OWN scratch table, never to the live rollup: the
    # oracle must not share a medium with the thing it checks. The earlier revision
    # ran both into `campaign_hourly` — and since a row's version is now data-derived,
    # identical content lands identical rows, so the ReplacingMergeTree would collapse
    # the two runs and the "identical (6dp)" line could have been reading the FULL
    # rebuild's rows back as the incremental one's (review gate, round 2).
    keys = sorted((r[0], int(r[2])) for r in touched)
    full = _measure_into_scratch(
        client, "_rollup_bench_full", rollup.refresh_sql(full=True), {}
    )
    incremental = _measure_into_scratch(
        client,
        "_rollup_bench_incremental",
        rollup.refresh_sql(full=False),
        {"keys": keys},
    )

    served = _keyed(_final_rows(client))
    after_incremental = _merged(incremental["rows"], served)
    if full["rows"] != after_incremental:
        differing = sorted(
            k for k in full["rows"] if full["rows"].get(k) != after_incremental.get(k)
        )
        raise AssertionError(
            "the incremental refresh, applied to the served rollup, disagrees with a "
            f"full rebuild (rounded to {ROUND} dp) on {differing[:5]}"
        )
    if incremental["written_rows"] >= full["written_rows"]:
        raise AssertionError(
            "incremental refresh wrote no fewer rows than the full rebuild "
            f"(incremental={incremental['written_rows']} >= "
            f"full={full['written_rows']}) — is the dirty-key filter still on both "
            "exposures_landed reads?"
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
from {table} final
order by campaign_id, hour
"""


def _final_rows(client: Client, table: str = "campaign_hourly") -> list[tuple]:
    return client.query(_FINAL_ROWS.format(table=table)).result_rows


def _granules(client: Client) -> list[tuple]:
    """rows and marks per source table — the printout's evidence for the read-side
    verdict. One granule is 8192 rows and reads back as one mark plus a boundary mark,
    so marks > 2 means the table spans more than one granule (`_read_finding` keys the
    read outcome off this)."""
    return client.query(
        "select table, sum(rows), sum(marks) from system.parts "
        "where active and table in ('exposures_landed', 'attributed_conversions') "
        "group by table order by table"
    ).result_rows


def _multi_granule(m: dict) -> bool:
    """True when a source table spans more than one granule. ClickHouse records one
    mark per granule plus a final boundary mark, so >2 marks == >1 granule (a
    single 8192-row granule reads back as 2 marks)."""
    return any(marks > 2 for _, _, marks in m["granules"])


def _read_finding(m: dict) -> tuple[str, str]:
    """(verdict, mechanism) for the read side, derived from the measured numbers —
    so the report reads honestly for whatever profile generated it. Three cases:
    a read win (the dirty-key predicate pruned granules), reads unchanged on a
    single-granule table (nothing to prune), reads unchanged on a multi-granule
    table (the dirty keys span every granule, so no mark range can be skipped)."""
    full_r = m["full"]["read_rows"]
    inc_r = m["incremental"]["read_rows"]
    if inc_r < full_r:
        return (
            "read win",
            f"The dirty-key predicate `(campaign_id, hour)` prunes granules the full "
            f"rebuild scans: incremental reads {inc_r:,} vs {full_r:,}, because the "
            f"dirty set touches only some of exposures_landed's granules and the "
            f"leading sort key `(campaign_id, event_time, …)` lets ClickHouse skip "
            f"the rest.",
        )
    if not _multi_granule(m):
        return (
            "unchanged (single granule)",
            "Both source tables sit inside ONE 8,192-row granule, so a dirty-key "
            "predicate has nothing to skip and the incremental refresh reads exactly "
            "what the full rebuild reads.",
        )
    return (
        "unchanged (multi-granule)",
        f"Even though the source tables span multiple granules, the dirty set is "
        f"{m['dirty']} of {m['total_keys']} `(campaign_id, hour)` keys — the shared-IP "
        f"deferrals reconciliation recovers are spread across every campaign and hour, "
        f"so at least one dirty key falls in every granule's key range and the "
        f"predicate skips no marks. Granule pruning needs the dirty keys ABSENT from "
        f"whole granules; here they are everywhere.",
    )


def format_report(m: dict) -> str:
    write_ratio = (
        f"{m['full']['written_rows'] / m['incremental']['written_rows']:.1f}x"
        if m["incremental"]["written_rows"]
        else "n/a"
    )
    verdict, mechanism = _read_finding(m)
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
        f"incremental={m['incremental']['read_rows']}  "
        f"({verdict}; printed, NOT asserted)",
        "  "
        + "; ".join(
            f"{t}: {rows} rows in {marks} marks" for t, rows, marks in m["granules"]
        ),
        "  " + mechanism,
    ]
    return "\n".join(lines)


def render(m: dict) -> str:
    """The docs/RESULTS.md block, regenerated verbatim by this command (the
    `cost-levers` / `scale-curve` pattern, so `make check-docs` guards it). Both
    numbers are stated — the write saving AND the read outcome — with the mark counts
    and a granule-derived mechanism, so the block reads honestly for whatever profile
    generated it: a reader cannot take the headline for a read win, nor read a
    single-granule caveat on a multi-granule run."""
    granules = "; ".join(
        f"`{t}` {rows:,} rows in {marks} marks" for t, rows, marks in m["granules"]
    )
    write_ratio = m["full"]["written_rows"] / max(m["incremental"]["written_rows"], 1)
    sets = (
        f"identical ({m['dirty']} keys)"
        if m["equal_sets"]
        else f"{m['changed']} changed ⊆ {m['dirty']} dirty"
    )
    verdict, mechanism = _read_finding(m)
    read_cell = (
        "fewer — printed, **not** asserted"
        if m["incremental"]["read_rows"] < m["full"]["read_rows"]
        else "unchanged — printed, **not** asserted"
    )
    return "\n".join(
        [
            _START,
            "",
            f"_Measured by `make rollup-bench PROFILE={m['profile']}` after "
            f"`make run`, on a rollup of {m['total_keys']} `(campaign_id, hour)` keys "
            f"of which the "
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
            f"{m['incremental']['read_rows']:,} | {read_cell} |",
            "",
            f"**The write saving is the structural one.** Rewriting only the "
            f"{m['incremental']['written_rows']} changed keys instead of all "
            f"{m['total_keys']} is what an incremental rollup buys, and it is what "
            "stops `campaign_hourly` gaining a full copy per refresh — the un-merged "
            "part growth RUNBOOK incident #1 is about.",
            "",
            f"**Rows read — {verdict}: {granules}.** {mechanism} The saving here is "
            "entirely in what is WRITTEN.",
            "",
            "**Dirty-set gate:** the keys reconciliation changed vs the keys the "
            "pipeline's own second-pass refresh recomputed — "
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
    # Marker check BEFORE apply_ddl: applying runs the report_snapshots migration and
    # this tool then INSERTs into campaign_hourly, so a wrong-profile invocation must
    # refuse before it writes anything, not after (review gate).
    assert_profile_marker(db_profile_marker(client), profile)
    apply_ddl(client)
    measured = run(client)
    measured["profile"] = profile
    write_results(render(measured))
    print(format_report(measured))
    print("\nwrote docs/RESULTS.md → 'Rollup refresh: full rebuild vs dirty set'")


if __name__ == "__main__":
    main()
