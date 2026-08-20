"""`make cost-levers` — three query-cost levers, each measured before/after.

Measures ClickHouse-native cost levers on the bench_large serving tables and
writes the result to docs/RESULTS.md. Reuses the Phase-7 honest harness from
queries/bench.py unchanged: `_canonicalize` (OPTIMIZE ... FINAL every read table,
so read_rows reflects merged steady state, not un-merged version-parts — the
RUNBOOK incident-#1 determinism fix) and `_measure` (median of _RUNS with the
query cache off, returning the server's X-ClickHouse-Summary read_rows/read_bytes,
which are cache-independent and deterministic).

The three levers, and why each needs the query it carries (the all-time
per-campaign report is already near-optimal for this schema — the levers win on
date-/dimension-scoped access, which is exactly when a platform reaches for them;
DECISIONS Phase 13):

  1. PROJECTION ordered by event_time on attributed_conversions. The base table is
     sorted by conversion_id, so event_time is scattered across every granule and a
     date-range predicate prunes nothing; the projection is an alternate physical
     ordering ClickHouse auto-picks for the range. WINS. (Non-FINAL: a projection
     can't serve FINAL; valid because the canonicalized table is single-version.)
  2. FINAL-avoidance / skip index — a DOCUMENTED NEGATIVE RESULT. Two candidates,
     both measured, both lose on this schema: (2a) explicit argMax(...) GROUP BY
     conversion_id reads MORE than SELECT ... FINAL (FINAL is already optimal on
     merged single-version data); (2b) a bloom skip index on a non-leading column
     (genre, and the 350x-more-selective ip) skips zero granules, because every
     non-key column is uniformly scattered under the (campaign_id, event_time, ...)
     sort — the blocker is physical clustering, not selectivity.
  3. PREWHERE the selective window predicate ahead of the wide-column read. WINS.
     Measured optimize_move_to_prewhere=0 (before) vs explicit PREWHERE (after) —
     against ClickHouse's auto-moved default there is no delta.

Determinism + honesty: canonicalize before measuring; magnitude-free direction
asserts (winners read fewer bytes; the negative-result candidates are asserted to
NOT prune, so a silent future change that made them help would fail the run and
flag the writeup stale); result rows asserted identical to 6 dp on every pair.
"""

from pathlib import Path

from clickhouse_connect.driver.client import Client

from clickhouse.client import connect
from queries.bench import _canonicalize, _measure, _round_row

SQL_PATH = Path(__file__).parent / "cost_levers.sql"
RESULTS_PATH = Path(__file__).parent.parent / "docs" / "RESULTS.md"
_START = "<!-- COST_LEVERS_START -->"
_END = "<!-- COST_LEVERS_END -->"


def _parse_blocks(text: str) -> dict[str, str]:
    """Split cost_levers.sql into `-- >>> name` … blocks (SQL comment lines dropped)."""
    blocks: dict[str, list[str]] = {}
    name = None
    for line in text.splitlines():
        marker = line.strip()
        if marker.startswith("-- >>> "):
            name = marker[len("-- >>> ") :].strip()
            blocks[name] = []
        elif name is not None and not marker.startswith("--"):
            blocks[name].append(line)
    return {k: "\n".join(v).strip() for k, v in blocks.items()}


def _rows_equal(a: dict, b: dict) -> bool:
    ar = [_round_row(r) for r in a["rows"]]
    br = [_round_row(r) for r in b["rows"]]
    return ar == br


# Pure direction predicates (magnitude-free), shared by measure() and the offline
# unit tests. A winning lever reads fewer bytes after; the negative-result
# candidates are asserted to NOT improve, so a silent future change that made them
# help fails the run and flags the RESULTS writeup stale.
def _reduces_bytes(before: dict, after: dict) -> bool:
    return after["read_bytes"] < before["read_bytes"]


def _pruned_rows(no_idx: dict, with_idx: dict) -> bool:
    return with_idx["read_rows"] < no_idx["read_rows"]


def _setup(client: Client, sql: dict[str, str]) -> None:
    """Add + materialize the lever objects (idempotent: drop then add). All off
    clickhouse/ddl.sql — never on the golden path."""
    client.command(sql["setup_projection"])  # deduplicate_merge_projection_mode
    for key in (
        "drop_projection",
        "add_projection",
        "materialize_projection",
        "drop_idx_genre",
        "add_idx_genre",
        "materialize_idx_genre",
        "drop_idx_ip",
        "add_idx_ip",
        "materialize_idx_ip",
    ):
        client.command(sql[key])


def measure(client: Client | None = None) -> dict:
    """Run every lever's before/after pair; assert direction (winners) and the
    negative results; return the measured numbers for the RESULTS writer."""
    client = client or connect()
    sql = _parse_blocks(SQL_PATH.read_text())
    _setup(client, sql)
    _canonicalize(client)  # merged steady state before ANY measurement

    out: dict = {}

    # Logical table sizes from count() on the canonicalized tables — the
    # intention-revealing, pruning-independent size headline. NOT a lever query's
    # read_rows, which equals the row count only because that side happens to prune
    # nothing (a fragile coupling exactly where the phase is about pruning).
    out["sizes"] = {
        t: client.query(f"select count() from {t} final").result_rows[0][0]
        for t in ("attributed_conversions", "exposures_landed")
    }
    # Highest-row-count shared-pool IP, chosen deterministically (ip tie-break) so the
    # skip-index probe carries no seed-pinned literal and re-runs identically.
    out["ip_value"] = client.query(
        "select ip from exposures_landed final "
        "group by ip order by count() desc, ip limit 1"
    ).result_rows[0][0]

    # ---- Lever 1: projection (non-FINAL), toggled by optimize_use_projections ----
    before = _measure(client, sql["lever1_query"], {"optimize_use_projections": 0})
    after = _measure(client, sql["lever1_query"], {"optimize_use_projections": 1})
    if not _rows_equal(before, after):
        raise AssertionError(f"lever1 rows differ: {before['rows']} vs {after['rows']}")
    if not _reduces_bytes(before, after):
        raise AssertionError(
            "lever1 (projection) did not reduce read_bytes: "
            f"after={after['read_bytes']} >= before={before['read_bytes']} — is the "
            "projection materialized and the query non-FINAL?"
        )
    out["lever1"] = {"before": before, "after": after}

    # ---- Lever 2a: FINAL vs argMax (negative — FINAL already optimal) ----
    final = _measure(client, sql["lever2a_final"], {"optimize_use_projections": 0})
    argmax = _measure(client, sql["lever2a_argmax"], {"optimize_use_projections": 0})
    if not _rows_equal(final, argmax):
        raise AssertionError(
            f"lever2a rows differ: {final['rows']} vs {argmax['rows']}"
        )
    if _reduces_bytes(final, argmax):
        raise AssertionError(
            "lever2a: argMax unexpectedly beat FINAL "
            f"({argmax['read_bytes']} < {final['read_bytes']}) — the FINAL-avoidance "
            "negative result is stale; re-examine and update RESULTS."
        )
    out["lever2a"] = {"final": final, "argmax": argmax}

    # ---- Lever 2b: bloom skip index (negative — no clustering, no prune) ----
    out["lever2b"] = {}
    for col in ("genre", "ip"):
        q = sql[f"lever2b_{col}"]
        if col == "ip":
            q = q.format(ip=out["ip_value"])
        no_idx = _measure(client, q, {"use_skip_indexes": 0})
        with_idx = _measure(client, q, {"use_skip_indexes": 1})
        if not _rows_equal(no_idx, with_idx):
            raise AssertionError(f"lever2b_{col} rows differ")
        if _pruned_rows(no_idx, with_idx):
            raise AssertionError(
                f"lever2b_{col}: the bloom index unexpectedly pruned "
                f"({with_idx['read_rows']} < {no_idx['read_rows']} rows) — the "
                "no-clustering negative result is stale; re-examine and update RESULTS."
            )
        out["lever2b"][col] = {"no_idx": no_idx, "with_idx": with_idx}

    # ---- Lever 3: PREWHERE (auto-move off both sides; explicit PREWHERE after) ----
    off = {"optimize_move_to_prewhere": 0, "optimize_use_projections": 0}
    where = _measure(client, sql["lever3_where"], off)
    prewhere = _measure(client, sql["lever3_prewhere"], off)
    if not _rows_equal(where, prewhere):
        raise AssertionError(
            f"lever3 rows differ: {where['rows']} vs {prewhere['rows']}"
        )
    if not _reduces_bytes(where, prewhere):
        raise AssertionError(
            "lever3 (PREWHERE) did not reduce read_bytes: "
            f"after={prewhere['read_bytes']} >= before={where['read_bytes']}"
        )
    out["lever3"] = {"before": where, "after": prewhere}
    return out


def _ratio(before: int, after: int) -> str:
    return f"{before / after:.2f}x" if after else "n/a"


def _pair_table(before: dict, after: dict, before_h: str, after_h: str) -> str:
    return "\n".join(
        [
            f"| measure | {before_h} | {after_h} | ratio |",
            "|---|---|---|---|",
            f"| rows read | {before['read_rows']:,} | {after['read_rows']:,} "
            f"| {_ratio(before['read_rows'], after['read_rows'])} |",
            f"| bytes read | {before['read_bytes']:,} | {after['read_bytes']:,} "
            f"| {_ratio(before['read_bytes'], after['read_bytes'])} |",
        ]
    )


def render(out: dict) -> str:
    """Build the deterministic RESULTS block (numbers only — cache-independent,
    canonicalized, byte-stable on re-run). Prose lives outside the markers."""
    l1b, l1a = out["lever1"]["before"], out["lever1"]["after"]
    l3b, l3a = out["lever3"]["before"], out["lever3"]["after"]
    f, a = out["lever2a"]["final"], out["lever2a"]["argmax"]
    g = out["lever2b"]["genre"]
    ip = out["lever2b"]["ip"]
    ip_val = out["ip_value"]
    # Size headline from count() — logical row count, pruning-independent.
    ac_n = out["sizes"]["attributed_conversions"]
    el_n = out["sizes"]["exposures_landed"]
    ac_gran, el_gran = round(ac_n / 8192), round(el_n / 8192)
    parts = [
        _START,
        "",
        "_Measured by `make cost-levers` on `bench_large` "
        f"(attributed_conversions {ac_n:,} rows ≈ {ac_gran} granules; "
        f"exposures_landed {el_n:,} ≈ {el_gran} granules). Both tables canonicalized "
        "to merged steady state first; rows/bytes are ClickHouse's cache-independent "
        "`X-ClickHouse-Summary`. Re-run byte-stable._",
        "",
        "**Lever 1 — projection ordered by `event_time` (WINS).** A date-scoped "
        "reporting slice over `attributed_conversions`. The base table is sorted by "
        "`conversion_id`, so `event_time` is scattered across every granule and the "
        "range predicate prunes nothing; the projection keeps an alternate copy "
        "ordered by `event_time` that ClickHouse auto-picks for the range.",
        "",
        _pair_table(l1b, l1a, "no projection", "projection"),
        "",
        "- _Why the bytes drop:_ the projection reads only the window's granules "
        "instead of the whole table. _Cost:_ a projection is a second physical copy "
        "of the table (more disk) maintained on every insert (slower writes). "
        "_Caveat:_ a projection can't serve a `FINAL` query — measured non-FINAL, "
        "valid because the canonicalized table is single-version, so FINAL and "
        "non-FINAL return identical rows here.",
        "",
        "**Lever 2 — FINAL-avoidance / skip index (DOCUMENTED NEGATIVE RESULT).** "
        "The schema does not reward a secondary lever here, and knowing when *not* to "
        "add one is the point. Two candidates measured, both lose:",
        "",
        "_2a — `SELECT ... FINAL` vs explicit `argMax(...) GROUP BY conversion_id`:_",
        "",
        _pair_table(f, a, "FINAL", "argMax GROUP BY"),
        "",
        "`argMax` reads MORE, not less: on merged single-version data `FINAL` reads "
        "only the columns it needs, while the manual collapse must scan "
        "`conversion_id`, `revenue`, `attributed`, and `processed_at` for every row "
        "and build a hash table. `FINAL` is already optimal — the version-part cost "
        "RUNBOOK incident #1 describes exists only *before* the merge, which "
        "`_canonicalize` (correctly) removes.",
        "",
        "_2b — bloom skip index on a non-leading column (`program_genre`, and the "
        f"far-more-selective `ip` — {ip['no_idx']['rows'][0][0]} of {el_n:,} rows):_",
        "",
        "| query | rows read, no index | rows read, bloom index | granules skipped |",
        "|---|---|---|---|",
        f"| `program_genre = 'sports'` | {g['no_idx']['read_rows']:,} "
        f"| {g['with_idx']['read_rows']:,} | 0 |",
        f"| `ip = '{ip_val}'` | {ip['no_idx']['read_rows']:,} "
        f"| {ip['with_idx']['read_rows']:,} | 0 |",
        "",
        "The index skips **zero** granules for either predicate — even the "
        "0.3%-selective `ip`. The blocker is physical clustering, not selectivity: "
        "`exposures_landed` is sorted `(campaign_id, event_time, exposure_id)`, so the "
        "leading key already prunes a campaign filter (a bloom on `campaign_id` would "
        "be redundant), and every non-key column is uniformly scattered across all "
        "granules — an `ip`'s rows sit in every granule, so no granule can be "
        "excluded. _The condition that would change it:_ physical clustering of the "
        "filtered column (a sort key that groups it, or naturally clustered data). "
        "_Cost of "
        "adding one anyway:_ write-time index maintenance and disk for a summary that "
        "prunes nothing.",
        "",
        "**Lever 3 — PREWHERE the window predicate (WINS).** A wide-column read behind "
        "the selective window filter.",
        "",
        _pair_table(l3b, l3a, "WHERE (no auto-move)", "PREWHERE"),
        "",
        "- _Why the bytes drop:_ `WHERE` (with `optimize_move_to_prewhere = 0`) reads "
        "every selected column for all scanned rows, then filters; `PREWHERE` reads "
        "the filter columns first and fetches the wide columns (the `assists` array, "
        "ids) "
        "only for surviving rows. Same rows read (the window doesn't prune granules "
        "without the projection), fewer bytes. _Cost:_ none structural — but measured "
        "against ClickHouse's default (which auto-moves the predicate already) the "
        "delta is zero, so this only 'wins' relative to an explicitly disabled move.",
        "",
        "_Honesty boundary: these are `bench_large` numbers; the mechanisms are the "
        "claim, not the magnitudes. All three win on **scoped** access (a date range, "
        "one dimension) — the all-time per-campaign report is already near-optimal for "
        "this schema (campaign is the leading sort key), which is exactly the setting "
        "where a platform reaches for these levers. The profile was not tuned to "
        "inflate any win; lever 2 is reported as the negative result it measured._",
        "",
        _END,
    ]
    return "\n".join(parts)


def write_results(section: str) -> None:
    text = RESULTS_PATH.read_text()
    if _START not in text or _END not in text:
        raise AssertionError(
            f"{RESULTS_PATH} is missing the {_START} / {_END} markers — add the "
            "'Query cost levers' section skeleton first."
        )
    head = text[: text.index(_START)]
    tail = text[text.index(_END) + len(_END) :]
    RESULTS_PATH.write_text(head + section + tail)


def main() -> None:
    out = measure()
    write_results(render(out))
    print("cost levers measured (bench_large) — wrote docs/RESULTS.md:")
    print(
        f"  lever 1 (projection): {out['lever1']['before']['read_bytes']:,} -> "
        f"{out['lever1']['after']['read_bytes']:,} bytes  [WINS]"
    )
    print(
        f"  lever 2a (FINAL vs argMax): {out['lever2a']['final']['read_bytes']:,} vs "
        f"{out['lever2a']['argmax']['read_bytes']:,} bytes  [negative — FINAL optimal]"
    )
    print(
        "  lever 2b (bloom skip index): 0 granules skipped  [negative — no clustering]"
    )
    print(
        f"  lever 3 (PREWHERE): {out['lever3']['before']['read_bytes']:,} -> "
        f"{out['lever3']['after']['read_bytes']:,} bytes  [WINS]"
    )


if __name__ == "__main__":
    main()
