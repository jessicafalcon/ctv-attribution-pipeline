"""Phase-6 reconciliation — the second attribution path.

The hot engine keeps only a 7-day window, so a conversion whose causing exposure
is more than 7 days earlier in event-time is emitted unattributed (its exposure
was evicted before the conversion released). That exposure still lives in
`exposures_landed` (landed regardless of hot eviction), so a periodic batch job
recovers the conversion by re-running the SAME last-touch decision over the long
(90-day) window — closing the long-window tail without keeping 90 days of engine
state.

Reuses `streaming.attribute.last_touch` (the exact leaf the hot engine uses) at
a 90d window, so hot and reconciled decisions cannot diverge. It only rewrites
hot-*unattributed* rows (attributed=0, path=hot); a hot-attributed row is never
re-opened — re-attributing it over 90d yields the same last-touch exposure but
would flip its `path` for no reason. Corrected rows carry `path=reconciled` and a
version (`processed_at`) strictly above the hot row's, so ReplacingMergeTree
FINAL keeps the correction.

Two kinds of candidate (Phase 16):
- **state-miss** (`candidate_count == 1`): the household is certain; the causing
  exposure aged out of the 7d hot window. Re-run the leaf over 90d.
- **ambiguous_ip** (`candidate_count > 1`): a shared-IP conversion the hot path
  refused to guess. `expand_candidates` explodes the row's persisted
  `candidate_households` (the full set the engine saw at deferral time — Phase
  17; no device graph, no broker), the leaf scores
  each household, and `pick_household` applies the most-recent-exposure rule
  (ties: `exposure_id`, then `household_id`) — the ONE implementation of that
  tiebreak, moved here from the deleted hot-path reduce. Reconciliation owns it
  because it holds every exposure; a late correct credit beats a fast wrong one.

Determinism: a pure function of ClickHouse FINAL state, the fixed window, and a
data-derived `reconciled_at` (no wall clock) — so a replay/re-run converges.
"""

import argparse
from datetime import UTC, datetime, timedelta

from clickhouse_connect.driver.client import Client
from prometheus_client import REGISTRY, start_http_server, write_to_textfile

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from lake.land_attributed import land_attributed
from producer.models import AttributedConversion, Exposure, ResolvedConversion
from reconcile import metrics, rollup
from reconcile.sources import ClickHouseExposureSource, ExposureSource
from streaming.attribute import (
    Candidate,
    candidate_households_by_conversion,
    last_touch,
)

# The long window: exposures up to 90 days before a conversion are eligible
# (ARCHITECTURE §3.4). This is the reconciliation counterpart to the engine's
# 7-day HOT_WINDOW.
LONG_WINDOW = timedelta(days=90)

# reconciled_at = max(ingest_time over the fixed serving state) + this delta.
# processed_at is DateTime64(3) (ms), so a 1s gap is comfortably strictly-greater
# than every hot processed_at (each ≤ that max). Documented constant, not a magic
# literal (DECISIONS Phase 6). The same delta is the report_snapshots post-pass
# offset (rollup computes reported_at server-side as max(ingest_time) + offset).
RECONCILE_DELTA_MS = 1000
RECONCILE_DELTA = timedelta(milliseconds=RECONCILE_DELTA_MS)

_CANDIDATE_COLS = (
    "conversion_id, event_time, ingest_time, device_id, ip, conversion_type, "
    "revenue, order_id, household_id, resolution, ambiguous, candidate_count, "
    "exposure_id, assists, attributed, path, processed_at, reason, "
    "candidate_households"
)


class PrePhase17RowError(ValueError):
    """An ambiguous hot row with no persisted candidate set (written before the
    Phase-17 column). Reconciliation cannot explode it; the DB must be
    re-populated from the engine."""


def reconciled_at_for(base: datetime) -> datetime:
    """The reconciliation-pass version: `base + RECONCILE_DELTA`. `base` is the
    max ingest_time over the fixed serving state (see `_max_ingest`), so this is
    data-derived, stable across re-runs, and strictly above every hot version."""
    return base + RECONCILE_DELTA


def pick_household(candidates: list[Candidate]) -> Candidate | None:
    """The cross-household tiebreak for an ambiguous conversion: among the
    ATTRIBUTED candidates keep the most-recent last-touch exposure —
    `(last_touch_time, exposure_id)` is already a total order (exposure_id is
    globally unique), `household_id` a vestigial final tiebreak. None if no
    candidate household has an in-window exposure (the conversion stays
    unattributed). Moved verbatim from the hot path's deleted
    `reduce_conversion` (Phase 16) — it is implemented here and nowhere else."""
    attributed = [c for c in candidates if c.row.attributed]
    if not attributed:
        return None
    return max(
        attributed,
        key=lambda c: (c.last_touch_time, c.row.exposure_id, c.row.household_id),
    )


def expand_candidates(
    candidates: list[AttributedConversion],
) -> list[ResolvedConversion]:
    """One row per candidate HOUSEHOLD. A state-miss row passes through; an
    ambiguous_ip placeholder is exploded into one row per entry of its persisted
    `candidate_households` — the exact set the engine saw at deferral time
    (sorted, deterministic; the model validator guarantees it is non-empty and
    contains the placeholder). No device graph, no broker (Phase 17). Batch
    fan-out here is the point: the architecture's claim is no fan-out on the HOT
    path; with the full 90-day picture it is cheap and correct."""
    base_fields = set(ResolvedConversion.model_fields)
    out: list[ResolvedConversion] = []
    for conv in candidates:
        base = conv.model_dump(include=base_fields)
        if conv.candidate_count > 1:
            out.extend(
                ResolvedConversion(**{**base, "household_id": h})
                for h in conv.candidate_households
            )
        else:
            out.append(ResolvedConversion(**base))
    return out


def reconcile(
    candidates: list[ResolvedConversion],
    exposures_by_household: dict[str, list[Exposure]],
    window: timedelta,
    reconciled_at: datetime,
) -> list[AttributedConversion]:
    """Pure matcher over EXPANDED candidates (see `expand_candidates`). Rows are
    grouped by `conversion_id`: one row → the leaf over its household at the
    long `window`; several rows (an ambiguous conversion's candidate households)
    → the leaf per household, then `pick_household`. Emit ONLY the conversions
    that now match, re-stamped `path=reconciled` with the `reconciled_at`
    version. A conversion still without an in-window exposure is NOT emitted —
    it stays as its hot unattributed row, so a second pass re-selects it and
    changes nothing (idempotent)."""
    groups: dict[str, list[ResolvedConversion]] = {}
    for conv in candidates:
        groups.setdefault(conv.conversion_id, []).append(conv)
    # The exploded rows ARE the candidate set; the recovered row keeps it.
    by_cid = candidate_households_by_conversion(candidates)
    recovered: list[AttributedConversion] = []
    for cid, rows in groups.items():
        if len(rows) == 1 and rows[0].candidate_count > 1:
            raise ValueError(
                f"{cid}: ambiguous_ip candidate not expanded — call "
                "expand_candidates(candidates) first"
            )
        scored = [
            last_touch(
                exposures_by_household.get(r.household_id, []),
                r,
                window,
                by_cid.get(cid, []),
            )
            for r in rows
        ]
        winner = pick_household(scored)
        if winner is not None:
            recovered.append(
                winner.row.model_copy(
                    update={"path": "reconciled", "processed_at": reconciled_at}
                )
            )
    return recovered


def _read_candidates(client: Client) -> list[AttributedConversion]:
    """Hot-unattributed rows only (attributed=0 AND path='hot') from FINAL, as
    the full hot row — both `reason` values, state_miss and ambiguous_ip, and the
    `candidate_households` the ambiguous ones are exploded over. The `reason`
    column is the explicit contract; a NON-NULL value is asserted to agree with
    `candidate_count` (a mismatch would mean a writer bypassed the engine's
    `_attributed`). NULL is accepted: rows written before the Phase-16 additive
    migration carry NULL until the next engine pass rewrites them. An ambiguous
    row whose `candidate_households` is empty was written before the Phase-17
    column existed and can never be exploded: refused loud, naming the fix
    (re-populate with `make run`) — the eval_meta-guard standard, not a bare
    pydantic error. Never reads the accuracy side file."""
    rows = client.query(
        f"select {_CANDIDATE_COLS} from attributed_conversions final "
        "where attributed = 0 and path = 'hot' order by conversion_id"
    ).result_rows
    for r in rows:
        expected = "ambiguous_ip" if r[11] > 1 else "state_miss"
        if r[17] is not None and r[17] != expected:
            raise ValueError(
                f"{r[0]}: reason={r[17]!r} disagrees with candidate_count={r[11]}"
            )
        if r[11] > 1 and not r[18]:
            raise PrePhase17RowError(
                f"{r[0]}: ambiguous (candidate_count={r[11]}) but "
                "candidate_households is empty — this DB predates Phase 17; "
                "re-populate it (make run / make run-hot) before reconciling"
            )
    return [
        AttributedConversion(
            conversion_id=r[0],
            event_time=r[1],
            ingest_time=r[2],
            device_id=r[3],
            ip=r[4],
            conversion_type=r[5],
            revenue=r[6],
            order_id=r[7],
            household_id=r[8],
            resolution=r[9],
            ambiguous=bool(r[10]),
            candidate_count=r[11],
            exposure_id=r[12],
            assists=list(r[13]),
            attributed=bool(r[14]),
            path=r[15],
            processed_at=r[16],
            reason=r[17],
            candidate_households=list(r[18]),
        )
        for r in rows
    ]


def _max_ingest(client: Client) -> datetime:
    """max(ingest_time) over the fixed serving state — the union of both landed
    tables. Fixed input set (independent of which rows get recovered), so
    `reconciled_at` is identical on a re-run and the job converges.

    Read as an epoch-millis integer, not a datetime: clickhouse-connect renders a
    DateTime column in the client's local timezone, so the same stored instant
    comes back at different wall-clocks across processes — which stamped the same
    `base` onto different `reported_at`s (a determinism break). An integer carries
    no timezone; rebuilding it as UTC here (and casting the write under 'UTC', see
    rollup) makes the whole round-trip context-independent."""
    epoch_ms = client.query(
        "select toUnixTimestamp64Milli(max(t)) from ("
        "select max(ingest_time) as t from exposures_landed final "
        "union all "
        "select max(ingest_time) as t from attributed_conversions final)"
    ).result_rows[0][0]
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)


def _restatement_abs_delta(client: Client) -> float:
    """Largest absolute per-campaign ROAS change between this pass's pre/post
    snapshots: `max |roas_now − roas_as_reported|`. Same argMin/argMax-over-
    reported_at collapse as queries/restatement.sql (the two reported_at values
    this pass are pre offset 0 and post offset RECONCILE_DELTA_MS). NULL ROAS
    (zero-spend campaigns) drop out of the max; no campaigns → 0.0. Backs the
    RestatementMagnitude alert."""
    rows = client.query(
        "select coalesce(max(abs(d)), 0) from ("
        "select argMax(roas, reported_at) - argMin(roas, reported_at) as d "
        "from report_snapshots final group by campaign_id, period)"
    ).result_rows
    return float(rows[0][0])


def recover(
    candidates: list[AttributedConversion],
    exposure_source: ExposureSource,
    window: timedelta,
    reconciled_at: datetime,
) -> list[AttributedConversion]:
    """Recover the given candidates: explode ambiguous ones over their persisted
    candidate households, read every candidate household's exposures from the
    source, run the matcher, return the recovered rows — WITHOUT writing them.
    The caller lands them in the lake (`land_attributed`) and loads the touched
    days into ClickHouse (Phase 17). The recovery half of a pass, shared by
    `run` (all candidates, ClickHouse source) and the Dagster day-partitioned
    asset (one day's candidates, Iceberg source). `reconciled_at` is passed in
    (not recomputed) so every day of a partitioned run stamps the same version —
    identical to a single full pass."""
    expanded = expand_candidates(candidates)
    exposures = exposure_source.read_for(expanded, window)
    return reconcile(expanded, exposures, window, reconciled_at)


def finalize(client: Client) -> None:
    """Global finalize, run once per reconciliation (not per day): snapshot the
    pre/post reports and refresh the rollup. reported_at is computed server-side
    as max(ingest_time) + offset, so both snapshots are order-independent and
    identical no matter which process writes them — the PRE report as of the hot
    pass (offset 0; path='hot' only, invariant under reconciliation) and the POST
    report (offset RECONCILE_DELTA_MS, both paths)."""
    rollup.write_report_snapshot(client, 0, hot_only=True)
    rollup.write_report_snapshot(client, RECONCILE_DELTA_MS, hot_only=False)
    rollup.refresh_campaign_hourly(client, RECONCILE_DELTA_MS)


def run(
    client: Client | None = None,
    exposure_source: ExposureSource | None = None,
) -> dict[str, int]:
    """One reconciliation pass: recover ALL hot misses (state-miss and
    ambiguous_ip) over the long window, land the corrections in the lake, load
    the touched days into ClickHouse, then finalize (snapshots + rollup refresh).
    Returns counts for logging/tests.

    `exposure_source` selects where the candidate households' exposures are read
    from: the default ClickHouse source keeps `make run` byte-identical; the
    Dagster asset passes an Iceberg/DuckDB source (Phase 12). No broker: the
    candidate households come from the rows themselves (Phase 17). Everything
    else — candidates, snapshots, rollup, insert — stays on ClickHouse."""
    apply_ddl()
    client = client or connect()
    exposure_source = exposure_source or ClickHouseExposureSource(client)

    # Imported here: the offline suites import this module without Dagster.
    from orchestration.load import materialize_load

    reconciled_at = reconciled_at_for(_max_ingest(client))
    candidates = _read_candidates(client)
    recovered = recover(candidates, exposure_source, LONG_WINDOW, reconciled_at)
    materialize_load(land_attributed(recovered))
    finalize(client)

    still_missing = len(candidates) - len(recovered)
    metrics.CANDIDATES.inc(len(candidates))
    metrics.RECOVERED.inc(len(recovered))
    metrics.STILL_MISSING.inc(still_missing)
    metrics.observe_restatement(_restatement_abs_delta(client))
    return {
        "candidates": len(candidates),
        "recovered": len(recovered),
        "still_missing": still_missing,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Phase-6 reconciliation pass")
    parser.add_argument("--metrics-port", type=int, default=None)
    parser.add_argument(
        "--metrics-out",
        default=None,
        help="dump this stage's terminal Prometheus registry to a textfile "
        "(promtool-fixture provenance; see make metrics-capture)",
    )
    args = parser.parse_args(argv)
    if args.metrics_port:
        start_http_server(args.metrics_port, addr="127.0.0.1")
    counts = run()
    print(
        f"reconcile: {counts['candidates']} candidates → "
        f"{counts['recovered']} recovered, {counts['still_missing']} still missing "
        f"(path=reconciled; report_snapshots pre/post written)"
    )
    if args.metrics_out:
        write_to_textfile(args.metrics_out, REGISTRY)


if __name__ == "__main__":
    main()
