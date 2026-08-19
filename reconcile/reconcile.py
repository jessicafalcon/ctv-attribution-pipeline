"""Phase-6 reconciliation — the second attribution path.

The hot engine keeps only a 7-day window, so a conversion whose causing exposure
is more than 7 days earlier in event-time is emitted unattributed (its exposure
was evicted before the conversion released). That exposure still lives in
`exposures_landed` (landed regardless of hot eviction), so a periodic batch job
recovers the conversion by re-running the SAME last-touch decision over the long
(90-day) window — closing the long-window tail without keeping 90 days of engine
state.

Reuses `streaming.attribute.attribute_household` (the exact leaf the hot engine
uses) at a 90d window, so hot and reconciled decisions cannot diverge. It only
rewrites hot-*unattributed* rows (attributed=0, path=hot); a hot-attributed row
is never re-opened — re-attributing it over 90d yields the same last-touch
exposure but would flip its `path` for no reason. Corrected rows carry
`path=reconciled` and a version (`processed_at`) strictly above the hot row's, so
ReplacingMergeTree FINAL keeps the correction.

Determinism: a pure function of ClickHouse FINAL state, the fixed window, and a
data-derived `reconciled_at` (no wall clock) — so a replay/re-run converges.
"""

import argparse
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from clickhouse_connect.driver.client import Client
from prometheus_client import start_http_server

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from producer.models import AttributedConversion, Exposure, ResolvedConversion
from reconcile import metrics, rollup
from streaming.attribute import attribute_household
from streaming.sink import insert_attributed

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
    "revenue, order_id, household_id, resolution, ambiguous, candidate_count"
)
_EXPOSURE_COLS = (
    "exposure_id, event_time, ingest_time, campaign_id, household_id, ip, "
    "app_id, program_genre, spend"
)


def reconciled_at_for(base: datetime) -> datetime:
    """The reconciliation-pass version: `base + RECONCILE_DELTA`. `base` is the
    max ingest_time over the fixed serving state (see `_max_ingest`), so this is
    data-derived, stable across re-runs, and strictly above every hot version."""
    return base + RECONCILE_DELTA


def reconcile(
    candidates: list[ResolvedConversion],
    exposures_by_household: dict[str, list[Exposure]],
    window: timedelta,
    reconciled_at: datetime,
) -> list[AttributedConversion]:
    """Pure matcher. For each hot-unattributed candidate, re-run the last-touch
    leaf over its household's exposures at the long `window`; emit ONLY the ones
    that now match, re-stamped `path=reconciled` with the `reconciled_at` version.
    A candidate still without an in-window exposure is NOT emitted — it stays as
    its hot unattributed row, so a second pass re-selects it and changes nothing
    (idempotent). Matching happens in the household the hot reduction settled on;
    the shared-IP fan-out already collapsed on the hot path (no re-fan-out)."""
    recovered: list[AttributedConversion] = []
    for conv in candidates:
        exposures = exposures_by_household.get(conv.household_id, [])
        candidate = attribute_household(exposures, [conv], window)[0]
        if candidate.row.attributed:
            recovered.append(
                candidate.row.model_copy(
                    update={"path": "reconciled", "processed_at": reconciled_at}
                )
            )
    return recovered


def _read_candidates(client: Client) -> list[ResolvedConversion]:
    """Hot-unattributed rows only (attributed=0 AND path='hot') from FINAL,
    reconstructed as ResolvedConversion. Never reads the accuracy side file."""
    rows = client.query(
        f"select {_CANDIDATE_COLS} from attributed_conversions final "
        "where attributed = 0 and path = 'hot' order by conversion_id"
    ).result_rows
    return [
        ResolvedConversion(
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
        )
        for r in rows
    ]


def _read_exposures_for(
    client: Client, household_ids: set[str]
) -> dict[str, list[Exposure]]:
    """Bulk-load the candidate households' exposures from FINAL in ONE query and
    group in memory (not an N+1 per-candidate read; the leaf window-filters). The
    long-window filter is left to `attribute_household`, so all of a household's
    exposures are loaded — fine at profile scale; a per-household window predicate
    is the scaling move (SCALING)."""
    if not household_ids:
        return {}
    rows = client.query(
        f"select {_EXPOSURE_COLS} from exposures_landed final "
        "where household_id in {hhs:Array(String)} order by exposure_id",
        parameters={"hhs": sorted(household_ids)},
    ).result_rows
    by_household: dict[str, list[Exposure]] = defaultdict(list)
    for r in rows:
        by_household[r[4]].append(
            Exposure(
                exposure_id=r[0],
                event_time=r[1],
                ingest_time=r[2],
                campaign_id=r[3],
                household_id=r[4],
                ip=r[5],
                app_id=r[6],
                program_genre=r[7],
                spend=r[8],
            )
        )
    return by_household


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


def run(client: Client | None = None) -> dict[str, int]:
    """One reconciliation pass: snapshot the pre-reconciliation report, recover
    the hot misses over the long window, then refresh the rollup and snapshot the
    post-reconciliation report. Returns counts for logging/tests."""
    apply_ddl()
    client = client or connect()

    base = _max_ingest(client)
    reconciled_at = reconciled_at_for(base)

    candidates = _read_candidates(client)
    exposures = _read_exposures_for(client, {c.household_id for c in candidates})
    recovered = reconcile(candidates, exposures, LONG_WINDOW, reconciled_at)
    if recovered:
        insert_attributed(client, recovered)

    # Snapshots (reported_at computed server-side as max(ingest_time) + offset, so
    # both are order-independent AND identical no matter which process writes them):
    # the PRE report as of the hot pass (offset 0; path='hot' only — invariant under
    # reconciliation), and the POST report (offset RECONCILE_DELTA_MS, both paths).
    # Refresh the rollup once, at the current (post) version.
    rollup.write_report_snapshot(client, 0, hot_only=True)
    rollup.write_report_snapshot(client, RECONCILE_DELTA_MS, hot_only=False)
    rollup.refresh_campaign_hourly(client, RECONCILE_DELTA_MS)

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
    args = parser.parse_args(argv)
    if args.metrics_port:
        start_http_server(args.metrics_port, addr="127.0.0.1")
    counts = run()
    print(
        f"reconcile: {counts['candidates']} candidates → "
        f"{counts['recovered']} recovered, {counts['still_missing']} still missing "
        f"(path=reconciled; report_snapshots pre/post written)"
    )


if __name__ == "__main__":
    main()
