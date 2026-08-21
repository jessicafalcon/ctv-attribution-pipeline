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
from datetime import UTC, datetime, timedelta

from clickhouse_connect.driver.client import Client
from prometheus_client import REGISTRY, start_http_server, write_to_textfile

from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from producer.models import AttributedConversion, Exposure, ResolvedConversion
from reconcile import metrics, rollup
from reconcile.sources import ClickHouseExposureSource, ExposureSource
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
    client: Client,
    candidates: list[ResolvedConversion],
    exposure_source: ExposureSource,
    window: timedelta,
    reconciled_at: datetime,
) -> list[AttributedConversion]:
    """Recover the given candidates: read their households' exposures from the
    source, run the matcher, insert the recovered rows. The recovery half of a
    pass, shared by `run` (all candidates, ClickHouse source) and the Dagster
    day-partitioned asset (one day's candidates, Iceberg source). `reconciled_at`
    is passed in (not recomputed) so every day of a partitioned run stamps the
    same version — identical to a single full pass."""
    exposures = exposure_source.read_for(candidates, window)
    recovered = reconcile(candidates, exposures, window, reconciled_at)
    if recovered:
        insert_attributed(client, recovered)
    return recovered


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
    """One reconciliation pass: recover ALL hot misses over the long window, then
    finalize (snapshots + rollup refresh). Returns counts for logging/tests.

    `exposure_source` selects where the candidate households' exposures are read
    from: the default ClickHouse source keeps `make run` byte-identical; the
    Dagster asset passes an Iceberg/DuckDB source (Phase 12). Everything else —
    candidates, snapshots, rollup, insert — stays on ClickHouse."""
    apply_ddl()
    client = client or connect()
    exposure_source = exposure_source or ClickHouseExposureSource(client)

    reconciled_at = reconciled_at_for(_max_ingest(client))
    candidates = _read_candidates(client)
    recovered = recover(client, candidates, exposure_source, LONG_WINDOW, reconciled_at)
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
