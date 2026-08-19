"""`make context` — build the typed `AttributionContext` from ClickHouse and
print it. This is the deterministic, LLM-free observe step (ARCHITECTURE §4.2);
the agent loop that reasons over it is Phase 9. Reading the object back proves it
is populated and pydantic-valid — the Phase-8 Done-when.

Serving layer only (N1): reads attributed_conversions / exposures_landed FINAL and
report_snapshots. The causal side file is never touched here."""

import argparse

from clickhouse_connect.driver.client import Client

from agent import readers
from agent.collect import build_context
from agent.context import AttributionContext
from clickhouse.client import connect
from queries import report, restatement


def collect(client: Client, profile: str) -> AttributionContext:
    """Fetch every context input and assemble it. Campaign metrics and
    restatements reuse report.sql / restatement.sql (one source with `make report`
    / `make restate`); the rest come from agent/readers.py."""
    ip_overall = readers.query(client, readers.IP_CLUSTER_OVERALL)[0]
    lag_rows = readers.query(client, readers.WINDOW_EDGE_LAGS)
    return build_context(
        profile=profile,
        match_rate_rows=readers.query(client, readers.MATCH_RATE_BY_DAY),
        report_rows=report.run(client),
        restatement_rows=restatement.run(client),
        window_edge_lags=[r[0] for r in lag_rows],
        ip_overall_row=ip_overall,
        ip_top_rows=readers.query(client, readers.IP_CLUSTER_TOP),
        genre_exp_rows=readers.query(client, readers.GENRE_EXPOSURES),
        genre_conv_rows=readers.query(client, readers.GENRE_CONVERSIONS),
    )


def _fmt(value: float | None) -> str:
    return "NULL" if value is None else f"{value:.4f}"


def format_context(ctx: AttributionContext) -> str:
    lines = [
        f"attribution context — profile {ctx.profile} (from ClickHouse, no LLM)",
        "-" * 62,
        f"  processed / attributed : {ctx.processed} / {ctx.attributed}",
        f"  match rate             : {_fmt(ctx.match_rate)}"
        f"  (over {len(ctx.match_rate_by_day)} days)",
        "",
        "  shared-IP (wrong-household signal):",
        f"    ip-resolved attributed : {ctx.ip_clusters.ip_resolved_attributed}"
        f" / {ctx.ip_clusters.attributed}"
        f"  (fraction {_fmt(ctx.ip_clusters.ip_resolved_fraction)})",
        f"    ambiguous attributed   : {ctx.ip_clusters.ambiguous_attributed}"
        f"  (max candidate_count {ctx.ip_clusters.max_candidate_count})",
        "    top clusters           : "
        + ", ".join(
            f"{c.ip}={c.attributed_conversions}"
            for c in ctx.ip_clusters.top_clusters[:5]
        ),
        "",
        "  window-edge (attribution lag over the 7d hot window):",
        "    " + "  ".join(f"{b.label}:{b.count}" for b in ctx.window_edge.buckets),
        f"    near-boundary          : {ctx.window_edge.near_boundary_count}"
        f" / {ctx.window_edge.attributed_hot}"
        f"  (fraction {_fmt(ctx.window_edge.near_boundary_fraction)})",
        "",
        "  genre reach (RAW, not co-view-adjusted):",
    ]
    for g in ctx.genre_reach:
        lines.append(
            f"    {g.genre:<8} exposures={g.exposures} "
            f"conv={g.attributed_conversions} "
            f"conv/exp={_fmt(g.conversions_per_exposure)}"
        )
    lines += ["", "  per-campaign metrics:"]
    for c in ctx.campaigns:
        lines.append(
            f"    {c.campaign:<8} roas={_fmt(c.roas)} cpa={_fmt(c.cpa)} "
            f"cvr={_fmt(c.cvr)} svr={_fmt(c.site_visit_rate)}"
        )
    lines += ["", "  restatements (pre-reconcile → now):"]
    if not ctx.restatements:
        lines.append("    (none — report_snapshots empty)")
    for r in ctx.restatements:
        lines.append(
            f"    {r.campaign:<8} roas {_fmt(r.roas_as_reported)} → "
            f"{_fmt(r.roas_now)}  (Δ {r.roas_delta:+.4f}, "
            f"conv {r.conversions_as_reported}→{r.conversions_now})"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="build the AttributionContext")
    parser.add_argument("--profile", default="tiny")
    args = parser.parse_args(argv)
    ctx = collect(connect(), args.profile)
    print(format_context(ctx))


if __name__ == "__main__":
    main()
