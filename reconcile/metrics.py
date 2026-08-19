"""Prometheus metrics for the reconciliation job (`reconcile_` prefix per
CLAUDE.md). Counters only — the job is a periodic batch, so cumulative counts
over runs are the useful signal (a rate is a query-time concern in Prometheus)."""

from prometheus_client import Counter, Gauge

CANDIDATES = Counter(
    "reconcile_candidates_total",
    "Hot-unattributed conversions (attributed=0, path=hot) scanned as "
    "reconciliation candidates.",
)
RESTATEMENT_ROAS_ABS_DELTA = Gauge(
    "reconcile_restatement_roas_abs_delta",
    "Largest absolute ROAS restatement this pass: max over campaigns of "
    "|roas_now − roas_as_reported| between the pre- and post-reconciliation "
    "report_snapshots. Reconciliation only recovers misses, so per-campaign ROAS "
    "can only rise; this is how far the largest-moved campaign's reported ROAS "
    "shifted after advertisers may have acted on the pre number. Set once per pass.",
)
RECOVERED = Counter(
    "reconcile_recovered_total",
    "Candidates recovered — attributed to an exposure in the long (90d) window "
    "and re-written with path=reconciled.",
)
STILL_MISSING = Counter(
    "reconcile_still_missing_total",
    "Candidates with no exposure inside the long window — left as their hot "
    "unattributed row (a later pass may still recover them).",
)


def observe_restatement(abs_delta: float) -> None:
    """Record the largest absolute ROAS restatement this pass (once per pass)."""
    RESTATEMENT_ROAS_ABS_DELTA.set(abs_delta)
