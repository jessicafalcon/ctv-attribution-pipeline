"""Prometheus metrics for the reconciliation job (`reconcile_` prefix per
CLAUDE.md). Counters only — the job is a periodic batch, so cumulative counts
over runs are the useful signal (a rate is a query-time concern in Prometheus)."""

from prometheus_client import Counter

CANDIDATES = Counter(
    "reconcile_candidates_total",
    "Hot-unattributed conversions (attributed=0, path=hot) scanned as "
    "reconciliation candidates.",
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
