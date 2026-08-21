"""Prometheus metrics for the lake → ClickHouse load stage (Phase 17). Stage
prefix `lake_` (CLAUDE.md convention: every stage has metrics). Emitted from the
process that runs the load — the engine (`make run-hot`), the reconcile job, or
`make replay-serving` — into that process's default registry, so `make
metrics-capture`'s `engine.prom` / `reconcile.prom` carry them."""

from prometheus_client import Counter

ROWS_LOADED = Counter(
    "lake_rows_loaded_total",
    "Rows inserted into a ClickHouse serving table from the lake by the Dagster "
    "load (cumulative; re-materializing a day re-counts its rows — the "
    "ReplacingMergeTree collapses them, this does not).",
    ["table"],
)
