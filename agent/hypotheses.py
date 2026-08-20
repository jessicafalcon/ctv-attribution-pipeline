"""The hypothesis catalog (ARCHITECTURE §4.2 "Hypothesize") as a typed enum — the
closed set of causes the agent ranks. The model may only choose from these; a
free-text cause is not representable in the `AttributionFinding`, which is the point
(schema-constrained output, CLAUDE.md output contract).

Each member maps to a §4.1 signal and the frozen `AttributionContext` field that
feeds it (see the spec's catalog table). CO_VIEW_INFLATION is in the set for
completeness but is NOT reliably discriminable from RAW `genre_reach` alone — the
co-view-*adjusted* factor is deferred (BACKLOG 26 / Phase 10) — so the loop's prompt
forbids returning it as a CONFIDENT top hypothesis and routes an unexplained genre
skew to AMBIGUOUS_NEEDS_HUMAN."""

from enum import StrEnum


class Hypothesis(StrEnum):
    DEVICE_GRAPH_MISMATCH = "device_graph_mismatch"  # wrong-household / shared-IP
    WINDOW_EDGE_EFFECT = "window_edge_effect"  # attribution-window boundary pile-up
    CO_VIEW_INFLATION = "co_view_inflation"  # caveated: raw reach can't confirm it
    LATE_ARRIVAL_DISTORTION = "late_arrival_distortion"  # restatement after acting
    REAL_PERFORMANCE_CHANGE = "real_performance_change"  # genuine lift
    UPSTREAM_DATA_CHANGE = "upstream_data_change"  # match-rate move, none of the above
