"""Canonical household-grain accuracy pins — the single source of truth for the
precision/recall numbers asserted by the test suites (integration
`test_eval_report` / `test_engine_hardening` / `test_reconcile`, offline
`test_accuracy` / `test_medium_parity`) AND printed in the docs accuracy TABLES
(README.md, docs/RESULTS.md). Change a number here and the live assertions and the
docs-table guard (tests/test_docs_accuracy_pins.py) move together (BACKLOG 36).

Scope: the docs guard covers the accuracy TABLES only. Numbers restated in doc
PROSE (the RESULTS "why the numbers moved" section, the lakehouse 0.9733 line,
PHASES, README run-comments) are NOT reachable by a table parser — keep those in
sync by hand (a prose-citation guard is a deferred BACKLOG option).

The COUNTS are the pinned facts; precision and recall derive from them exactly as
`accuracy/score.py` computes them, so the pin and the scored report agree bit for
bit.
"""

from dataclasses import dataclass

# ClickHouse (digest-pinned image, 24.8.14.39): how many of the 100,000 cent
# values 0.00 … 999.99 `toDecimal64(<Float64>, 4)` truncates (RUNBOOK incident 3;
# ARCHITECTURE §8). Asserted live in tests/integration/test_reconcile.py; the
# docs cite it through tests/test_docs_accuracy_pins.py. An image bump may move it.
TODECIMAL_TRUNCATED_CENT_VALUES = 5228


@dataclass(frozen=True)
class AccuracyPin:
    credited: int
    truth: int
    correct: int

    @property
    def precision(self) -> float:
        return self.correct / self.credited

    @property
    def recall(self) -> float:
        return self.correct / self.truth


# tiny hot (tests/integration/test_eval_report.py) — last-touch organic
# over-credit, 0 wrong-household. precision 0.681, recall 0.914.
# Phase 16 (was 52/35/35): the 5 shared-IP conversions (3 caused) are deferred
# to reconciliation instead of guessed hot; post-reconcile tiny is 52/35/35 again.
TINY_HOT = AccuracyPin(credited=47, truth=35, correct=32)
# medium hot (tests/integration/test_engine_hardening.py) — dedup + hour-late
# arrivals. precision 0.705, recall 0.989.
# Phase 16 (was 130/92/92): 1 caused shared-IP conversion deferred hot;
# post-reconcile medium is 130/92/92 again.
MEDIUM_HOT = AccuracyPin(credited=129, truth=92, correct=91)
# long_delay (tests/integration/test_reconcile.py) — days-late conversions miss
# the 7d hot window; reconciliation lifts recall 0.587 → 0.973. precision is not
# reported for long_delay (the docs show "—").
# Phase 16 hot (was 83/75/44): the 2 caused shared-IP conversions the old reduce
# credited to the WRONG household (plus 1 organic) are deferred — correct count
# and recall unchanged; credited drops by 3. POST is unchanged (112/75/73): the
# reconcile pass credits those same conversions with the same rule.
LONG_DELAY_HOT = AccuracyPin(credited=80, truth=75, correct=44)
LONG_DELAY_POST = AccuracyPin(credited=112, truth=75, correct=73)

# Phase 16's central claim — "same answer after reconciliation" — asserted offline
# in tests/test_post_reconcile_pins.py (not in the docs accuracy tables, which stay
# hot-path for tiny/medium). tiny/medium POST equal their pre-Phase-16 hot numbers.
TINY_POST = AccuracyPin(credited=52, truth=35, correct=35)
MEDIUM_POST = AccuracyPin(credited=130, truth=92, correct=92)
# shared_ip_spike (seed 0): hot defers 19 caused (0 wrong-household, by
# construction); the reconcile pass makes the identical pick the deleted hot reduce
# made — 69/80 correct, 11 wrong-household (caused_wrong_household is asserted
# alongside these in the tests). Referenced by tests/test_fault_profiles.py,
# tests/test_reconcile.py and tests/integration/test_context.py.
SHARED_IP_HOT = AccuracyPin(credited=94, truth=80, correct=61)
SHARED_IP_POST = AccuracyPin(credited=119, truth=80, correct=69)
SHARED_IP_POST_WRONG_HOUSEHOLD = 11
