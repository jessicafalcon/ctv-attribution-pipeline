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
