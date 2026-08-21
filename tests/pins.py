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
# over-credit, 0 wrong-household. precision 0.673, recall 1.000.
TINY_HOT = AccuracyPin(credited=52, truth=35, correct=35)
# medium hot (tests/integration/test_engine_hardening.py) — dedup + hour-late
# arrivals, same recall as a clean run. precision 0.708, recall 1.000.
MEDIUM_HOT = AccuracyPin(credited=130, truth=92, correct=92)
# long_delay (tests/integration/test_reconcile.py) — days-late conversions miss
# the 7d hot window; reconciliation lifts recall 0.587 → 0.973. precision is not
# reported for long_delay (the docs show "—").
LONG_DELAY_HOT = AccuracyPin(credited=83, truth=75, correct=44)
LONG_DELAY_POST = AccuracyPin(credited=112, truth=75, correct=73)
