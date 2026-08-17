"""Phase 0 smoke test: the repo layout exists.

Keeps the suite non-empty so `make test` (and the run-tests hook, which
treats "no tests collected" as skip) exercises a real pytest pass.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

LAYOUT = [
    "producer",
    "resolve",
    "streaming",
    "reconcile",
    "clickhouse",
    "queries",
    "observability",
    "agent",
    "fixtures/tiny",
    "specs",
    "docs",
]


def test_repo_layout() -> None:
    missing = [d for d in LAYOUT if not (ROOT / d).is_dir()]
    assert not missing, f"missing directories: {missing}"
