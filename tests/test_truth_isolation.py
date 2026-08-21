"""The pipeline NEVER reads truth links (CLAUDE.md determinism policy).

Structural guard: no pipeline-stage module may even mention truth links or
the data/truth path. Only the producer (which writes them), the accuracy eval
(`accuracy/`, which scores against them — Phase 4), the agent's eval harness,
and tests may.

Every new top-level package on the pipeline's write/load path must be added to
PIPELINE_DIRS in the phase that creates it (Phase 17 added `lake` and
`orchestration` at the review gate, after shipping without them).
"""

from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
PIPELINE_DIRS = [
    "resolve",
    "streaming",
    "reconcile",
    "clickhouse",
    "queries",
    "lake",
    "orchestration",
]


def test_pipeline_stages_never_mention_truth() -> None:
    offenders = []
    for dirname in PIPELINE_DIRS:
        for path in (REPO_ROOT / dirname).rglob("*"):
            if path.suffix in {".py", ".sql"} and "truth" in path.read_text().lower():
                offenders.append(str(path.relative_to(REPO_ROOT)))
    assert not offenders, f"pipeline code references truth links: {offenders}"


def test_agent_truth_access_confined_to_eval() -> None:
    offenders = []
    for path in (REPO_ROOT / "agent").rglob("*.py"):
        relative = path.relative_to(REPO_ROOT)
        if "eval" not in relative.parts and "truth" in path.read_text().lower():
            offenders.append(str(relative))
    assert not offenders, f"agent code outside eval/ references truth: {offenders}"
