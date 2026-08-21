"""Every documented clean-state chain resets the lake (Phase 17 review gate).

A clean stack is a clean lake: `make down` does not touch data/lake/, and the
serving tables are LOADED from the lake's current rows — so a `make run-hot` over a
lake that already holds a `make run`'s reconciled rows shifts the hot-only pins.
The rule applies to the phase's own gate: the spec DONE command, PHASES' Phase-17
Done-when, CI's integration job, and the README / CLAUDE.md canonical demos must
all carry `make lake-reset` between `make down` (or the job start) and
`make up`/`make seed`. The clean-stack test-int-* targets are covered by
test_makefile.py. Earlier phases' Done-when commands in PHASES.md are history and
are not rewritten.
"""

import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
RESET = (
    r"make down && make lake-reset\s+(PROFILE=long_delay\s+)?CONFIRM=yes"
    r"\s*&&\s*make up"
)

CHAINS = {
    "specs/phase-17-lake-of-record.md": 1,
    "docs/PHASES.md": 1,
    "CLAUDE.md": 2,
    "README.md": 2,
}


def test_every_documented_clean_state_chain_resets_the_lake() -> None:
    for rel, expected in CHAINS.items():
        text = (ROOT / rel).read_text()
        assert len(re.findall(RESET, text)) >= expected, (
            f"{rel}: chains missing lake-reset"
        )


def test_current_docs_never_go_down_then_up_without_a_lake_reset() -> None:
    # The negative form: `make down && make up` with nothing in between is the
    # stale pattern this guard exists to catch (living docs only; PHASES keeps
    # earlier phases' Done-when commands verbatim).
    for rel in ("README.md", "CLAUDE.md", "specs/phase-17-lake-of-record.md"):
        assert "make down && make up" not in (ROOT / rel).read_text(), rel


def test_ci_resets_the_lake_before_populating() -> None:
    steps = [
        ln.strip()
        for ln in (ROOT / ".github/workflows/ci.yml").read_text().splitlines()
        if ln.strip().startswith("- run: make")
    ]
    i_reset = steps.index("- run: make lake-reset CONFIRM=yes")
    i_seed = steps.index("- run: make seed PROFILE=tiny")
    i_hot = steps.index("- run: make run-hot")
    assert i_reset < i_seed < i_hot
