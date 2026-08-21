"""Makefile guards, driven by `make -n` dry runs.

Two bugs that lived on main since Phase 2 / Phase 5 and surfaced in the Phase-16
review: (1) `SOURCE ?= fixtures  # comment` carried the whitespace before the
inline comment into the value, so a bare `make resolve` passed
`--source "fixtures  "` and exited 2; (2) `test-int-medium` ran `run-hot` without
`PROFILE=medium`, stamping `eval_meta` as tiny over a medium database — the
BACKLOG-43 profile guard then compared tiny to tiny and let `make eval` print
nonsense instead of failing loudly.

Quirk worth knowing: GNU make still EXECUTES recipe lines that invoke `$(MAKE)`
under `-n`, so each `test-int-*` dry run really spawns child makes; they are
harmless only because `-n` propagates through MAKEFLAGS, so the children also
dry-run and `docker compose down -v` never executes. No services, no network.
"""

import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
MAKEFILE = REPO_ROOT / "Makefile"

# `?=` is overridable from the environment; scrub so an exported SOURCE/PROFILE
# on the developer's shell can't fail these tests for a non-Makefile reason.
_ENV = {k: v for k, v in os.environ.items() if k not in {"SOURCE", "PROFILE"}}


def _dry_run(target: str) -> list[str]:
    out = subprocess.run(
        ["make", "-n", target],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=_ENV,
    ).stdout
    return out.splitlines()


def _profile_tokens(line: str) -> set[str]:
    """Every PROFILE value on a line, token-exact (`PROFILE=x` or `--profile "x"`)."""
    return set(re.findall(r"PROFILE=(\S+)", line)) | set(
        re.findall(r'--profile "([^"]+)"', line)
    )


def _isolated_live_targets() -> list[str]:
    """The clean-stack targets, discovered from the Makefile — a new one is
    covered by construction, not by remembering to list it here."""
    targets = re.findall(r"^(test-int-[a-z-]+):", MAKEFILE.read_text(), re.M)
    assert targets, "no test-int-* targets found"
    return targets


def test_make_resolve_default_source_has_no_trailing_whitespace() -> None:
    (line,) = [ln for ln in _dry_run("resolve") if "resolve.replay" in ln]
    assert re.search(r'--source "fixtures"(\s|$)', line), line


def test_no_variable_assignment_carries_an_inline_comment() -> None:
    # The general form of the SOURCE bug: make keeps the whitespace before an
    # inline `#` as part of the value. Comments go on their own line.
    offenders = [
        ln
        for ln in MAKEFILE.read_text().splitlines()
        if re.match(r"^[A-Za-z_][A-Za-z_0-9]*\s*[:?]?=.*#", ln)
    ]
    assert offenders == [], offenders


def test_every_isolated_live_target_seeds_populates_and_marks_one_profile() -> None:
    # Each clean-stack target seeds profile P, populates with PROFILE=P, and the
    # populate path stamps eval_meta with P — one profile per target, token-exact.
    # A seed/populate mismatch (the test-int-medium bug) fails here by name.
    for target in _isolated_live_targets():
        lines = _dry_run(target)
        seeds = {t for ln in lines if " seed " in ln for t in _profile_tokens(ln)}
        populates = {
            t
            for ln in lines
            if re.search(r"\bmake (run|run-hot|lake-land)\b", ln)
            for t in _profile_tokens(ln)
        }
        markers = {
            t for ln in lines if "write_marker" in ln for t in _profile_tokens(ln)
        }
        assert len(seeds) == 1, f"{target}: seed profiles {seeds}"
        assert populates == seeds, f"{target}: populate {populates} != seed {seeds}"
        assert markers == seeds, f"{target}: eval_meta marker {markers} != seed {seeds}"
