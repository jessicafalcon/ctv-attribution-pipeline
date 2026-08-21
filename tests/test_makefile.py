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

import pytest

REPO_ROOT = Path(__file__).parent.parent
MAKEFILE = REPO_ROOT / "Makefile"

# `?=` is overridable from the environment; scrub so an exported SOURCE/PROFILE
# on the developer's shell can't fail these tests for a non-Makefile reason.
_ENV = {k: v for k, v in os.environ.items() if k not in {"SOURCE", "PROFILE"}}


def _dry_run(target: str, *args: str) -> list[str]:
    out = subprocess.run(
        ["make", "-n", target, *args],
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
    # `:` then end/tab/newline — not the `target: PROFILE = p` pin lines
    targets = re.findall(r"^(test-int-[a-z-]+):\s*$", MAKEFILE.read_text(), re.M)
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
            if re.search(r"\bmake (run|run-hot)\b", ln)
            for t in _profile_tokens(ln)
        }
        markers = {
            t for ln in lines if "write_marker" in ln for t in _profile_tokens(ln)
        }
        resets = {
            t
            for ln in lines
            if re.search(r"\bmake lake-reset\b", ln) and "CONFIRM=yes" in ln
            for t in _profile_tokens(ln)
        }
        assert len(seeds) == 1, f"{target}: seed profiles {seeds}"
        assert populates == seeds, f"{target}: populate {populates} != seed {seeds}"
        assert markers == seeds, f"{target}: eval_meta marker {markers} != seed {seeds}"
        # Phase 17: a clean stack is a clean lake — the target resets ITS profile's
        # lake (explicit CONFIRM=yes), else run-hot would reload an older pass.
        assert resets == seeds, f"{target}: lake-reset {resets} != seed {seeds}"
        # … and the target pins PROFILE target-wide, so its pytest line (run by the
        # parent make, not the `$(MAKE) … PROFILE=p` children) gets the same
        # LAKE_ROOT as the populate step.
        (seed,) = seeds
        pin = rf"^{target}: PROFILE = {re.escape(seed)}$"
        assert re.search(pin, MAKEFILE.read_text(), re.M), (
            f"{target}: missing target-specific `PROFILE = {seed}`"
        )


def _make_in_sandbox(tmp_path: Path, *args: str) -> subprocess.CompletedProcess:
    """Run the REAL lake-reset recipe (not -n) with the repo Makefile but cwd = a
    sandbox dir, so a guard failure could only ever remove something inside the
    sandbox. UV_PROJECT points `uv run` at the repo from the foreign cwd."""
    (tmp_path / "data" / "lake" / "tiny").mkdir(parents=True)
    (tmp_path / "data" / "x").mkdir()
    return subprocess.run(
        ["make", "-f", str(MAKEFILE), "-C", str(tmp_path), *args],
        capture_output=True,
        text=True,
        # UV_PROJECT: the repo venv from a foreign cwd; PYTHONPATH: the repo is
        # not an installed package (pytest's pythonpath=. does the same thing).
        env={**_ENV, "UV_PROJECT": str(REPO_ROOT), "PYTHONPATH": str(REPO_ROOT)},
        stdin=subprocess.DEVNULL,
    )


def test_destructive_recipes_are_one_python_process_each() -> None:
    # Rounds 2 and 3 of the Phase-17 review each found a hole in a Make-level
    # guard; the fix is structural: every destructive recipe is ONE line invoking
    # lake.destructive, which validates, confirms, then acts inside one process.
    for target, action in (
        ("lake-reset", "reset"),
        ("replay-serving", "replay"),
        ("lake-maintain", "maintain"),
    ):
        lines = [ln for ln in _dry_run(target, "PROFILE=tiny") if ln.strip()]
        assert lines[0].startswith(
            f'uv run python -m lake.destructive {action} --profile "tiny"'
        ), lines
        assert "--yes" not in lines[0]  # prompts unless CONFIRM=yes on the command line
        assert "rm -rf" not in "\n".join(lines) and "truncate" not in "\n".join(lines)
        (yes_line,) = [
            ln
            for ln in _dry_run(target, "PROFILE=tiny", "CONFIRM=yes")
            if "lake.destructive" in ln
        ]
        assert yes_line.rstrip().endswith("--yes")


def test_confirm_counts_only_from_the_command_line() -> None:
    # `$(origin CONFIRM)`: an exported CONFIRM=yes must not become --yes.
    out = subprocess.run(
        ["make", "-n", "lake-reset", "PROFILE=tiny"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env={**_ENV, "CONFIRM": "yes"},
    ).stdout
    assert "--yes" not in out


@pytest.mark.parametrize("flags", [[], ["-i"]])
def test_lake_reset_hostile_profile_refused_even_under_make_i(
    tmp_path: Path, flags
) -> None:
    # `make -i` steps over failed recipe LINES; it cannot step inside one process
    # (round 3: the shell-level guard was bypassed exactly this way).
    res = _make_in_sandbox(
        tmp_path, *flags, "lake-reset", "PROFILE=../x", "CONFIRM=yes"
    )
    assert "refusing" in res.stdout + res.stderr
    assert (tmp_path / "data" / "x").exists() and (
        tmp_path / "data" / "lake" / "tiny"
    ).exists()


def test_lake_reset_without_confirm_aborts_even_under_make_i(tmp_path: Path) -> None:
    res = _make_in_sandbox(tmp_path, "-i", "lake-reset", "PROFILE=tiny")
    assert "aborted" in res.stdout
    assert (tmp_path / "data" / "lake" / "tiny").exists()


def test_lake_reset_removes_exactly_the_profile_lake(tmp_path: Path) -> None:
    res = _make_in_sandbox(tmp_path, "lake-reset", "PROFILE=tiny", "CONFIRM=yes")
    assert res.returncode == 0, res.stdout + res.stderr
    assert not (tmp_path / "data" / "lake" / "tiny").exists()
    assert (tmp_path / "data" / "x").exists()
