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
    """Run the REAL recipe (not -n) with the repo Makefile but cwd = a sandbox dir,
    so a guard failure could only ever remove something inside the sandbox."""
    (tmp_path / "data" / "lake" / "tiny").mkdir(parents=True)
    (tmp_path / "data" / "x").mkdir()
    return subprocess.run(
        ["make", "-f", str(MAKEFILE), "-C", str(tmp_path), "lake-reset", *args],
        capture_output=True,
        text=True,
        env=_ENV,
    )


@pytest.mark.parametrize(
    "hostile",
    [
        ["PROFILE=", "CONFIRM=yes"],  # → data/lake/  (every profile's lake)
        ["PROFILE=../x", "CONFIRM=yes"],  # → escapes data/lake
        ["PROFILE=/abs", "CONFIRM=yes"],
    ],
)
def test_lake_reset_refuses_a_root_outside_data_lake_profile(
    tmp_path: Path, hostile: list[str]
) -> None:
    res = _make_in_sandbox(tmp_path, *hostile)
    assert res.returncode != 0, res.stdout + res.stderr
    assert "refusing" in res.stdout
    assert (tmp_path / "data" / "lake" / "tiny").exists()  # nothing removed
    assert (tmp_path / "data" / "x").exists()


def test_lake_reset_ignores_a_command_line_lake_root_override(tmp_path: Path) -> None:
    # `override LAKE_ROOT` — a caller cannot retarget the rm (it would propagate
    # into the test-int-* targets, where CONFIRM=yes is already set).
    (tmp_path / "elsewhere").mkdir()
    res = _make_in_sandbox(
        tmp_path, "LAKE_ROOT=" + str(tmp_path / "elsewhere"), "CONFIRM=yes"
    )
    assert res.returncode == 0, res.stdout + res.stderr
    assert (tmp_path / "elsewhere").exists()
    assert not (tmp_path / "data" / "lake" / "tiny").exists()  # the real target went
    assert "removed data/lake/tiny" in res.stdout


def test_lake_reset_prompts_unless_confirmed_and_scopes_to_the_profile() -> None:
    recipe = "\n".join(_dry_run("lake-reset"))
    # `$(_CONFIRMED)` expands to "" in a dry run: the guard shape is what survives
    assert "rm -rf" in recipe and '!= "yes" ]' in recipe and "read ans" in recipe
    # per-profile root, rebuilt from the VALIDATED shell variable — never from a
    # make-interpolated value; never data/lake itself
    assert 'rm -rf "data/lake/$_LAKE_RESET_PROFILE"' in recipe, recipe
    assert 'rm -rf "data/lake"' not in recipe


INJECTION = 'x" ; touch pwned ; echo "'


def test_lake_reset_never_interpolates_the_profile_into_the_recipe() -> None:
    # Security review round 2: a PROFILE carrying shell metacharacters used to run
    # its payload BEFORE the refusal. The dry run must show the user text nowhere
    # in the recipe (it reaches the shell as an environment variable), and the
    # `case` guard must be the first command.
    lines = [ln for ln in _dry_run("lake-reset", f"PROFILE={INJECTION}") if ln.strip()]
    assert "touch pwned" not in "\n".join(lines)
    assert lines[0].lstrip().startswith('case "$_LAKE_RESET_PROFILE"'), lines[0]


def test_lake_reset_injection_runs_nothing_and_refuses(tmp_path: Path) -> None:
    res = _make_in_sandbox(tmp_path, f"PROFILE={INJECTION}", "CONFIRM=yes")
    assert res.returncode != 0
    assert not (tmp_path / "pwned").exists()
    assert "refusing" in res.stdout
    assert (tmp_path / "data" / "lake" / "tiny").exists()


def test_lake_reset_ignores_an_exported_confirm(tmp_path: Path) -> None:
    # `$(origin CONFIRM)` must be the command line: an ambient CONFIRM=yes in the
    # shell does not skip the prompt (stdin is closed → "aborted").
    (tmp_path / "data" / "lake" / "tiny").mkdir(parents=True)
    res = subprocess.run(
        [
            "make",
            "-f",
            str(MAKEFILE),
            "-C",
            str(tmp_path),
            "lake-reset",
            "PROFILE=tiny",
        ],
        capture_output=True,
        text=True,
        env={**_ENV, "CONFIRM": "yes"},
        stdin=subprocess.DEVNULL,
    )
    assert res.returncode != 0
    assert "aborted" in res.stdout
    assert (tmp_path / "data" / "lake" / "tiny").exists()
