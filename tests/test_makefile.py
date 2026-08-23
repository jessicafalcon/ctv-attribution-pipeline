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

Since fix/make-quote-profile every recipe interpolates the UNEXPANDED PROFILE /
SOURCE / PARTITION through `$(call _Q,$(value VAR))`, closing the Phase-17
residual where an env- or command-line-origin `PROFILE='$(shell …)'` ran the
shell at recipe-expansion time — even under `make -n`, which is NOT a dry run of a
variable's value. `test_profile_recipe_target_set_is_complete` pins the target set
so a new `$(PROFILE)` recipe cannot appear unquoted; the marker probes prove no
`$(shell …)` expands and the value reaches Python as one single-quoted argument.
"""

import re
import subprocess
from pathlib import Path

import pytest

from tests._env import clean_env

REPO_ROOT = Path(__file__).parent.parent
MAKEFILE = REPO_ROOT / "Makefile"

# `?=` is overridable from the environment; scrub so an exported SOURCE/PROFILE
# on the developer's shell can't fail these tests for a non-Makefile reason.
# The scrub list is shared with tests/test_destructive.py (tests/_env.py).
_ENV = clean_env()


def _dry_run(target: str, *args: str) -> list[str]:
    out = subprocess.run(
        ["make", "-n", "--no-print-directory", target, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=_ENV,
    ).stdout
    return [ln for ln in out.splitlines() if not ln.startswith("make[")]


def _profile_tokens(line: str) -> set[str]:
    """Every PROFILE value on a line, token-exact (`PROFILE=x` or `--profile 'x'`).
    Since fix/make-quote-profile the recipe form is single-quoted (`_Q`); the child
    `$(MAKE) … PROFILE=x` lines stay bare, so both forms are matched."""
    return set(re.findall(r"PROFILE=(\S+)", line)) | set(
        re.findall(r"""--profile ['"]([^'"]+)['"]""", line)
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
    assert re.search(r"--source 'fixtures'(\s|$)", line), line


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
        resets = {
            t
            for ln in lines
            if re.search(r"\bmake lake-reset\b", ln) and "CONFIRM=yes" in ln
            for t in _profile_tokens(ln)
        }
        assert len(seeds) == 1, f"{target}: seed profiles {seeds}"
        assert populates == seeds, f"{target}: populate {populates} != seed {seeds}"
        # eval_meta is stamped in-process by the populate step (no write_marker
        # recipe line anywhere — `make -i` must not stamp over a failed step)
        assert not any("write_marker" in ln for ln in lines), target
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
        marker = rf"^{target}: export CTV_INT = 1$"
        assert re.search(marker, MAKEFILE.read_text(), re.M), (
            f"{target}: must export CTV_INT=1 (integration tests skip without it)"
        )


def test_integration_tests_skip_without_the_marker() -> None:
    out = subprocess.run(
        ["uv", "run", "pytest", "tests/integration/test_engine.py", "-q", "-rs"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        # a NESTED pytest on purpose: the skip must fire in a fresh interpreter,
        # exactly as the run-tests hook would start one
        env={k: v for k, v in clean_env().items() if k != "CTV_INT"},
    ).stdout
    assert "SKIPPED" in out and "passed" not in out, out
    assert "CTV_INT" in out


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
        env=clean_env(UV_PROJECT=str(REPO_ROOT), PYTHONPATH=str(REPO_ROOT)),
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
            f"uv run python -m lake.destructive {action} --profile 'tiny'"
        ), lines
        assert len(lines) == 1, lines  # ONE process; nothing can run after a refusal
        assert "--yes" not in lines[0]  # prompts unless CONFIRM=yes on the command line
        assert "rm -rf" not in lines[0] and "truncate" not in lines[0]
        (yes_line,) = [
            ln
            for ln in _dry_run(target, "PROFILE=tiny", "CONFIRM=yes")
            if "lake.destructive" in ln
        ]
        assert yes_line.rstrip().endswith("--yes")


@pytest.mark.parametrize("target", ["lake-reset", "replay-serving", "lake-maintain"])
def test_confirm_counts_only_from_the_command_line(target: str) -> None:
    # `$(origin CONFIRM)`: an exported CONFIRM=yes must not become --yes, on any
    # of the three destructive paths (round 3: replay-serving had its own check).
    out = subprocess.run(
        ["make", "-n", target, "PROFILE=tiny"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=clean_env(CONFIRM="yes"),
    ).stdout
    assert "--yes" not in out


@pytest.mark.parametrize("flags", [[], ["-i"]])
@pytest.mark.parametrize("target", ["lake-reset", "replay-serving", "lake-maintain"])
def test_hostile_profile_refused_even_under_make_i(
    tmp_path: Path, flags: list[str], target: str
) -> None:
    # `make -i` steps over failed recipe LINES; it cannot step inside one process
    # (round 3: the shell-level guard was bypassed exactly this way). Executed
    # for all three targets (round 5) — a refused replay must not reach the
    # eval_meta stamp either (it is in the same process, after the load).
    res = _make_in_sandbox(tmp_path, *flags, target, "PROFILE=../x", "CONFIRM=yes")
    out = res.stdout + res.stderr
    assert "refusing" in out
    assert "marker set" not in out and "removed" not in out
    assert (tmp_path / "data" / "x").exists()
    assert (tmp_path / "data" / "lake" / "tiny").exists()


def test_docs_say_confirm_for_every_destructive_target() -> None:
    # docs-vs-behaviour: each destructive target's CLAUDE.md Commands bullet
    # names CONFIRM (round 5: lake-maintain's did not, and the live command
    # aborts without it).
    text = (REPO_ROOT / "CLAUDE.md").read_text()
    for target in ("lake-reset", "replay-serving", "lake-maintain"):
        pat = rf"^- `make {target} PROFILE=<p> \[CONFIRM=yes\]`"
        assert re.search(pat, text, re.M), (
            f"CLAUDE.md: `make {target}` must show CONFIRM"
        )


def test_lake_reset_without_confirm_aborts_even_under_make_i(tmp_path: Path) -> None:
    res = _make_in_sandbox(tmp_path, "-i", "lake-reset", "PROFILE=tiny")
    assert "aborted" in res.stdout
    assert (tmp_path / "data" / "lake" / "tiny").exists()


def test_lake_reset_removes_exactly_the_profile_lake(tmp_path: Path) -> None:
    res = _make_in_sandbox(tmp_path, "lake-reset", "PROFILE=tiny", "CONFIRM=yes")
    assert res.returncode == 0, res.stdout + res.stderr
    assert not (tmp_path / "data" / "lake" / "tiny").exists()
    assert (tmp_path / "data" / "x").exists()


@pytest.mark.parametrize("target", ["review-gate", "mutate"])
def test_review_tool_spec_value_is_one_literal_argument(target: str) -> None:
    # The first cut interpolated "$(SPEC)"; the threat-model probe
    # `SPEC='x"; echo PWNED; "'` RAN the echo. `_Q` single-quotes the value, so
    # the whole string reaches Python as one argument and is refused there.
    hostile = 'x"; echo PWNED; "'
    (line,) = [ln for ln in _dry_run(target, f"SPEC={hostile}") if "scripts/" in ln]
    assert "--spec 'x\"; echo PWNED; \"'" in line, line
    assert line.count("echo") == 1  # inside the quoted argument only
    (line,) = [ln for ln in _dry_run(target, "SPEC=it's") if "scripts/" in ln]
    assert "--spec 'it'\\''s'" in line, line


@pytest.mark.parametrize("target", ["review-gate", "mutate"])
def test_review_tool_spec_is_not_expanded_by_make(tmp_path: Path, target: str) -> None:
    # `$(value SPEC)`: an env-origin `SPEC='$(shell touch marker)'` used to run the
    # shell at recipe-expansion time, even under -n (security review, PR #35).
    marker = tmp_path / "expanded"
    out = subprocess.run(
        ["make", "-n", target],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=clean_env(SPEC=f"$(shell touch {marker})"),
    ).stdout
    assert not marker.exists()
    assert "--spec '$(shell touch" in out, out


# ------------------------------------------------- fix/make-quote-profile: every
# PROFILE / SOURCE / PARTITION recipe passes $(call _Q,$(value VAR)) so no user
# value reaches sh expanded. The three destructive recipes are the sharp edge.

# Every recipe that interpolates the PROFILE make-variable. The three destructive
# ones first. test-int-* are NOT here: they pass a LITERAL `PROFILE=<p>` to child
# makes and never expand `$(PROFILE)` themselves — the child recipe is what
# interpolates, and it is already in this set.
PROFILE_RECIPE_TARGETS = [
    "lake-reset",
    "replay-serving",
    "lake-maintain",
    "seed",
    "resolve",
    "run",
    "run-hot",
    "reconcile-dagster",
    "dagster-ui",
    "metrics-capture",
    "eval",
    "context",
    "agent-run",
]


def _q(value: str) -> str:
    """The single-quoted form `_Q = '$(subst ','\\'',$(1))'` produces for `value`."""
    return "'" + value.replace("'", "'\\''") + "'"


def _recipe_lines_by_target() -> dict[str, list[str]]:
    """target -> its tab-indented recipe lines, parsed from the Makefile source.
    A `target: VAR = v` pin line is a header, not a recipe line, so a literal
    `PROFILE = medium` pin never counts as an interpolation."""
    out: dict[str, list[str]] = {}
    current: str | None = None
    for line in MAKEFILE.read_text().splitlines():
        header = re.match(r"^([a-z][a-z0-9-]*):", line)
        if header:
            current = header.group(1)
            out.setdefault(current, [])
        elif line.startswith("\t") and current is not None:
            out[current].append(line)
        elif line and not line[0].isspace():
            current = None  # a variable assignment / comment ends the recipe block
    return out


def _profile_interpolating_targets() -> set[str]:
    """Targets whose recipe EXPANDS the PROFILE make-variable — `$(value PROFILE)`
    or a bare `$(PROFILE)` — as opposed to a literal `PROFILE=x` handed to a
    child make."""
    return {
        t
        for t, lines in _recipe_lines_by_target().items()
        for ln in lines
        if "$(value PROFILE)" in ln or re.search(r"\$\(PROFILE\)", ln)
    }


def test_profile_recipe_target_set_is_complete() -> None:
    # The pin that stops a new `$(PROFILE)` recipe from appearing unquoted: the
    # set of targets whose recipe interpolates PROFILE must equal the parametrized
    # list the probes below cover. Add a recipe, add it here (and it inherits the
    # marker probes) or this fails.
    assert _profile_interpolating_targets() == set(PROFILE_RECIPE_TARGETS)


def test_every_profile_reference_goes_through_value_and_Q() -> None:
    # No recipe may interpolate the make-EXPANDED PROFILE / SOURCE / PARTITION;
    # each reference is `$(call _Q,$(value VAR))`. A bare `"$(PROFILE)"` — the
    # Phase-17 residual — fails here (fix/make-quote-profile).
    offenders: list[tuple[str, str]] = []
    for target, lines in _recipe_lines_by_target().items():
        for ln in lines:
            bare = re.sub(
                r"\$\(call _Q,\$\(value (?:PROFILE|SOURCE|PARTITION)\)\)", "", ln
            )
            if re.search(r"\$\((?:PROFILE|SOURCE|PARTITION)\)", bare):
                offenders.append((target, ln))
    assert offenders == [], offenders


@pytest.mark.parametrize("target", PROFILE_RECIPE_TARGETS)
@pytest.mark.parametrize("origin", ["env", "cmdline"])
def test_profile_shell_is_not_expanded_by_make(
    tmp_path: Path, target: str, origin: str
) -> None:
    # `$(value PROFILE)`: an env- OR command-line-origin `PROFILE='$(shell touch
    # marker)'` used to run the shell at recipe-expansion time, even under -n
    # (BACKLOG row; PR-35 probes). The literal must reach the recipe single-quoted.
    marker = tmp_path / f"expanded_{origin}"
    value = f"$(shell touch {marker})"
    argv = ["make", "-n", "--no-print-directory", target]
    if origin == "env":
        env = clean_env(PROFILE=value)
    else:
        argv.append(f"PROFILE={value}")
        env = clean_env()
    out = subprocess.run(
        argv, cwd=REPO_ROOT, capture_output=True, text=True, env=env
    ).stdout
    assert not marker.exists(), f"{target}/{origin}: make expanded the $(shell …)"
    assert _q(value) in out, out


@pytest.mark.parametrize("target", PROFILE_RECIPE_TARGETS)
def test_profile_hostile_quoting_is_one_argument(tmp_path: Path, target: str) -> None:
    # `PROFILE='x"; touch m; "'` renders as one single-quoted argument — the
    # `; touch` lives inside the quotes, never as a second shell command.
    value = f'x"; touch {tmp_path / "pwned"}; "'
    out = subprocess.run(
        ["make", "-n", "--no-print-directory", target, f"PROFILE={value}"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=clean_env(),
    ).stdout
    assert _q(value) in out, out
    assert not (tmp_path / "pwned").exists()  # -n never executes a leaf recipe


@pytest.mark.parametrize("target", PROFILE_RECIPE_TARGETS)
def test_profile_apostrophe_is_escaped(target: str) -> None:
    # `PROFILE=it's` → `'it'\''s'` (the only char `_Q` escapes is `'`).
    out = subprocess.run(
        ["make", "-n", "--no-print-directory", target, "PROFILE=it's"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=clean_env(),
    ).stdout
    assert "'it'\\''s'" in out, out


@pytest.mark.parametrize("target", ["lake-reset", "replay-serving", "lake-maintain"])
def test_destructive_shell_profile_is_refused_and_lake_untouched(
    tmp_path: Path, target: str
) -> None:
    # The REAL recipe (not -n) under `make -i` with a $(shell …) PROFILE: `$(value
    # PROFILE)` keeps it unexpanded and `_Q` single-quotes it, so
    # lake/destructive.py's [a-z0-9_]+ validation refuses it — the destructive
    # action never runs, the lake is untouched. The make-expansion vector,
    # complementing test_hostile_profile_refused_even_under_make_i (PROFILE=../x).
    #
    # NOTE: this does NOT assert the $(shell …) marker is uncreated. A
    # command-line-origin PROFILE is expanded by make at STARTUP to export it to
    # recipe environments — independent of the recipe or $(value) — so the touch
    # runs before Python ever validates. Closing THAT needs `unexport PROFILE …`;
    # it is an open design question (see the phase report). What is guaranteed
    # here is the load-bearing invariant: the value is refused and nothing
    # destructive happens.
    res = _make_in_sandbox(
        tmp_path,
        "-i",
        target,
        f"PROFILE=$(shell touch {tmp_path / 'expanded'})",
        "CONFIRM=yes",
    )
    out = res.stdout + res.stderr
    assert "refusing" in out, out
    assert "marker set" not in out and "removed" not in out
    assert (tmp_path / "data" / "x").exists()
    assert (tmp_path / "data" / "lake" / "tiny").exists()
