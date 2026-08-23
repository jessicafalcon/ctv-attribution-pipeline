"""Offline pins for the review tooling (scripts/review_gate.py, scripts/mutate.py)
on throwaway repos built in tmp_path with `git init`. No services, no network;
the whole module runs in a few seconds (three short pytest subprocesses)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import mutate  # noqa: E402
import review_common as common  # noqa: E402
import review_gate as gate  # noqa: E402
import round_tag  # noqa: E402

SUITE = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider"]

MOD = """
def guarded(xs):
    if not xs:
        return None
    return max(xs, key=lambda x: (x[0], x[1]))


def plain(xs):
    return sorted(xs)
"""
TEST = """
from pkg.mod import guarded


def test_guarded():
    assert guarded([(1, 2), (2, 1)]) == (2, 1)
    assert guarded([]) is None
"""
SPEC = """# Phase X

## Evidence (REQUIRED)

| Done-when | Proof |
|---|---|
| 1 | `tests/test_mod.py::test_guarded` |
| 2 | `tests/test_mod.py::test_missing` |

## Invariants (REQUIRED)

```mutations
pkg/mod.py::guarded     invert-guard
pkg/mod.py::plain   constant-return:0
```

## Record updates (REQUIRED)

- [ ] `DECISIONS.md` — entry
- [ ] `docs/PHASES.md` — row
"""


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "pkg").mkdir(parents=True)
    (root / "pkg" / "__init__.py").write_text("")
    (root / "pkg" / "mod.py").write_text(MOD)
    (root / "tests").mkdir()
    (root / "tests" / "test_mod.py").write_text(TEST)
    (root / "specs").mkdir()
    (root / "specs" / "phase-x.md").write_text(SPEC)
    (root / "docs").mkdir()
    (root / "docs" / "PHASES.md").write_text("old_symbol is gone\n")
    (root / "DECISIONS.md").write_text("base\n")
    _git(root, "init", "-q", "-b", "main")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "base")
    return root


# ------------------------------------------------------------ review_gate


def test_gate_fails_on_a_missing_evidence_test_id(repo: Path, capsys) -> None:
    ok = gate.check_evidence(SPEC, repo)
    out = capsys.readouterr().out
    assert ok is False
    assert (
        "FAIL evidence: named test does not exist: tests/test_mod.py::test_missing"
        in out
    )
    assert "test_guarded" not in out  # the existing id is not reported


def test_gate_fails_on_a_record_file_absent_from_the_diff(repo: Path, capsys) -> None:
    base = _git(repo, "rev-parse", "HEAD").strip()
    (repo / "DECISIONS.md").write_text("base\nphase x\n")
    (repo / "README.md").write_text("not on the list\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "records")
    ok = gate.check_records(SPEC, repo, base)
    out = capsys.readouterr().out
    assert ok is False
    assert "absent from the diff: docs/PHASES.md" in out
    assert "WARN records: record file in the diff but not on the list: README.md" in out


def test_gate_diffs_against_the_merge_base_not_mains_tip(repo: Path, capsys) -> None:
    # Three-dot: main advancing under the branch (18a's situation) must not
    # surface main's own record edits as drift on the branch.
    _git(repo, "checkout", "-q", "-b", "branch")
    (repo / "DECISIONS.md").write_text("base\nphase x\n")
    (repo / "docs" / "PHASES.md").write_text("row\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "branch records")
    _git(repo, "checkout", "-q", "main")
    (repo / "BACKLOG.md").write_text("main moved on\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "main advances")
    _git(repo, "checkout", "-q", "branch")
    assert gate.check_records(SPEC, repo, "main") is True
    out = capsys.readouterr().out
    assert "BACKLOG.md" not in out  # main's commit is not the branch's drift
    assert "PASS records: 2 listed record files are in the diff" in out


def test_gate_fails_on_a_deleted_symbol_hit(repo: Path, capsys) -> None:
    assert gate.check_deleted(["old_symbol"], repo, None) is False
    assert (
        "FAIL deleted symbol still referenced: old_symbol (1 hits)"
        in capsys.readouterr().out
    )
    assert gate.check_deleted(["never_there"], repo, None) is True


def test_gate_ignores_struck_and_historical_hits(repo: Path) -> None:
    (repo / "docs" / "PHASES.md").write_text(
        "~~old_symbol~~ DONE\nx <!-- historical --> old_symbol\n"
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "history")
    assert gate.check_deleted(["old_symbol"], repo, None) is True


# ------------------------------------------------------------------ spec


@pytest.mark.parametrize(
    "bad", ["", "  ", "../x", "specs/../DECISIONS.md", "DECISIONS.md", "specs/"]
)
def test_spec_outside_specs_is_refused(repo: Path, bad: str) -> None:
    with pytest.raises(common.Refused, match="refusing"):
        common.resolve_spec(bad, repo)


def test_absolute_spec_is_refused_even_inside_specs(repo: Path) -> None:
    with pytest.raises(common.Refused, match="refusing"):
        common.resolve_spec(str(repo / "specs" / "phase-x.md"), repo)
    assert common.resolve_spec("specs/phase-x.md", repo) == (
        repo / "specs" / "phase-x.md"
    )


def test_unknown_operator_is_refused() -> None:
    with pytest.raises(common.Refused, match="unknown operator"):
        mutate.parse_mutations(
            "## Invariants\n```mutations\npkg/mod.py::f   nuke\n```\n"
        )
    with pytest.raises(common.Refused, match="repo-relative"):
        mutate.parse_mutations(
            "## Invariants\n```mutations\n../x.py::f   invert-guard\n```\n"
        )


# --------------------------------------------------------------- mutate


def test_mutate_reports_survived_and_killed_and_leaves_the_tree_untouched(
    repo: Path, tmp_path: Path, capsys
) -> None:
    (repo / "scratch.txt").write_text("uncommitted\n")  # the tree is dirty on purpose
    before = _git(repo, "status", "--porcelain")
    assert before.strip()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    code = mutate.sweep(mutate.parse_mutations(SPEC), repo, scratch, SUITE)
    out = capsys.readouterr().out
    assert code == 1
    assert "KILLED   pkg/mod.py::guarded invert-guard -> pkg/mod.py:3" in out
    assert "SURVIVED pkg/mod.py::plain constant-return:0 -> pkg/mod.py:9" in out
    assert "mutate FAIL: 1/2 killed" in out
    assert _git(repo, "status", "--porcelain") == before
    assert (repo / "pkg" / "mod.py").read_text() == MOD
    assert list(scratch.iterdir()) == []  # every worktree removed
    assert "mut-" not in _git(repo, "worktree", "list")
    assert "worktree registry changed" not in out  # asserted inside the finally


@pytest.mark.parametrize(
    "bad",
    [
        '__import__("os").system("id")',
        "f()",
        "x",
        "1+1",
        "[" * 200 + "]" * 200,
    ],
)
def test_constant_return_value_must_be_a_short_literal(bad: str) -> None:
    text = f"## Invariants\n```mutations\npkg/mod.py::f   constant-return:{bad}\n```\n"
    with pytest.raises(common.Refused, match="not a literal|longer than"):
        mutate.parse_mutations(text)


def test_constant_return_accepts_literals() -> None:
    for ok in ("0", "None", "'x'", "(1,2)", "-1.5", "True"):
        text = (
            f"## Invariants\n```mutations\npkg/mod.py::f   constant-return:{ok}\n```\n"
        )
        assert mutate.parse_mutations(text)[0].arg == ok


def test_suite_env_is_reduced(tmp_path: Path) -> None:
    env = common.suite_env(tmp_path)
    assert set(env) == {"PATH", "HOME", "PYTHONPATH", "CTV_INT"}
    assert env["PYTHONPATH"] == str(tmp_path)


def test_the_sweep_has_two_independent_guards_against_the_live_stack(tmp_path):
    # Guard 1: the marker — the sweep's pytest EXECUTES, so it gets CTV_INT=0 and
    # tests/conftest.py skips integration; only the gate's collect-only gets 1.
    assert common.suite_env(tmp_path)["CTV_INT"] == "0"
    assert common.suite_env(tmp_path, ctv_int="1")["CTV_INT"] == "1"
    # Guard 2: the ignore flag — one unpinned literal is not a guard.
    assert "--ignore=tests/integration" in mutate.SUITE


def test_make_targets_derivation_equals_the_phony_line() -> None:
    # Assert the derivation, never trust the parser (check_docs reads marker
    # constants out of the generators the same way).
    root = Path(__file__).parent.parent
    phony = next(
        ln
        for ln in (root / "Makefile").read_text().splitlines()
        if ln.startswith(".PHONY:")
    )
    assert gate.make_targets(root) == set(phony.split(":", 1)[1].split())


def test_make_targets_handles_multi_name_rules_and_skips_assignments(tmp_path):
    (tmp_path / "Makefile").write_text("a b:\n\ttrue\nname:=v\nx :=1\nc_d:\n\ttrue\n")
    assert gate.make_targets(tmp_path) == {"a", "b", "c_d"}


@pytest.mark.parametrize(
    "path",
    [
        "tests/conftest.py",
        "./tests/oracle.py",
        "tests/./oracle.py",
        "lake/../tests/x.py",
    ],
)
def test_mutation_targets_under_tests_are_refused_for_every_operator(path: str):
    # Resolved once, gated on Path.parts — never a string prefix (r3 security 1:
    # `./tests/oracle.py` walked through `startswith("tests/")`).
    for op in ("delete-call", "constant-return:0", "invert-guard", "swap-sort-key"):
        text = f"## Invariants\n```mutations\n{path}::f   {op}\n```\n"
        with pytest.raises(common.Refused, match="under tests/"):
            mutate.parse_mutations(text)


def test_mutation_target_escaping_the_repo_is_refused() -> None:
    for path in ("tests/../../x.py", "../lake/x.py", "/abs/x.py", "lake/x.txt"):
        text = f"## Invariants\n```mutations\n{path}::f   invert-guard\n```\n"
        with pytest.raises(common.Refused, match="refusing"):
            mutate.parse_mutations(text)


def test_mutation_target_is_normalized_once() -> None:
    text = "## Invariants\n```mutations\nlake/./sub/../x.py::f   invert-guard\n```\n"
    assert mutate.parse_mutations(text)[0].file == "lake/x.py"


def test_untracked_target_is_one_error_line_and_the_sweep_continues(
    repo: Path, tmp_path: Path, capsys
) -> None:
    (repo / "pkg" / "loose.py").write_text("def f():\n    if 1:\n        return 2\n")
    spec = (
        "## Invariants\n```mutations\npkg/loose.py::f   invert-guard\n"
        "pkg/nope.py::f   invert-guard\npkg/mod.py::guarded   invert-guard\n```\n"
    )
    scratch = tmp_path / "s"
    scratch.mkdir()
    code = mutate.sweep(mutate.parse_mutations(spec), repo, scratch, SUITE)
    out = capsys.readouterr().out
    assert code == 1
    assert (
        "ERROR    pkg/loose.py::f invert-guard: refusing: pkg/loose.py is not a" in out
    )
    assert "ERROR    pkg/nope.py::f invert-guard:" in out
    assert "KILLED   pkg/mod.py::guarded invert-guard" in out  # the sweep went on
    assert "mutate FAIL: 1/3 killed, 0 survived, 2 errors" in out  # triple sums to 3


def test_history_exemption_applies_to_markdown_only(repo: Path, capsys) -> None:
    (repo / "pkg" / "mod.py").write_text(MOD + "\nx = ~~old_symbol  # not history\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "py")
    assert gate.check_deleted(["old_symbol"], repo, None) is False
    assert "pkg/mod.py" in capsys.readouterr().out


def test_backticked_make_only(repo: Path, capsys) -> None:
    (repo / "Makefile").write_text("lint:\n\ttrue\n")
    assert gate.check_evidence(
        "## Evidence\n| 1 | make sure `make lint` is green |\n", repo
    )
    assert "PASS evidence" in capsys.readouterr().out


def test_an_unknown_backticked_target_fails_without_running_make(repo: Path, capsys):
    (repo / "Makefile").write_text("lint:\n\ttrue\n")
    assert gate.check_evidence("## Evidence\n| 1 | `make nope` |\n", repo) is False
    assert "named make target does not exist: make nope" in capsys.readouterr().out
    assert gate.make_targets(repo) == {"lint"}


def test_deleted_symbol_is_literal_and_git_errors_are_distinct(
    repo: Path, capsys
) -> None:
    assert (
        gate.check_deleted(["zz[("], repo, None) is True
    )  # literal `[` is not in the tree
    assert gate.check_deleted(["old_symbol"], repo, None) is False
    out = capsys.readouterr().out
    assert "PASS deleted symbol gone: zz[(" in out
    assert "(1 hits)" in out


def test_registry_change_is_its_own_latched_outcome(
    repo: Path, tmp_path: Path, capsys, monkeypatch
):
    real_run = mutate.run
    decoy = tmp_path / "decoy"
    planted = {"done": False}

    def run_with_decoy(cmd, cwd, env=None):
        code, out = real_run(cmd, cwd, env)
        if cmd[:3] == ["git", "worktree", "remove"] and not planted["done"]:
            planted["done"] = True
            real_run(
                ["git", "worktree", "add", "--detach", "-q", str(decoy), "HEAD"], cwd
            )
        return code, out

    monkeypatch.setattr(mutate, "run", run_with_decoy)
    spec = (
        "## Invariants\n```mutations\npkg/mod.py::guarded   invert-guard\n"
        "pkg/mod.py::plain   constant-return:0\n```\n"
    )
    scratch = tmp_path / "s"
    scratch.mkdir()
    code = mutate.sweep(mutate.parse_mutations(spec), repo, scratch, SUITE)
    out = capsys.readouterr().out
    real_run(["git", "worktree", "remove", "--force", str(decoy)], repo)
    assert code == 1
    assert out.count("REGISTRY worktree registry changed") == 1  # latched, once
    assert (
        "mutate FAIL: 1/2 killed, 1 survived, 0 errors, worktree registry changed"
        in out
    )


# ------------------------------------------------------------- round_tag


def test_round_tag_is_a_boundary_only(repo: Path) -> None:
    round_tag.write(1, repo)
    assert round_tag.read(1, repo) == 1
    msg = _git(repo, "tag", "-l", "--format=%(contents)", "review-round-1")
    assert msg == "round=1\n\n"  # git's newline + tag -l's; nothing else in it
    with pytest.raises(common.Refused, match="exists"):
        round_tag.write(1, repo)


@pytest.mark.parametrize(
    "message", ["round=2", "round=1 ", "round=1\ncap=yes", "x", ""]
)
def test_round_tag_parse_is_anchored_and_never_defaults(message: str) -> None:
    with pytest.raises(common.Refused, match="parse error"):
        round_tag.parse(message + "\n\n", 1)
    assert round_tag.parse("round=1\n\n", 1) == 1


def test_round_tag_missing_or_tampered_tag_stops(repo: Path) -> None:
    with pytest.raises(common.Refused, match="missing"):
        round_tag.read(1, repo)
    _git(repo, "tag", "-a", "review-round-1", "HEAD", "-m", "round=1\ncorrectness=2")
    with pytest.raises(common.Refused, match="parse error"):
        round_tag.read(1, repo)


def test_round_tag_requires_ancestry(repo: Path) -> None:
    # another branch's round is not this branch's boundary
    _git(repo, "checkout", "-q", "-b", "phase-a")
    (repo / "a.txt").write_text("a\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "a")
    round_tag.write(1, repo)
    _git(repo, "checkout", "-q", "main")
    _git(repo, "checkout", "-q", "-b", "phase-b")
    with pytest.raises(common.Refused, match="not an ancestor"):
        round_tag.read(1, repo)
    with pytest.raises(common.Refused, match="exists"):
        round_tag.write(1, repo)  # the name is taken; the developer decides


def test_round_tag_refuses_another_checkout(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(common.Refused, match="another checkout"):
        round_tag.check_cwd()


def test_exec_under_suite_env_uses_no_shell(tmp_path: Path, capsys, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-not-for-children")
    code = common.exec_under_suite_env(
        tmp_path, [sys.executable, "-c", "import os; print(sorted(os.environ))"]
    )
    out = capsys.readouterr().out
    assert code == 0
    seen = set(eval(out.strip()))  # the interpreter adds LC_CTYPE etc. on macOS
    assert {"CTV_INT", "HOME", "PATH", "PYTHONPATH"} <= seen
    assert "ANTHROPIC_API_KEY" not in seen
