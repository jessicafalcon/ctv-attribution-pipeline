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
