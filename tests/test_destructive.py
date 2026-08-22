"""lake/destructive.py: one process validates → confirms → acts (Phase-17 review,
round 3). Python-level pins, run in a sandbox cwd so a guard failure could only
touch the sandbox. The Makefile side (one-line recipes, `$(origin CONFIRM)`,
`make -i`) is pinned in tests/test_makefile.py.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
INJECTION = 'x" ; touch pwned ; echo "'


def _run(tmp_path: Path, *args: str, env: dict | None = None, stdin=subprocess.DEVNULL):
    (tmp_path / "data" / "lake" / "tiny").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "x").mkdir(exist_ok=True)
    clean = {
        k: v
        for k, v in os.environ.items()
        if k not in {"LAKE_ROOT", "PYTEST_CURRENT_TEST"}
    }
    return subprocess.run(
        [sys.executable, "-m", "lake.destructive", *args],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env={**clean, "PYTHONPATH": str(REPO_ROOT), **(env or {})},
        stdin=stdin,
    )


@pytest.mark.parametrize("bad", ["", "../x", "/abs", "Tiny", "a b", INJECTION, "x\ny"])
def test_reset_refuses_a_malformed_profile_before_anything(
    tmp_path: Path, bad: str
) -> None:
    res = _run(tmp_path, "reset", "--profile", bad, "--yes")
    assert res.returncode != 0
    assert "refusing" in res.stderr
    assert not (tmp_path / "pwned").exists()
    assert (tmp_path / "data" / "lake" / "tiny").exists() and (
        tmp_path / "data" / "x"
    ).exists()


def test_reset_without_yes_aborts_on_a_non_tty(tmp_path: Path) -> None:
    res = _run(tmp_path, "reset", "--profile", "tiny")
    assert res.returncode == 1 and "aborted" in res.stdout
    assert (tmp_path / "data" / "lake" / "tiny").exists()


def test_reset_with_yes_removes_exactly_the_profile_lake(tmp_path: Path) -> None:
    res = _run(tmp_path, "reset", "--profile", "tiny", "--yes")
    assert res.returncode == 0, res.stderr
    assert not (tmp_path / "data" / "lake" / "tiny").exists()
    assert (tmp_path / "data" / "x").exists()


def test_reset_that_cannot_remove_fails_loud_not_green(tmp_path: Path) -> None:
    # A clean-stack target that prints "removed" over a lake it could not remove
    # would load that stale lake and shift the pins silently (round 4). Make the
    # root unremovable (a non-writable subdirectory holding a file) and assert a
    # non-zero exit with no "removed".
    import os
    import stat

    locked = tmp_path / "data" / "lake" / "tiny" / "locked"
    locked.mkdir(parents=True)
    (locked / "f").write_text("x")
    locked.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        res = _run(tmp_path, "reset", "--profile", "tiny", "--yes")
    finally:
        locked.chmod(stat.S_IRWXU)
    if os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    assert res.returncode != 0
    assert "removed" not in res.stdout
    assert (tmp_path / "data" / "lake" / "tiny").exists()


def test_lake_root_env_is_refused_outside_pytest(tmp_path: Path) -> None:
    # LAKE_ROOT is a test-only override; an entry point refuses it.
    res = _run(tmp_path, "reset", "--profile", "tiny", "--yes", env={"LAKE_ROOT": "/"})
    assert res.returncode != 0 and "test-only" in res.stderr
    assert (tmp_path / "data" / "lake" / "tiny").exists()


def test_replay_refuses_an_empty_lake_before_the_prompt(tmp_path: Path) -> None:
    res = _run(tmp_path, "replay", "--profile", "tiny", "--yes")
    assert (
        res.returncode != 0
        and "refusing" in res.stderr
        and "holds no days" in res.stderr
    )
    assert not (tmp_path / "data" / "lake" / "tiny" / "catalog.db").exists()


def test_confirm_or_abort_answers(monkeypatch, capsys) -> None:
    from lake import destructive as d

    d.confirm_or_abort("x", yes=True)  # no prompt
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt: "yes")
    d.confirm_or_abort("x", yes=False)
    monkeypatch.setattr("builtins.input", lambda prompt: "n")
    with pytest.raises(SystemExit):
        d.confirm_or_abort("x", yes=False)
    assert "aborted" in capsys.readouterr().out

    def eof(prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", eof)
    with pytest.raises(SystemExit):
        d.confirm_or_abort("x", yes=False)
