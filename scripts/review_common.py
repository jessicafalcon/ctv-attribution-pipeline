"""Shared by scripts/review_gate.py, scripts/mutate.py and scripts/round_tag.py
(not a pytest file).

One spec-path validator, one section parser, one subprocess runner, one reduced
child environment (`suite_env`) — and `review_common.py exec <tree> -- <cmd…>`,
which runs a command under that environment with NO shell in between (the
functionality-tester's hand-mutation recipe; `mutate.py` calls `suite_env`
directly). No package beyond the stdlib (subprocess, pathlib)."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class Refused(Exception):
    """A one-line refusal: printed as-is, exit 2, never a traceback."""


def resolve_spec(arg: str, root: Path = ROOT) -> Path:
    """`--spec` must name an existing FILE under `<root>/specs/`; nothing else is
    derived from it (no profile, no root, no glob). Empty, absolute, `../x`, a
    directory, a symlink out of specs/, or anything outside specs/ is refused."""
    if not arg or not arg.strip():
        raise Refused("refusing: SPEC is empty (usage: SPEC=specs/<file>.md)")
    specs = (root / "specs").resolve()
    target = (root / arg).resolve() if not Path(arg).is_absolute() else Path(arg)
    if Path(arg).is_absolute() or specs not in target.parents:
        raise Refused(f"refusing: SPEC must be a path under specs/, got {arg!r}")
    if not target.is_file():
        raise Refused(f"refusing: SPEC is not an existing file: {arg!r}")
    return target


def section(text: str, heading: str) -> str:
    """Body of the first `## <heading>…` section (heading matched as a prefix, so
    `Evidence (REQUIRED)` is found by `Evidence`); '' if absent."""
    m = re.search(rf"^## {re.escape(heading)}.*?$(.*?)(?=^## |\Z)", text, re.M | re.S)
    return m.group(1) if m else ""


def run(
    cmd: list[str], cwd: Path, env: dict[str, str] | None = None
) -> tuple[int, str]:
    """Run `cmd`, capture stdout+stderr merged, never raise on non-zero."""
    res = subprocess.run(
        cmd, cwd=cwd, capture_output=True, text=True, env=env, stdin=subprocess.DEVNULL
    )
    return res.returncode, res.stdout + res.stderr


def suite_env(tree: Path, ctv_int: str = "0") -> dict[str, str]:
    """The ONE environment a SPEC-SHAPED child pytest gets — mutate's suite and the
    gate's --collect-only; `make test` / ruff children inherit the parent env on
    purpose, same trust as typing them. PATH, HOME, PYTHONPATH=tree, and CTV_INT:
    "0" by default (the sweep EXECUTES tests — integration stays skipped by the
    marker AND by `--ignore=tests/integration`, two independent pinned guards);
    the gate passes "1" so --collect-only can list integration ids (collects,
    never executes). Never credentials (security review, PR #35, rounds 1-2)."""
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", str(Path.home())),
        "PYTHONPATH": str(tree),
        "CTV_INT": ctv_int,
    }


def tail(out: str, n: int = 20) -> str:
    lines = out.rstrip().splitlines()
    return "\n".join("    " + ln for ln in lines[-n:])


def die(msg: str, code: int = 2) -> None:
    print(msg)
    sys.exit(code)


def exec_under_suite_env(tree: Path, cmd: list[str]) -> int:
    """Run `cmd` with cwd=tree under `suite_env(tree)`; forward its exit code.
    argv list, never a shell — `env -i $(…)` word-split a HOME with a space into
    a different command (security review, PR #35, cap commit)."""
    code, out = run(cmd, tree, env=suite_env(tree))
    sys.stdout.write(out)
    return code


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "exec" and sys.argv[3] == "--":
        sys.exit(exec_under_suite_env(Path(sys.argv[2]), sys.argv[4:]))
    die("usage: review_common.py exec <worktree> -- <command…>")
