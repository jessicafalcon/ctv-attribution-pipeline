"""Shared by scripts/review_gate.py and scripts/mutate.py (not a pytest file).

One spec-path validator, one section parser, one subprocess runner. No package
beyond the stdlib (ast, subprocess, pathlib)."""

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


def suite_env(tree: Path) -> dict[str, str]:
    """The ONE environment a child pytest gets (mutate's suite, the gate's
    --collect-only): PATH, HOME, PYTHONPATH=tree, CTV_INT=1 (collect-only never
    executes; the sweep's suite ignores tests/integration). Never the parent's full
    environment — credentials do not reach code a spec's text shaped (security
    review, PR #35: one path was reduced, the other inherited everything)."""
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", str(Path.home())),
        "PYTHONPATH": str(tree),
        "CTV_INT": "1",
    }


def tail(out: str, n: int = 20) -> str:
    lines = out.rstrip().splitlines()
    return "\n".join("    " + ln for ln in lines[-n:])


def die(msg: str, code: int = 2) -> None:
    print(msg)
    sys.exit(code)
