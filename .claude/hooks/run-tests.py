#!/usr/bin/env python3
# .claude/hooks/run-tests.py
# PostToolUse hook (matcher: Write|Edit|MultiEdit|NotebookEdit) — runs pytest
# after a .py file in this project changes. Makes a broken test VISIBLE the
# instant it breaks. Python-based so it doesn't depend on jq.
#
# This gate deliberately fails OPEN (missing pytest, malformed event →
# allow): it is a visibility aid, not a security control. A hung suite
# (timeout) DOES block — silence there would hide a real problem. Don't
# "fix" the open cases into fail-closed — that would block all edits on a
# broken venv.
import json
import os
import subprocess
import sys


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        sys.exit(0)

    ti = data.get("tool_input", {}) or {}
    fp = ti.get("file_path", "") or ""
    if not fp.lower().endswith(".py"):
        sys.exit(0)  # not Python — skip fast (silently; only "tests green" means "ran")

    root = os.environ.get("CLAUDE_PROJECT_DIR", "")
    if not root:
        sys.exit(0)
    # Only react to files inside THIS project — an edit in a sibling repo
    # would otherwise produce a misleading green from a suite that never
    # covers it.
    if not os.path.abspath(fp).startswith(os.path.abspath(root) + os.sep):
        sys.exit(0)
    try:
        os.chdir(root)
    except OSError:
        sys.exit(0)

    # Prefer the project venv's pytest; else pytest on PATH; else skip quietly.
    venv_pytest = os.path.join(".venv", "bin", "pytest")
    if os.path.exists(venv_pytest):
        cmd = [venv_pytest, "-q"]
    else:
        from shutil import which

        if which("pytest"):
            cmd = ["pytest", "-q"]
        else:
            print(
                "[run-tests] pytest not found yet — set up the venv to enable.",
                file=sys.stderr,
            )
            sys.exit(0)

    try:
        res = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120, check=False
        )
    except subprocess.TimeoutExpired:
        print(
            f"[run-tests] suite timed out (120s) after editing {fp}.", file=sys.stderr
        )
        sys.exit(2)
    # pytest exit code 5 = "no tests collected" — not a failure, just an empty/
    # early repo. Don't block on it, or every .py edit before tests exist alarms.
    if res.returncode == 5:
        sys.exit(0)
    if res.returncode != 0:
        print(f"[run-tests] TESTS FAILING after editing {fp}:", file=sys.stderr)
        tail = (res.stdout + res.stderr).splitlines()[-15:]
        print("\n".join(tail), file=sys.stderr)
        sys.exit(2)

    print(f"[run-tests] tests green after {fp}.", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
