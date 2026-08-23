#!/usr/bin/env python3
"""The round boundary: `review-round-N` annotated tags, written and read by CODE.

DECISIONS "Process" (2026-08-23): model-written text never reaches an oracle;
the tag carries a round number and nothing else. The tag is a RANGE BOUNDARY —
round N+1 reviews `review-round-N..HEAD` — so the only things that matter are
that it names the right round and that it is on this branch's history.

    round_tag.py write N   → tags HEAD `review-round-N` with the message `round=N`
    round_tag.py read N    → prints `round=N` after the anchored parse + ancestry check

`read` is a parse error (exit 2) when the tag is missing, its message is not
exactly the one line `round=N`, or it is not an ancestor of HEAD (a tag from
another branch is not this branch's boundary — `git merge-base --is-ancestor`).
Never a default. No cap field, no counts: the two-round cap is the architect's
call, made by comparing round N's table to round N−1's (CLAUDE.md Workflow
rules). Both commands refuse to run outside this repo's checkout (the script
binds ROOT; a second checkout must not tag this one). Never pushes; not a
pytest file."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from review_common import ROOT, Refused, die, run  # noqa: E402

_LINE = re.compile(r"^round=([1-9][0-9]*)$")


def tag_name(n: int) -> str:
    return f"review-round-{n}"


def check_cwd(root: Path = ROOT) -> None:
    """Refuse unless the caller's cwd is inside THIS checkout (ROOT is bound to
    the script's repo; a second checkout invoking it would tag this one)."""
    code, out = run(["git", "rev-parse", "--show-toplevel"], Path.cwd())
    if code != 0 or Path(out.strip()).resolve() != root.resolve():
        raise Refused(f"refusing: run from inside {root} (cwd is another checkout)")


def parse(message: str, n: int) -> int:
    """Anchored parse: exactly the one line `round=N`, N matching. git appends
    one newline to the stored message and `tag -l` one more; both stripped."""
    if message.endswith("\n"):
        message = message[:-1]
    if message.endswith("\n"):
        message = message[:-1]
    m = _LINE.match(message)
    if not m or "\n" in message:
        raise Refused(
            f"parse error: {tag_name(n)} message is not `round={n}`: {message!r}"
        )
    if int(m.group(1)) != n:
        raise Refused(
            f"parse error: {tag_name(n)} names round {m.group(1)}, expected {n}"
        )
    return n


def read(n: int, root: Path = ROOT) -> int:
    code, out = run(["git", "tag", "-l", "--format=%(contents)", tag_name(n)], root)
    if code != 0 or not out.strip():
        raise Refused(
            f"parse error: tag {tag_name(n)} is missing — not a round boundary"
        )
    parse(out, n)
    code, _ = run(["git", "merge-base", "--is-ancestor", tag_name(n), "HEAD"], root)
    if code != 0:
        raise Refused(
            f"parse error: {tag_name(n)} is not an ancestor of HEAD — another "
            "branch's round, not this one's boundary"
        )
    return n


def write(n: int, root: Path = ROOT) -> None:
    _, out = run(["git", "tag", "-l", tag_name(n)], root)
    if out.strip():
        raise Refused(f"refusing: {tag_name(n)} exists — a round is reviewed once")
    code, out = run(["git", "tag", "-a", tag_name(n), "HEAD", "-m", f"round={n}"], root)
    if code != 0:
        raise Refused(f"git tag failed: {out.strip()}")
    read(n, root)  # read back what was written, through the same parser


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("write").add_argument("n", type=int)
    sub.add_parser("read").add_argument("n", type=int)
    a = ap.parse_args(argv)
    try:
        check_cwd()
        if a.n < 1:
            raise Refused("refusing: N must be ≥ 1")
        if a.cmd == "write":
            write(a.n)
            print(f"tagged {tag_name(a.n)} (local, annotated, message `round={a.n}`)")
        else:
            print(f"round={read(a.n)}")
    except Refused as e:
        die(str(e))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # one line, never a traceback
        die(f"round_tag error: {type(e).__name__}: {e}", 1)
