#!/usr/bin/env python3
"""The round record: `review-round-N` annotated tags, written and read by CODE.

DECISIONS "Process" (2026-08-23): model-written text reaches a control decision
only through fixed fields a script parses; never free text. `/review-round`
calls this script — it never composes or parses a tag message itself.

    round_tag.py write N --range R --agents a,b --correctness K --cap yes|no|n/a
                 --gate "review-gate:OK mutate:k/s/e"       → tags HEAD, local only
    round_tag.py read N                                      → prints the six fields
    round_tag.py cap N --this yes|no|n/a                     → CAP | cap watch | no cap

The message is EXACTLY six `key=value` lines, each value matched by its own
pattern; a trailing blank line (git adds one) is stripped before the anchored
parse; anything else — a seventh line, a missing key, a value outside its
pattern, finding text anywhere — is a parse error: exit 2, never a default.
Round 1 writes `cap=n/a` (no previous fixes) and `cap N` with N=1 never reads a
tag; N ≥ 2 reads `review-round-(N−1)`. `correctness=0` forces `cap=no`: the cap is
about findings INSIDE the previous diff, and no findings is no evidence. Two
clean rounds therefore print "no cap". Never pushes; never a pytest file."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from review_common import ROOT, Refused, die, run  # noqa: E402

KEYS = ("round", "range", "agents", "correctness", "cap", "gate")
PATTERNS = {
    "round": re.compile(r"^[1-9][0-9]*$"),
    "range": re.compile(r"^[A-Za-z0-9_./-]+\.{2,3}[A-Za-z0-9_./-]+$"),
    "agents": re.compile(r"^[a-z-]+(,[a-z-]+)*$"),
    "correctness": re.compile(r"^(0|[1-9][0-9]*)$"),
    "cap": re.compile(r"^(yes|no|n/a)$"),
    "gate": re.compile(
        r"^review-gate:(OK|FAIL) mutate:"
        r"(0|[1-9][0-9]*)/(0|[1-9][0-9]*)/(0|[1-9][0-9]*)$"
    ),
}
CAP_LINE = "CAP: fixes are generating findings — write the invariant, re-implement once"
WATCH_LINE = "cap watch: one more such round trips the cap"


def tag_name(n: int) -> str:
    return f"review-round-{n}"


def parse(message: str, n: int | None = None) -> dict[str, str]:
    """Anchored parse of a tag message → the six fields; Refused on any deviation."""
    if message.endswith("\n"):  # git appends exactly ONE trailing newline
        message = message[:-1]
    lines = message.split("\n")
    if len(lines) != len(KEYS):
        raise Refused(f"parse error: expected {len(KEYS)} lines, got {len(lines)}")
    fields: dict[str, str] = {}
    for line, key in zip(lines, KEYS, strict=True):
        k, sep, v = line.partition("=")
        if not sep or k != key:
            raise Refused(
                f"parse error: line {len(fields) + 1} must be `{key}=…`, got {line!r}"
            )
        if not PATTERNS[key].match(v):
            raise Refused(f"parse error: {key}={v!r} does not match its pattern")
        fields[key] = v
    if n is not None and int(fields["round"]) != n:
        raise Refused(f"parse error: tag names round {fields['round']}, expected {n}")
    if fields["round"] == "1":
        if fields["cap"] != "n/a":
            raise Refused("parse error: round 1 has no previous fixes; cap must be n/a")
    elif fields["cap"] == "n/a":
        raise Refused("parse error: cap=n/a is round 1 only")
    elif fields["correctness"] == "0" and fields["cap"] != "no":
        raise Refused(
            "parse error: correctness=0 requires cap=no (no findings is no evidence)"
        )
    return fields


def compose(
    n: int, rng: str, agents: str, correctness: int, cap: str, gate: str
) -> str:
    fields = {
        "round": str(n),
        "range": rng,
        "agents": agents,
        "correctness": str(correctness),
        "cap": cap,
        "gate": gate,
    }
    message = "\n".join(f"{k}={fields[k]}" for k in KEYS)
    parse(message, n)  # the writer validates with the reader: one shape
    return message


def read(n: int, root: Path = ROOT) -> dict[str, str]:
    code, out = run(["git", "tag", "-l", "--format=%(contents)", tag_name(n)], root)
    if code != 0 or not out.strip():
        raise Refused(f"parse error: tag {tag_name(n)} is missing — not a round record")
    if out.endswith(
        "\n"
    ):  # `tag -l` terminates the entry; the stored message keeps its own
        out = out[:-1]
    return parse(out, n)


def write(n: int, message: str, root: Path = ROOT) -> None:
    code, out = run(["git", "tag", "-l", tag_name(n)], root)
    if out.strip():
        raise Refused(f"refusing: {tag_name(n)} exists — a round is reviewed once")
    code, out = run(["git", "tag", "-a", tag_name(n), "HEAD", "-m", message], root)
    if code != 0:
        raise Refused(f"git tag failed: {out.strip()}")
    assert read(n, root)["round"] == str(n)  # read back what was written


def cap_decision(n: int, this: str, root: Path = ROOT) -> str:
    """The two-round rule as code. Round 1: no PREV. N ≥ 2: PREV is the previous
    tag's cap, parsed anchored (a bad tag stops the command, never defaults)."""
    if n == 1:
        return "no cap (round 1: no previous fixes)"
    prev = read(n - 1, root)["cap"]
    if n >= 3 and this == "yes" and prev == "yes":
        return CAP_LINE
    if this == "yes":
        return WATCH_LINE
    return "no cap"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write")
    w.add_argument("n", type=int)
    w.add_argument("--range", required=True, dest="rng")
    w.add_argument("--agents", required=True)
    w.add_argument("--correctness", required=True, type=int)
    w.add_argument("--cap", required=True, choices=("yes", "no", "n/a"))
    w.add_argument("--gate", required=True)
    r = sub.add_parser("read")
    r.add_argument("n", type=int)
    c = sub.add_parser("cap")
    c.add_argument("n", type=int)
    c.add_argument("--this", required=True, choices=("yes", "no", "n/a"))
    a = ap.parse_args(argv)
    try:
        if a.cmd == "write":
            write(a.n, compose(a.n, a.rng, a.agents, a.correctness, a.cap, a.gate))
            print(f"tagged {tag_name(a.n)} (local, annotated, six fields)")
        elif a.cmd == "read":
            for k, v in read(a.n).items():
                print(f"{k}={v}")
        else:
            print(cap_decision(a.n, a.this))
    except Refused as e:
        die(str(e))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # one line, never a traceback
        die(f"round_tag error: {type(e).__name__}: {e}", 1)
