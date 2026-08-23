#!/usr/bin/env python3
"""The offline review gate (Phase-18a retrospective; DECISIONS "Process").

One process, one line per check, exit 1 on any FAIL, never a traceback. Run via
`make review-gate [SPEC=specs/<file>.md] [BASE=main] [DELETED=a,b]` — the first
thing `/review-round N` does, before any agent is spawned.

  a. `make test`, `make lint`          (subprocess; last 20 lines on red)
  b. `make check-docs`
  c. Evidence rows   — every `tests/….py::test_x` and `make <target>` the spec's
                       Evidence section names must exist (pytest --collect-only,
                       make -n). Needs --spec.
  d. Record updates  — every file on the spec's Record-updates list is in
                       `git diff --name-only <base>...HEAD` (three-dot: the
                       branch's own changes since the merge-base, so a main that
                       advanced under the branch adds nothing) (FAIL); every record
                       file in the diff that is NOT on the list is a WARN. --spec.
  e. Deleted symbols — each name in --deleted still found anywhere in the tracked
                       tree (except the spec itself, which names the deletion) is
                       a FAIL (self-review item 1).

Nothing here edits, commits, or fixes. Not a pytest file (the run-tests hook)."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from review_common import (  # noqa: E402
    ROOT,
    Refused,
    die,
    resolve_spec,
    run,
    section,
    tail,
)

_TEST_ID = re.compile(r"(tests/[\w./-]+\.py)?::(test_\w+)")
_MAKE = re.compile(r"\bmake ([a-z][a-z0-9-]*)")
_RECORD_LINE = re.compile(r"^\s*- \[[ xX]\]\s*(.*)$")
_TICK = re.compile(r"`([^`]+)`")
RECORD_FILES = re.compile(
    r"^(DECISIONS\.md|BACKLOG\.md|CLAUDE\.md|README\.md|docs/.*)$"
)


# ------------------------------------------------------------------ a, b


def check_make(target: str, root: Path) -> bool:
    code, out = run(["make", target], root)
    if code == 0:
        print(f"PASS make {target}")
        return True
    print(f"FAIL make {target} (exit {code}); last lines:\n{tail(out)}")
    return False


# --------------------------------------------------------------------- c


def evidence_ids(text: str) -> tuple[list[str], list[str]]:
    """(test ids, make targets) named in the Evidence section. A bare `::test_x`
    inherits the last file named in the same row (the specs' shorthand)."""
    tests: list[str] = []
    targets: set[str] = set()
    for row in section(text, "Evidence").splitlines():
        last_file = ""
        for file, name in _TEST_ID.findall(row):
            last_file = file or last_file
            if last_file:
                tests.append(f"{last_file}::{name}")
        targets.update(_MAKE.findall(row))
    return sorted(set(tests)), sorted(targets)


def collected_ids(root: Path) -> set[str]:
    code, out = run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        root,
        env={"CTV_INT": "1", "PATH": "/usr/bin:/bin", "HOME": str(Path.home())},
    )
    ids: set[str] = set()
    for ln in out.splitlines():
        if "::" in ln and not ln.startswith(" "):
            path, _, rest = ln.partition("::")
            ids.add(f"{path}::{rest.split('[', 1)[0].split('::')[-1]}")
            ids.add(f"{path}::{rest.split('[', 1)[0]}")
    return ids


def make_target_exists(target: str, root: Path) -> bool:
    code, _ = run(["make", "-n", "-s", target], root)
    return code != 2


def check_evidence(spec_text: str, root: Path) -> bool:
    tests, targets = evidence_ids(spec_text)
    if not tests and not targets:
        print(
            "FAIL evidence: the spec's Evidence section names no test id or make target"
        )
        return False
    ids = collected_ids(root)
    ok = True
    for t in tests:
        if t not in ids:
            print(f"FAIL evidence: named test does not exist: {t}")
            ok = False
    for m in targets:
        if not make_target_exists(m, root):
            print(f"FAIL evidence: named make target does not exist: make {m}")
            ok = False
    if ok:
        print(
            f"PASS evidence: {len(tests)} test ids + {len(targets)} make targets exist"
        )
    return ok


# --------------------------------------------------------------------- d


def record_list(text: str) -> list[str]:
    """Backticked paths on the `- [ ]` lines of the Record-updates section; a
    `/` or a `.md`/`Makefile` names a file, anything else is prose."""
    paths: list[str] = []
    for ln in section(text, "Record updates").splitlines():
        m = _RECORD_LINE.match(ln)
        if not m:
            continue
        for tok in _TICK.findall(m.group(1)):
            if "/" in tok or tok.endswith(".md") or tok == "Makefile":
                paths.append(tok.strip())
                break  # first path on the line is the record; later ticks are detail
    return paths


def _matches(entry: str, changed: list[str]) -> bool:
    if "*" in entry:
        pat = re.compile("^" + re.escape(entry).replace(r"\*", "[^/]*") + "$")
        return any(pat.match(c) for c in changed)
    return any(c == entry or c.startswith(entry.rstrip("/") + "/") for c in changed)


def check_records(spec_text: str, root: Path, base: str) -> bool:
    code, out = run(["git", "diff", "--name-only", f"{base}...HEAD"], root)
    if code != 0:
        print(f"FAIL records: git diff {base}...HEAD failed:\n{tail(out, 3)}")
        return False
    changed = [ln.strip() for ln in out.splitlines() if ln.strip()]
    listed = record_list(spec_text)
    if not listed:
        print("FAIL records: the spec's Record-updates section lists no file")
        return False
    ok = True
    for entry in listed:
        if not _matches(entry, changed):
            print(
                "FAIL records: listed in Record updates but absent from the diff: "
                f"{entry}"
            )
            ok = False
    for c in changed:
        if RECORD_FILES.match(c) and not any(_matches(e, [c]) for e in listed):
            print(f"WARN records: record file in the diff but not on the list: {c}")
    if ok:
        print(f"PASS records: {len(listed)} listed record files are in the diff")
    return ok


# --------------------------------------------------------------------- e


def check_deleted(symbols: list[str], root: Path, spec: Path | None) -> bool:
    ok = True
    for sym in symbols:
        code, out = run(["git", "grep", "-n", "-w", "-e", sym, "--", "."], root)
        hits = [
            ln
            for ln in out.splitlines()
            if ln.strip()
            and not (spec and ln.startswith(str(spec.relative_to(root)) + ":"))
            and "~~" not in ln  # the two sanctioned history forms (check_docs._living)
            and "<!-- historical -->" not in ln
        ]
        if hits:
            print(f"FAIL deleted symbol still referenced: {sym} ({len(hits)} hits)")
            for h in hits[:10]:
                print(f"    {h[:160]}")
            ok = False
        else:
            print(f"PASS deleted symbol gone: {sym}")
    return ok


# ------------------------------------------------------------------ main


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--spec", default="", help="specs/<file>.md (checks c, d)")
    ap.add_argument("--base", default="main", help="diff base for check d")
    ap.add_argument(
        "--deleted", default="", help="comma list of removed symbols (check e)"
    )
    ap.add_argument(
        "--skip-make", action="store_true", help=argparse.SUPPRESS
    )  # tests only
    a = ap.parse_args(argv)
    try:
        spec = resolve_spec(a.spec) if a.spec else None
    except Refused as e:
        die(str(e))
    results: list[bool] = []
    if not a.skip_make:
        results += [
            check_make("test", ROOT),
            check_make("lint", ROOT),
            check_make("check-docs", ROOT),
        ]
    if spec:
        text = spec.read_text()
        results += [check_evidence(text, ROOT), check_records(text, ROOT, a.base)]
    else:
        print("SKIP evidence, records: no --spec given")
    symbols = [s.strip() for s in a.deleted.split(",") if s.strip()]
    if symbols:
        results.append(check_deleted(symbols, ROOT, spec))
    else:
        print("SKIP deleted symbols: no --deleted given")
    red = results.count(False)
    print(
        f"review-gate {'FAIL' if red else 'OK'}: "
        f"{len(results) - red}/{len(results)} checks"
    )
    return 1 if red else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # one line, never a traceback (the gate reads lines)
        die(f"review-gate error: {type(e).__name__}: {e}", 1)
