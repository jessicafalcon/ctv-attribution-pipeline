#!/usr/bin/env python3
"""The offline review gate (Phase-18a retrospective; DECISIONS "Process").

One process, one line per check, exit 1 on any FAIL, never a traceback. Run via
`make review-gate [SPEC=specs/<file>.md] [BASE=main] [DELETED=a,b]` — the first
thing `/review-round N` does, before any agent is spawned.

  a. `make test`; `ruff check` + `ruff format --check` (read-only — never
     `make lint`, whose ruff-format hook rewrites files) (last 20 lines on red)
  b. `make check-docs`
  c. Evidence rows   — every `tests/….py::test_x` and `make <target>` the spec's
                       Evidence section names must exist (pytest --collect-only;
                       the Makefile's declared targets — never `make -n`). --spec.
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
    suite_env,
    tail,
)

_TEST_ID = re.compile(r"(tests/[\w./-]+\.py)?::(test_\w+)")
_MAKE = re.compile(r"`make ([a-z][a-z0-9-]*)[^`]*`")  # backticked form ONLY
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


def check_lint(root: Path) -> bool:
    """Read-only lint. NOT `make lint`: pre-commit's ruff-format hook rewrites
    files in place — in exactly the failing case — and a gate must not share a
    medium with the tree it judges (PR #35 coherence audit r1-B7)."""
    ok = True
    for name, cmd in (
        ("ruff check", ["uv", "run", "ruff", "check", "."]),
        ("ruff format --check", ["uv", "run", "ruff", "format", "--check", "."]),
    ):
        code, out = run(cmd, root)
        if code == 0:
            print(f"PASS {name}")
        else:
            print(f"FAIL {name} (exit {code}); last lines:\n{tail(out)}")
            ok = False
    return ok


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
    # `-o addopts=` clears the repo's `addopts = "-q"` (pyproject) so exactly ONE
    # `-q` reaches pytest. Two `-q` (pyproject's + ours) is quiet level 2, and under
    # pytest 9 `--collect-only -qq` prints a terse `path: count` summary with no
    # `::` node ids — so the parser below collected NOTHING and every Evidence id
    # looked missing (fix/review-gate-pytest9). One `-q` prints node ids on 8 and 9.
    # Safe to clear: repo addopts carries only `-q`; testpaths / pythonpath are
    # separate ini keys, and this call already passes `-p no:cacheprovider` itself.
    code, out = run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-o",
            "addopts=",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        root,
        env=suite_env(root, ctv_int="1"),
    )
    ids: set[str] = set()
    for ln in out.splitlines():
        if "::" in ln and not ln.startswith(" "):
            path, _, rest = ln.partition("::")
            ids.add(f"{path}::{rest.split('[', 1)[0].split('::')[-1]}")
            ids.add(f"{path}::{rest.split('[', 1)[0]}")
    return ids


def make_targets(root: Path) -> set[str]:
    """Target names declared in the Makefile: every whitespace-separated name left
    of the first `:` on a column-0 rule line (`a b:` declares two; `name:=v` and
    `name :=` are assignments, not rules). A set lookup, never `make -n <name>`:
    `-n` still recurses through `$(MAKE)` lines (the test-int-* recipes run
    `$(MAKE) down` / `lake-reset CONFIRM=yes`), so existence-checking a name
    scraped from spec text must not invoke make. The derivation is pinned
    against the `.PHONY` line (tests/test_review_tools.py), not trusted."""
    targets: set[str] = set()
    mk = root / "Makefile"
    if not mk.exists():
        return targets
    for line in mk.read_text().splitlines():
        m = re.match(r"^([A-Za-z0-9_.%/ -]+?)\s*:(?!=)", line)
        if m and not line.startswith((" ", "\t", "#", ".PHONY")):
            targets.update(n for n in m.group(1).split() if not n.startswith("."))
    return targets


def check_evidence(spec_text: str, root: Path) -> bool:
    tests, targets = evidence_ids(spec_text)
    if not tests and not targets:
        print(
            "FAIL evidence: the spec's Evidence section names no test id or make target"
        )
        return False
    ids = collected_ids(root)
    known = make_targets(root)
    ok = True
    if tests and not ids:
        # Collection produced no ids at all while the spec names test ids. That is a
        # GATE defect (e.g. a pytest --collect-only format drift), not an evidence
        # defect — reporting every named id as "missing" here would be the mirror of
        # the vacuous-green pattern this repo keeps finding: vacuous-RED that hides
        # the real cause. Fail loud on the cause instead (fix/review-gate-pytest9).
        print(
            "FAIL evidence: collection produced no ids — gate defect, not evidence "
            "defect (check `pytest --collect-only` output format)"
        )
        return False
    for t in tests:
        if t not in ids:
            print(f"FAIL evidence: named test does not exist: {t}")
            ok = False
    for m in targets:
        if m not in known:
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


def _historical(hit: str) -> bool:
    """The two sanctioned history forms (check_docs._living) exempt a hit ONLY in
    a markdown file: `~~` is a prose convention, never a reason to skip a line of
    Python (`~~x` is legal) — security review, PR #35 round 2."""
    path = hit.split(":", 1)[0]
    return path.endswith(".md") and ("~~" in hit or "<!-- historical -->" in hit)


def check_deleted(symbols: list[str], root: Path, spec: Path | None) -> bool:
    """`git grep -F -w`: the symbol is a literal, never a regex (a `[` used to
    produce "1 hits" whose one hit was git's own `fatal: brackets not balanced`).
    A git error is its own FAIL line, not a hit count."""
    ok = True
    for sym in symbols:
        code, out = run(["git", "grep", "-F", "-n", "-w", "-e", sym, "--", "."], root)
        if code >= 2:  # 0 = hits, 1 = none, 2+/128+ = git itself failed
            print(
                f"FAIL deleted symbol: git grep error for {sym!r}: "
                f"{tail(out, 1).strip()}"
            )
            ok = False
            continue
        hits = [
            ln
            for ln in out.splitlines()
            if ln.strip()
            and not (spec and ln.startswith(str(spec.relative_to(root)) + ":"))
            and not _historical(ln)
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
            check_lint(ROOT),
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
