#!/usr/bin/env python3
"""The mutation sweep (Phase-18a retrospective: the strongest gate findings came
from "delete this call — does the suite notice?"). Run via `make mutate SPEC=…`.

Reads the spec's Invariants section for ONE fenced block:

    ```mutations
    lake/load_serving.py::insert_attributed      delete-call
    reconcile/rollup.py::refresh_campaign_hourly constant-return:None
    reconcile/reconcile.py::pick_household       invert-guard
    reconcile/reconcile.py::pick_household       swap-sort-key
    ```

Exactly four operators:
  delete-call         remove every statement-level call to the function, in every
                      tracked non-test .py file (a call used as a value is left;
                      none found → ERROR)
  constant-return:<v> the function returns the literal <v> before doing anything
  invert-guard        negate the first `if` test inside the function
  swap-sort-key       reverse the first `sorted(…, key=lambda …: (a, b))` /
                      `.sort(key=…)` / `max(…, key=…)` / `min(…, key=…)` tuple
                      key in the function; else the column list after the first
                      `order by` in a string constant

Each mutation: `git worktree add --detach <tmp>/mutate-*/mut-N HEAD` (the system
temp dir; the working tree is never touched; the mutation is applied to HEAD), the
offline suite runs there with THIS interpreter and a REDUCED environment
(`review_common.suite_env`: PATH, HOME, PYTHONPATH, CTV_INT — never credentials),
the worktree is removed in a `finally` that also asserts `git worktree list` is
unchanged (stale ones pruned at start). Verdicts: KILLED / SURVIVED / ERROR, one
per mutation, summing to the mutation count; a registry change is a separate
latched REGISTRY line, reported once, failing the run. One line per
mutation — KILLED (suite red) or SURVIVED (suite green) — plus file:line; exit 1
if any SURVIVED or ERROR. Uses ast/subprocess/pathlib only. Not a pytest file."""

from __future__ import annotations

import argparse
import ast
import posixpath
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

sys.path.insert(0, str(Path(__file__).resolve().parent))
from review_common import (  # noqa: E402
    ROOT,
    Refused,
    die,
    resolve_spec,
    run,
    section,
    suite_env,
)

OPERATORS = ("delete-call", "constant-return", "invert-guard", "swap-sort-key")
MAX_LITERAL = 64  # chars; `constant-return:<v>` is a small literal, nothing else
_BLOCK = re.compile(r"```mutations\n(.*?)```", re.S)
_ORDER_BY = re.compile(r"(order by\s+)([^\n;)]+?)(\s*(?:limit\b|$))", re.I | re.M)
SUITE = [
    sys.executable,
    "-m",
    "pytest",
    "-q",
    "-x",
    "-p",
    "no:cacheprovider",
    "--ignore=tests/integration",
]


@dataclass(frozen=True)
class Mutation:
    file: str
    func: str
    op: str
    arg: str = ""

    def __str__(self) -> str:
        op = f"{self.op}:{self.arg}" if self.arg else self.op
        return f"{self.file}::{self.func} {op}"


def parse_mutations(spec_text: str) -> list[Mutation]:
    m = _BLOCK.search(section(spec_text, "Invariants"))
    if not m:
        raise Refused(
            "refusing: the spec's Invariants section has no ```mutations block — "
            "add `## Invariants` with one (shape: specs/TEMPLATE.md)"
        )
    out: list[Mutation] = []
    for ln in m.group(1).splitlines():
        if not ln.strip() or ln.lstrip().startswith("#"):
            continue
        parts = ln.split()
        if len(parts) != 2 or "::" not in parts[0]:
            raise Refused(
                f"refusing: bad mutations line (want `path.py::func op`): {ln!r}"
            )
        file, func = parts[0].split("::", 1)
        op, _, arg = parts[1].partition(":")
        if op not in OPERATORS or (op == "constant-return") != bool(arg):
            raise Refused(
                f"refusing: unknown operator {parts[1]!r} "
                f"(one of {', '.join(OPERATORS)})"
            )
        if arg:
            _literal_or_refuse(arg)
        out.append(Mutation(_repo_path(file), func, op, arg))
    if not out:
        raise Refused("refusing: the mutations block is empty")
    return out


def _repo_path(file: str) -> str:
    """A mutation target, resolved ONCE and gated on `Path.parts`, never on a
    string prefix (DECISIONS "Process", the model-text invariant): `./tests/x.py`,
    `tests/../lake/x.py` and `lake/./x.py` all normalize first (posixpath, so the
    rule is platform-independent), then the rules — relative, no `..`, `.py`, not
    under tests/ (casefolded: macOS is case-insensitive) — apply to the parts."""
    parts = PurePosixPath(posixpath.normpath(file)).parts
    if not parts or Path(file).is_absolute() or ".." in parts or parts[0] == "..":
        raise Refused(
            f"refusing: mutation target must be a repo-relative path: {file!r}"
        )
    if not parts[-1].endswith(".py"):
        raise Refused(f"refusing: mutation target must be a .py file: {file!r}")
    if parts[0].casefold() == "tests":  # the filesystem may not be case-sensitive
        raise Refused(
            f"refusing: {file!r} is under tests/ — the sweep never mutates the "
            "oracle it judges by (every operator, not only delete-call)"
        )
    return "/".join(parts)


def _literal_or_refuse(arg: str) -> None:
    """`<v>` is written into a source file that the sweep's pytest then imports —
    so it must be a Python LITERAL (`0`, `None`, `'x'`, `(1, 2)`), never an
    expression. Spec text is model-authored; it never reaches exec."""
    if len(arg) > MAX_LITERAL:
        raise Refused(
            f"refusing: constant-return value longer than {MAX_LITERAL} chars"
        )
    try:
        ast.literal_eval(arg)
    except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
        raise Refused(
            f"refusing: constant-return value is not a literal: {arg!r}"
        ) from None


# -------------------------------------------------------- text surgery


class Source:
    """Line-level edits guided by ast spans, so everything around the mutation
    keeps its formatting (ast.unparse of a whole file would reformat it)."""

    def __init__(self, path: Path, tree: Path) -> None:
        # A tracked symlink could point out of the worktree; the write must not.
        if tree.resolve() not in path.resolve().parents:
            raise Refused(f"refusing: {path.name} resolves outside the worktree")
        self.path = path
        self.text = path.read_text()
        self.lines = self.text.splitlines(keepends=True)
        self.tree = ast.parse(self.text)

    def function(self, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
        for node in ast.walk(self.tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name
            ):
                return node
        raise Refused(f"refusing: no function {name!r} in {self.path.name}")

    def replace_span(self, node: ast.AST, new: str) -> int:
        """Replace node's source span with `new`; returns the first line (1-based)."""
        l0, c0, l1, c1 = (
            node.lineno,
            node.col_offset,
            node.end_lineno,
            node.end_col_offset,
        )
        head = self.lines[l0 - 1][:c0]
        tail = self.lines[l1 - 1][c1:]
        self.lines[l0 - 1 : l1] = [head + new + tail]
        return l0

    def delete_stmt(self, node: ast.stmt) -> int:
        indent = self.lines[node.lineno - 1][: node.col_offset]
        self.lines[node.lineno - 1 : node.end_lineno] = [indent + "pass  # MUTATED\n"]
        return node.lineno

    def write(self) -> None:
        self.path.write_text("".join(self.lines))


def _calls(name: str) -> bool:
    def pred(node: ast.AST) -> bool:
        if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
            return False
        f = node.value.func
        return (isinstance(f, ast.Name) and f.id == name) or (
            isinstance(f, ast.Attribute) and f.attr == name
        )

    return pred


def apply(m: Mutation, tree: Path) -> str:
    """Apply `m` inside worktree `tree`; return `file:line` of the edit."""
    if m.op == "delete-call":
        pred = _calls(m.func)
        hits: list[str] = []
        _, files = run(["git", "ls-files", "--", "*.py"], tree)
        for rel in files.split():
            if rel.startswith("tests/"):
                continue
            src = Source(tree / rel, tree)
            stmts = [n for n in ast.walk(src.tree) if pred(n)]
            for n in sorted(
                stmts, key=lambda n: -n.lineno
            ):  # bottom-up keeps spans valid
                hits.append(f"{rel}:{src.delete_stmt(n)}")
            if stmts:
                src.write()
        if not hits:
            raise Refused(
                f"refusing: no statement-level call to {m.func}() outside tests/"
            )
        return ",".join(sorted(hits))
    code, _ = run(["git", "ls-files", "--error-unmatch", "--", m.file], tree)
    if code != 0:
        raise Refused(f"refusing: {m.file} is not a tracked file")
    src = Source(tree / m.file, tree)
    fn = src.function(m.func)
    if m.op == "constant-return":
        first = fn.body[0]
        indent = src.lines[first.lineno - 1][: first.col_offset]
        src.lines.insert(first.lineno - 1, f"{indent}return {m.arg}  # MUTATED\n")
        line = first.lineno
    elif m.op == "invert-guard":
        ifs = [n for n in ast.walk(fn) if isinstance(n, ast.If)]
        if not ifs:
            raise Refused(f"refusing: no `if` inside {m.func}()")
        test = min(ifs, key=lambda n: (n.lineno, n.col_offset)).test
        line = src.replace_span(test, f"not ({ast.unparse(test)})")
    elif m.op == "swap-sort-key":
        line = _swap_sort_key(src, fn, m)
    else:  # pragma: no cover — parse_mutations rejects anything else
        raise Refused(f"refusing: unknown operator {m.op}")
    src.write()
    return f"{m.file}:{line}"


def _swap_sort_key(src: Source, fn: ast.AST, m: Mutation) -> int:
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        is_sort = (isinstance(f, ast.Name) and f.id in ("sorted", "max", "min")) or (
            isinstance(f, ast.Attribute) and f.attr == "sort"
        )
        if not is_sort:
            continue
        for kw in node.keywords:
            if kw.arg == "key" and isinstance(kw.value, ast.Lambda):
                body = kw.value.body
                if isinstance(body, ast.Tuple) and len(body.elts) > 1:
                    swapped = ast.Tuple(elts=list(reversed(body.elts)), ctx=ast.Load())
                    return src.replace_span(body, ast.unparse(swapped))
    for node in ast.walk(fn):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            mm = _ORDER_BY.search(node.value)
            if mm:
                cols = [c.strip() for c in mm.group(2).split(",")]
                if len(cols) > 1:
                    new = (
                        node.value[: mm.start(2)]
                        + ", ".join(reversed(cols))
                        + node.value[mm.end(2) :]
                    )
                    return src.replace_span(node, repr(new))
    raise Refused(
        f"refusing: no multi-key sorted()/sort()/`order by` inside {m.func}()"
    )


# -------------------------------------------------------------- worktree


def sweep(
    mutations: list[Mutation], root: Path, scratch: Path, suite: list[str]
) -> int:
    """Run every mutation; print one line each; return the exit code."""
    survivors = 0
    errors = 0
    registry_changed = False  # latched: its own outcome, never a mutation verdict
    run(["git", "worktree", "prune"], root)  # a SIGKILLed earlier sweep leaves one
    _, before = run(["git", "worktree", "list"], root)
    for i, m in enumerate(mutations, 1):
        tree = scratch / f"mut-{i}"
        try:
            code, out = run(
                ["git", "worktree", "add", "--detach", "-q", str(tree), "HEAD"], root
            )
            if code != 0:
                print(
                    f"ERROR    {m}: git worktree add failed: "
                    f"{out.strip().splitlines()[-1]}"
                )
                errors += 1
                continue
            try:
                where = apply(m, tree)
            except (Refused, SyntaxError, OSError) as e:
                print(f"ERROR    {m}: {e}")
                errors += 1
                continue
            code, _ = run(suite, tree, env=suite_env(tree))
            verdict = "KILLED  " if code != 0 else "SURVIVED"
            survivors += code == 0
            print(f"{verdict} {m} -> {where}")
        finally:
            run(["git", "worktree", "remove", "--force", str(tree)], root)
            run(["git", "worktree", "prune"], root)
            _, after = run(["git", "worktree", "list"], root)
            if after != before and not registry_changed:
                # `git status` cannot see .git/worktrees; this can. Reported once.
                print(f"REGISTRY worktree registry changed after {m}:\n{after}")
                registry_changed = True
    killed = len(mutations) - survivors - errors  # the triple always sums to len
    assert killed >= 0
    bad = survivors + errors + registry_changed
    print(
        f"mutate {'FAIL' if bad else 'OK'}: {killed}/{len(mutations)} killed, "
        f"{survivors} survived, {errors} errors"
        + (", worktree registry changed" if registry_changed else "")
    )
    return 1 if bad else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--spec", required=True, help="specs/<file>.md with a ```mutations block"
    )
    a = ap.parse_args(argv)
    try:
        spec = resolve_spec(a.spec)
        mutations = parse_mutations(spec.read_text())
    except Refused as e:
        die(str(e))
    scratch = Path(tempfile.mkdtemp(prefix="mutate-"))  # system temp; no env knob
    try:
        return sweep(mutations, ROOT, scratch, SUITE)
    finally:
        try:
            scratch.rmdir()  # empty once every worktree was removed
        except OSError:
            pass


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # one line, never a traceback
        die(f"mutate error: {type(e).__name__}: {e}", 1)
