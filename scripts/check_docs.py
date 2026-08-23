#!/usr/bin/env python3
"""The one docs guard (Phase 19; was docs/check_runbook.py, Phase 15).

Standalone, no pytest, no services — run via `make check-docs` (the lint job runs
it too). Not a pytest file so a docs-only edit does not re-trigger the full suite.

Four checks (1-3 over README.md and every docs/*.md; 4 over CLAUDE.md/BACKLOG.md):
  1. Links/anchors — every relative markdown link points at a real file, and a
     `#anchor` resolves to a heading in that file (GitHub-style slug).
  2. Generated blocks — each `make`-regenerated block (`scale-curve`,
     `cost-levers`) is present exactly once under the marker its generator writes,
     and every README first-screen number sourced from a block equals the block.
  3. Traces — every guard / alert / make target / source phrase the docs name by
     identity exists in source, matched as an exact token (a partial rename such
     as `_canonicalize` → `_canonicalize_tables` FAILS; BACKLOG 37).
  4. BACKLOG count — CLAUDE.md's "Open BACKLOG rows: **N**" equals the un-struck
     rows in BACKLOG.md (the sentence two branches rewrite; PR #35 audit r1-D8).
Accuracy TABLE cells are guarded separately by tests/test_docs_accuracy_pins.py.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
README = ROOT / "README.md"
RESULTS = DOCS / "RESULTS.md"
SCALING = DOCS / "SCALING.md"

# [text](target) where target is a relative path (not http, not a bare #anchor).
# Link text may be empty: code spans are stripped before scanning, and a link whose
# text is `code` (most of ours) is then `[](target)`.
_LINK = re.compile(r"\[[^\]]*\]\((?!https?://)(?!#)([^)]+)\)")
# `make <target>` named as a command: inside backticks or a fenced code block
# (prose "make sure" is not a target).
_MAKE = re.compile(r"\bmake ([a-z][a-z0-9-]*)")
_FENCE = re.compile(r"```.*?```", re.S)
_TICK = re.compile(r"`([^`\n]*)`")


def _docs() -> list[Path]:
    """The living docs: README + docs/. Traced for targets, blocks and links."""
    return [README, *sorted(DOCS.glob("*.md"))]


# Link/anchor-checked only: the records name removed targets on purpose (history).
_LINK_ONLY = [ROOT / "DECISIONS.md", ROOT / "BACKLOG.md"]


def _slug(heading: str) -> str:
    """GitHub heading anchor: lowercase, drop punctuation, spaces -> hyphens."""
    text = heading.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s", "-", text)


def _anchors(md: Path) -> set[str]:
    """Heading slugs in `md`; a `#` line inside a fenced block is a comment, not
    a heading."""
    slugs: set[str] = set()
    for line in _FENCE.sub("", md.read_text()).splitlines():
        m = re.match(r"#{1,6}\s+(.*)", line)
        if m:
            slugs.add(_slug(m.group(1)))
    return slugs


# ---------------------------------------------------------------- 1. links


def _inside_root(path_part: str, target: Path) -> bool:
    """A doc link may only point inside the repo (the guard runs in CI against
    branch-controlled text; an absolute or `../../` target is never a doc link)."""
    return not Path(path_part).is_absolute() and (
        target == ROOT or ROOT in target.parents
    )


def _links(text: str) -> list[str]:
    """Relative link targets in markdown `text`; fenced / inline code is not a
    link (`bucket[8](household_id)` is code), but a link whose TEXT is code
    (`[`docs/X.md`](docs/X.md)`) is still a link."""
    return _LINK.findall(_TICK.sub("", _FENCE.sub("", text)))


def check_links(errors: list[str]) -> int:
    n = 0
    for md in [*_docs(), *_LINK_ONLY]:
        text = md.read_text()
        for raw in _links(text):
            n += 1
            path_part, _, anchor = raw.partition("#")
            target = (md.parent / path_part).resolve()
            where = md.relative_to(ROOT)
            if not _inside_root(path_part, target):
                errors.append(f"{where}: link escapes the repo: {raw}")
                continue
            if not target.exists():
                errors.append(f"{where}: broken link {raw} -> missing file {path_part}")
                continue
            if anchor and target.suffix == ".md" and anchor not in _anchors(target):
                errors.append(
                    f"{where}: broken anchor {raw} -> no heading #{anchor} "
                    f"in {path_part}"
                )
        # bare in-file anchors: [text](#slug)
        slugs = _anchors(md)
        for anchor in re.findall(r"\]\(#([^)]+)\)", text):
            n += 1
            if anchor not in slugs:
                errors.append(f"{md.relative_to(ROOT)}: broken anchor #{anchor}")
    return n


# ------------------------------------------------------- 2. generated blocks


def _const(module: str, name: str) -> str:
    """A module-level string constant, read from source (no import: the generators
    pull in clickhouse-connect / the engine)."""
    tree = ast.parse((ROOT / module).read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise KeyError(f"{module} has no constant {name}")


def _markers() -> list[tuple[str, Path, str, str]]:
    """(name, doc, begin, end) — the marker strings come from the generators
    themselves, so a marker rename in the generator fails here, not silently."""
    return [
        (
            "scale-curve",
            SCALING,
            _const("streaming/scale_probe.py", "_BEGIN"),
            _const("streaming/scale_probe.py", "_END"),
        ),
        (
            "cost-levers",
            RESULTS,
            _const("queries/measure_levers.py", "_START"),
            _const("queries/measure_levers.py", "_END"),
        ),
    ]


FIRST_SCREEN_MAX_LINES = 60


def _first_screen(readme: str) -> str:
    """README up to its first `## ` heading — the first screen, where the guarded
    block copies live (prose copies further down are not compared)."""
    cut = re.search(r"(?m)^## ", readme)
    return readme[: cut.start()] if cut else readme


def _block(doc: Path, begin: str, end: str) -> str:
    text = doc.read_text()
    return text[text.index(begin) + len(begin) : text.index(end)]


# README first-screen numbers that are COPIES of a generated block's output.
# (label, block name, regex over the block, regex over README) — group 1 of each
# must be equal. A stale block (or a hand-edited README copy) fails here.
_DERIVED: list[tuple[str, str, str, str]] = [
    (
        "bytes/exposure",
        "scale-curve",
        r"costs \*\*~(\d+) B\*\*",
        r"~(\d+) B/exposure",
    ),
    (
        "7d extrapolation",
        "scale-curve",
        r"~([\d.]+) TB at the measured",
        r"~([\d.]+) TB",
    ),
    (
        "lever 1 rows ratio",
        "cost-levers",
        r"\*\*Lever 1 [\s\S]*?\| rows read \|[^|\n]*\|[^|\n]*\| ([\d.]+)x \|",
        r"projection[^|\n]*\|[^|\n]*\|[^|\n]*?([\d.]+)× fewer rows",
    ),
    (
        "lever 2a bytes ratio",
        "cost-levers",
        r"_2a [\s\S]*?\| bytes read \|[^|\n]*\|[^|\n]*\| ([\d.]+)x \|",
        r"FINAL-avoidance[^|\n]*\|[^|\n]*\|[^|\n]*?([\d.]+)× the bytes",
    ),
    (
        "lever 3 bytes ratio",
        "cost-levers",
        r"\*\*Lever 3 [\s\S]*?\| bytes read \|[^|\n]*\|[^|\n]*\| ([\d.]+)x \|",
        r"PREWHERE[^|\n]*\|[^|\n]*\|[^|\n]*?([\d.]+)× fewer bytes",
    ),
]
# The block-side patterns anchor on the lever heading and the row LABEL, never on
# the measured cells, so a legitimate regeneration reports drift instead of
# "cannot find".


def check_generated(errors: list[str]) -> int:
    n = 0
    blocks: dict[str, str] = {}
    try:
        markers = _markers()
    except (KeyError, FileNotFoundError, SyntaxError, ValueError) as exc:
        errors.append(f"generated-block marker constant unreadable: {exc}")
        return 0
    for name, doc, begin, end in markers:
        text = doc.read_text()
        rel = doc.relative_to(ROOT)
        if text.count(begin) != 1 or text.count(end) != 1:
            errors.append(
                f"{rel}: generated block `{name}` markers not present exactly once"
            )
            continue
        if text.index(begin) > text.index(end):
            errors.append(f"{rel}: generated block `{name}` end marker precedes begin")
            continue
        body = _block(doc, begin, end)
        if not body.strip():
            errors.append(f"{rel}: generated block `{name}` is empty")
            continue
        blocks[name] = body
        n += 1
    readme = _first_screen(README.read_text())
    first_screen_lines = readme.count("\n")
    n += 1
    if first_screen_lines > FIRST_SCREEN_MAX_LINES:
        errors.append(
            f"README.md: {first_screen_lines} lines before the first `## ` "
            f"(limit {FIRST_SCREEN_MAX_LINES}, Phase-19 Done-when 1)"
        )
    for label, name, block_re, readme_re in _DERIVED:
        if name not in blocks:
            continue
        src = re.search(block_re, blocks[name])
        dst = re.search(readme_re, readme)
        if not src:
            errors.append(
                f"generated block `{name}`: cannot find {label} ({block_re!r})"
            )
        elif not dst:
            errors.append(f"README.md: no first-screen copy of {label} ({readme_re!r})")
        elif src.group(1) != dst.group(1):
            errors.append(
                f"README.md {label} = {dst.group(1)} but generated block `{name}` "
                f"says {src.group(1)} — stale copy or stale block"
            )
        n += 1
    return n


# ------------------------------------------------------------- 3. traces


def token_present(needle: str, haystack: str) -> bool:
    """Exact-token match: `needle` must not continue into an identifier on either
    side, so `_canonicalize` does NOT match `_canonicalize_tables`."""
    pattern = r"(?<![\w])" + re.escape(needle) + r"(?![\w])"
    return re.search(pattern, haystack) is not None


# (file relative to repo root, token that must be present) — the guards, alerts
# and source phrases README / RUNBOOK / RESULTS / SCALING name by identity.
TRACES: list[tuple[str, str]] = [
    ("observability/rules/alerts.yml", "ConsumerLag"),
    ("observability/rules/alerts.yml", "WatermarkStall"),
    ("observability/rules/alerts.yml", "MatchRateOutOfBand"),
    ("observability/rules/alerts.yml", "RestatementMagnitude"),
    ("queries/bench.py", "_canonicalize"),
    ("reconcile/rollup.py", "reported_at"),
    ("reconcile/rollup.py", "toDecimal64"),
    ("tests/test_rollup_decimal.py", "toDecimal64"),
    ("tests/test_money_domain.py", "CENT_DOMAIN"),
    ("producer/generate.py", "spend=round(rng.uniform"),
    ("docs/ARCHITECTURE.md", "TRUNCATES the binary value"),
    ("docs/RUNBOOK.md", "Incident 1"),
    ("docs/RUNBOOK.md", "Incident 4"),
    ("reconcile/reconcile.py", "toUnixTimestamp64Milli"),
    ("reconcile/reconcile.py", "_max_ingest"),
    ("reconcile/reconcile.py", "pick_household"),
    ("reconcile/reconcile.py", "expand_candidates"),
    ("lake/load_serving.py", "_utc"),
    ("lake/destructive.py", "confirm_or_abort"),
    ("Makefile", "unexport PROFILE SOURCE PARTITION SPEC BASE DELETED"),
    ("tests/test_clean_state_chains.py", "lake-reset"),
    ("tests/test_tz_invariance.py", "tzset"),
    ("tests/test_docs_accuracy_pins.py", "test_readme_accuracy_table_matches_pins"),
    ("tests/pins.py", "TINY_HOT"),
    ("tests/pins.py", "LONG_DELAY_POST"),
    ("streaming/scale_probe.py", "deep_sizeof"),
    ("streaming/attribute.py", "last_touch"),
    ("streaming/attribute.py", "one_row_per_conversion"),
    ("resolve/resolver.py", "resolve_one"),
    ("clickhouse/users.d/agent-ro.xml", "agent_ro"),
    ("agent/probes.py", "REGISTRY"),
    ("agent/config.py", "EVAL_REPS"),
    (
        "docs/ARCHITECTURE.md",
        "writes a NAIVE datetime as the client's LOCAL wall-clock",
    ),
    ("docs/ARCHITECTURE.md", "read_rows` counts un-merged version-parts"),
    ("docs/ARCHITECTURE.md", "renders DateTime columns in the client's local timezone"),
    ("docs/ARCHITECTURE.md", "The engine is a batch drain, not a continuous follow"),
    (".claude/commands/review-round.md", "missed in round"),
    (".claude/agents/functionality-tester.md", "Mutation"),
    ("specs/TEMPLATE.md", "Invariants"),
    ("scripts/review_gate.py", "check_evidence"),
    ("scripts/mutate.py", "swap-sort-key"),
    ("specs/TEMPLATE.md", "```mutations"),
    (".claude/commands/review-round.md", "review-round-"),
    ("scripts/round_tag.py", "is-ancestor"),
    ("tests/test_review_tools.py", "test_round_tag_requires_ancestry"),
    ("scripts/review_common.py", "exec_under_suite_env"),
]


_STRUCK = re.compile(r"~~.*?~~", re.S)
HISTORICAL = "<!-- historical -->"


def _living(text: str) -> str:
    """Drop the two sanctioned history forms from the target scan: struck-through
    text and any line tagged `<!-- historical -->` (line-granular; the whole line is
    skipped). Nothing else is exempt."""
    lines = [ln for ln in _STRUCK.sub("", text).splitlines() if HISTORICAL not in ln]
    return "\n".join(lines)


def _make_targets() -> set[str]:
    targets: set[str] = set()
    for line in (ROOT / "Makefile").read_text().splitlines():
        m = re.match(r"^([a-z][a-z0-9-]*):", line)
        if m:
            targets.add(m.group(1))
    return targets


def check_traces(errors: list[str]) -> int:
    n = 0
    for rel, needle in TRACES:
        n += 1
        f = ROOT / rel
        if not f.exists():
            errors.append(f"trace target missing: {rel}")
        elif not token_present(needle, f.read_text()):
            errors.append(f"trace lost: token '{needle}' not in {rel}")
    targets = _make_targets()
    for md in _docs():
        text = _living(md.read_text())
        commands = _FENCE.findall(text) + _TICK.findall(_FENCE.sub("", text))
        for name in sorted({m for c in commands for m in _MAKE.findall(c)}):
            n += 1
            if name not in targets:
                errors.append(
                    f"{md.relative_to(ROOT)}: names `make {name}` but the Makefile has "
                    f"no target `{name}`"
                )
    return n


# ----------------------------------------------------------- 4. backlog count


def check_backlog_count(errors: list[str]) -> int:
    """CLAUDE.md's stated open-row count == the un-struck rows in BACKLOG.md. Two
    branches that both add rows and both rewrite the sentence land a wrong count
    on whichever merges second (PR #35 audit r1-D8); here it is an edit-time
    failure."""
    backlog = (ROOT / "BACKLOG.md").read_text().splitlines()
    actual = sum(1 for ln in backlog if ln.startswith("| **"))
    m = re.search(r"Open BACKLOG rows: \*\*(\d+)\*\*", (ROOT / "CLAUDE.md").read_text())
    if not m:
        errors.append("CLAUDE.md: no 'Open BACKLOG rows: **N**' sentence")
    elif int(m.group(1)) != actual:
        errors.append(
            f"CLAUDE.md says {m.group(1)} open BACKLOG rows; BACKLOG.md has {actual}"
        )
    return 1


def main() -> int:
    errors: list[str] = []
    links = check_links(errors)
    blocks = check_generated(errors)
    traces = check_traces(errors)
    check_backlog_count(errors)
    if errors:
        print("check-docs FAILED:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print(
        f"check-docs OK: {links} links/anchors, {blocks} generated-block checks, "
        f"{traces} exact-token traces resolve, BACKLOG count matches"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
