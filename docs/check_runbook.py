#!/usr/bin/env python3
"""Trace/link check for docs/RUNBOOK.md (Phase 15).

Standalone, no pytest, no services — mirrors the Phase-11 README link discipline
so the DONE gate can assert every runbook cross-reference resolves without adding
a test file that would re-trigger the full suite. Run via `make check-runbook`.

Two checks:
  1. Every relative markdown link in RUNBOOK.md points at a real file, and any
     `#anchor` resolves to a heading in that file (GitHub-style slug).
  2. Every guard/alert the runbook names by identity actually exists in source —
     the "elevate, invent nothing" discipline made executable.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent
ROOT = DOCS.parent
RUNBOOK = DOCS / "RUNBOOK.md"

# [text](target) where target is a relative path (not http, not a bare #anchor).
_LINK = re.compile(r"\[[^\]]+\]\((?!https?://)(?!#)([^)]+)\)")


def _slug(heading: str) -> str:
    """GitHub heading anchor: lowercase, drop punctuation, spaces -> hyphens."""
    text = heading.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s", "-", text)


def _anchors(md: Path) -> set[str]:
    slugs: set[str] = set()
    for line in md.read_text().splitlines():
        m = re.match(r"#{1,6}\s+(.*)", line)
        if m:
            slugs.add(_slug(m.group(1)))
    return slugs


def _check_links(errors: list[str]) -> None:
    for raw in _LINK.findall(RUNBOOK.read_text()):
        path_part, _, anchor = raw.partition("#")
        target = (DOCS / path_part).resolve()
        if not target.exists():
            errors.append(f"broken link: {raw} -> missing file {path_part}")
            continue
        if anchor and target.suffix == ".md":
            if anchor not in _anchors(target):
                errors.append(
                    f"broken anchor: {raw} -> no heading #{anchor} in {path_part}"
                )


def _check_traces(errors: list[str]) -> None:
    """Each named guard/alert must exist where the runbook says it does."""
    traces = [
        # (file relative to repo root, substring that must be present)
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
        ("reconcile/reconcile.py", "toUnixTimestamp64Milli"),
        ("reconcile/reconcile.py", "_max_ingest"),
        ("lake/load_serving.py", "_utc"),
        ("tests/test_tz_invariance.py", "tzset"),
        (
            "docs/ARCHITECTURE.md",
            "writes a NAIVE datetime as the client's LOCAL wall-clock",
        ),
        ("docs/ARCHITECTURE.md", "read_rows` counts un-merged version-parts"),
        (
            "docs/ARCHITECTURE.md",
            "renders DateTime columns in the client's local timezone",
        ),
        (
            "docs/ARCHITECTURE.md",
            "The engine is a batch drain, not a continuous follow",
        ),
    ]
    for rel, needle in traces:
        f = ROOT / rel
        if not f.exists():
            errors.append(f"trace target missing: {rel}")
        elif needle not in f.read_text():
            errors.append(f"trace lost: '{needle}' no longer in {rel}")


def main() -> int:
    if not RUNBOOK.exists():
        print("FAIL: docs/RUNBOOK.md does not exist")
        return 1
    errors: list[str] = []
    _check_links(errors)
    _check_traces(errors)
    if errors:
        print("RUNBOOK trace check FAILED:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("RUNBOOK trace check OK: all links and traces resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
