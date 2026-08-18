"""Offline attribution replay: run the pure core over a frozen jsonl fixture
(or a seed/resolve mirror) without a broker or ClickHouse, writing the
canonical attributed jsonl. This is the Phase 3 DONE-command path — service-
free and deterministic, so the diff against the golden expected file has no
stream- or insert-ordering in it.

The live engine (streaming/dataflow.py) is the real pipeline component; it
shares the same pure leaves (streaming/attribute.py) so the two cannot diverge.
"""

import argparse
from pathlib import Path

from pydantic import BaseModel

from producer.models import Exposure, ResolvedConversion
from producer.serialize import jsonl
from streaming.attribute import attribute

REPO_ROOT = Path(__file__).parent.parent
SOURCES = {"fixtures": "fixtures", "out": "data/out"}


def _read[M: BaseModel](path: Path, model: type[M]) -> list[M]:
    return [model.model_validate_json(line) for line in path.read_text().splitlines()]


def replay(profile: str, source: str) -> Path:
    base = REPO_ROOT / SOURCES[source] / profile
    exposures = _read(base / "exposures.jsonl", Exposure)
    # fixtures keep the golden resolved output under expected/; the seed/resolve
    # mirror (out) writes it flat next to exposures.
    resolved_name = (
        "expected/conversions_resolved.jsonl"
        if source == "fixtures"
        else "conversions_resolved.jsonl"
    )
    resolved = _read(base / resolved_name, ResolvedConversion)

    attributed = attribute(exposures, resolved)

    out_dir = REPO_ROOT / "data" / "out" / profile
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "attributed.jsonl"
    out_path.write_text(jsonl(attributed))
    return out_path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--source", choices=sorted(SOURCES), default="fixtures")
    args = parser.parse_args(argv)
    out = replay(args.profile, args.source)
    print(f"attributed {args.profile} ({args.source}) → {out.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
