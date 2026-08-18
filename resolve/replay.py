"""Offline replay: resolve a frozen jsonl fixture (or a seed mirror) without a
broker, writing the canonical resolved jsonl. This is the Phase 2 DONE-command
path — service-free and deterministic, so the diff against the golden expected
file has no broker ordering in it.

The live stage (resolve/stage.py) is the real pipeline component; this shares
the same pure resolver so the two cannot diverge.
"""

import argparse
from pathlib import Path

from pydantic import BaseModel

from producer.models import Conversion, Household
from producer.serialize import jsonl
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream

REPO_ROOT = Path(__file__).parent.parent
SOURCES = {"fixtures": "fixtures", "out": "data/out"}


def _read[M: BaseModel](path: Path, model: type[M]) -> list[M]:
    return [model.model_validate_json(line) for line in path.read_text().splitlines()]


def replay(profile: str, source: str) -> Path:
    src_dir = REPO_ROOT / SOURCES[source] / profile
    households = _read(src_dir / "device_graph.jsonl", Household)
    conversions = _read(src_dir / "conversions.jsonl", Conversion)
    resolved = resolve_stream(conversions, GraphIndex.from_households(households))

    out_dir = REPO_ROOT / "data" / "out" / profile
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "conversions_resolved.jsonl"
    out_path.write_text(jsonl(resolved))
    return out_path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--source", choices=sorted(SOURCES), default="fixtures")
    args = parser.parse_args(argv)
    out = replay(args.profile, args.source)
    print(f"resolved {args.profile} ({args.source}) → {out.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
