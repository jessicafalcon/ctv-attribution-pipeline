"""`make eval` — score the engine's attribution against ground truth at
household grain and print an accuracy table.

The ONLY module outside producer/ (which writes them) that reads the truth
side file. Truth is read ONLY here, ONLY from `data/truth/<profile>/`, and is
NEVER loaded into ClickHouse (N1, DECISIONS Phase 4): the engine's decisions
come from ClickHouse FINAL, ground truth from the side file, and the two are
joined in this harness.
"""

import argparse
import json
import sys
from pathlib import Path

from accuracy.guard import assert_profile_marker, db_profile_marker
from accuracy.score import format_report, score
from clickhouse.client import connect, read_credited, read_exposure_households

REPO_ROOT = Path(__file__).parent.parent


def load_truth(profile: str) -> dict[str, str]:
    """conversion_id → truth_exposure_id from the side file (caused conversions
    only). Never touches ClickHouse."""
    path = REPO_ROOT / "data" / "truth" / profile / "truth_links.jsonl"
    if not path.exists():
        sys.exit(f"no truth links at {path} — run `make seed PROFILE={profile}` first")
    links = {}
    for line in path.read_text().splitlines():
        record = json.loads(line)
        links[record["conversion_id"]] = record["truth_exposure_id"]
    return links


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="attribution accuracy eval")
    parser.add_argument("--profile", default="tiny")
    args = parser.parse_args(argv)

    client = connect()
    assert_profile_marker(db_profile_marker(client), args.profile)
    credited = read_credited(client)
    exposure_household = read_exposure_households(client)
    truth = load_truth(args.profile)

    report = score(credited, truth, exposure_household, profile=args.profile)
    print(format_report(report))


if __name__ == "__main__":
    main()
