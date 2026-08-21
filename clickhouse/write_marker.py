"""Write the eval-guard profile marker (BACKLOG 43).

`make run`/`run-hot`/`lake-land` call this after the engine populates the serving
tables, recording which profile the DB now holds in the single-row `eval_meta`
table. `make eval` reads it back and refuses to score a mismatched profile
against these rows (see `accuracy/guard.py`). A separate process step, never the
engine sink — the engine path stays byte-identical.
"""

import argparse

from clickhouse.client import connect


def write_marker(profile: str, client=None) -> None:
    """Replace the single `eval_meta` row (k=0) with `profile`. Idempotent: a
    re-run inserts the same row, which ReplacingMergeTree collapses on read, so
    a replay from offset 0 converges to the same marker."""
    client = client or connect()
    client.insert("eval_meta", [[0, profile]], column_names=["k", "profile"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="write the eval-guard profile marker")
    parser.add_argument("--profile", required=True)
    args = parser.parse_args(argv)
    write_marker(args.profile)
    print(f"eval_meta: marker set to profile={args.profile}")


if __name__ == "__main__":
    main()
