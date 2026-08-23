"""The eval profile/DB-mismatch guard (BACKLOG 43), as a pure function so it is
unit-testable without a database.

`make eval` scores `data/truth/<profile>/` against whatever the ClickHouse
serving tables hold. If those tables were populated from a DIFFERENT profile, the
score is meaningless (e.g. tiny truth vs a long_delay DB → ~0.17). The populate
path stamps `eval_meta` with its profile (`clickhouse/write_marker.py`); this
asserts the stamp matches the profile being scored, and fails loud otherwise —
never a silent bad number.

A conversion_id-subset check is deliberately NOT used: ids are numbered
`c-NNNNNN` from 0, so a smaller profile's set is a subset of a larger one's
(tiny ⊆ long_delay), and a "truth ⊆ DB" check would false-pass the exact original
bug (tiny truth scored against a long_delay DB) — a guard that greenlights its
own failure.
"""


def db_profile_marker(client) -> str | None:
    """The profile string in `eval_meta` (None if the table is empty OR not yet
    created — a bare `make up` before any populate target applies the DDL, so the
    guard gives its friendly "no marker" message instead of a raw UNKNOWN_TABLE).
    The populate path stamps it so the DB self-describes which profile populated the
    serving tables (BACKLOG 43). Lives here, beside the assert, so every reader of
    the marker (`make eval`, `make rollup-bench`) reads it the same way."""
    if int(client.command("exists table eval_meta")) == 0:
        return None
    rows = client.query("select profile from eval_meta final").result_rows
    return rows[0][0] if rows else None


class ProfileMismatchError(SystemExit):
    """Loud exit (a SystemExit subclass) when the DB's marker profile does not
    match the profile being scored, or no marker is present. SystemExit so
    `make eval` stops with a message instead of printing a meaningless table."""


def assert_profile_marker(marker: str | None, profile: str) -> None:
    """`marker`: the profile string in `eval_meta` (None if the table is empty —
    no populate run yet). `profile`: the `--profile` being scored. Raises loudly
    on missing or mismatch; returns None on match."""
    if marker is None:
        raise ProfileMismatchError(
            f"eval: no eval_meta profile marker in ClickHouse — run "
            f"`make run PROFILE={profile}` (or run-hot / replay-serving) first so the "
            f"DB holds a profile to score against."
        )
    if marker != profile:
        raise ProfileMismatchError(
            f"eval: profile/DB mismatch — ClickHouse was populated from "
            f"'{marker}' but you asked to score '{profile}'. Reseed and re-run "
            f"'{profile}', or pass PROFILE={marker} to score what the DB holds."
        )
