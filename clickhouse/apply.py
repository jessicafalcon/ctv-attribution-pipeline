"""Apply the Phase 3 DDL against the running ClickHouse. Idempotent
(`create ... if not exists`); called by `make run` and the integration setup.
No migration framework yet — one flat DDL file, executed statement by
statement (DECISIONS: simplest standard solution now)."""

from pathlib import Path

from clickhouse_connect.driver.client import Client

from clickhouse.client import connect

DDL = Path(__file__).parent / "ddl.sql"


def _statements(ddl: str) -> list[str]:
    """Split into executable statements. Strip `--` line comments first so a
    semicolon inside a comment cannot split a statement (or leave a comment-only
    chunk that ClickHouse rejects as an empty query)."""
    uncommented = "\n".join(line.split("--", 1)[0] for line in ddl.splitlines())
    return [s.strip() for s in uncommented.split(";") if s.strip()]


def apply(client: Client | None = None) -> None:
    client = client or connect()
    for statement in _statements(DDL.read_text()):
        client.command(statement)


def main() -> None:
    apply()
    print("clickhouse: DDL applied")


if __name__ == "__main__":
    main()
