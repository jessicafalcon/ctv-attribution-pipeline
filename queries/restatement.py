"""`make restate` — the restatement table: each campaign's ROAS/conversions as
reported before reconciliation vs now, and the change reconciliation caused.
Executes queries/restatement.sql against report_snapshots FINAL; the diff math
lives in the SQL. Serving-layer only, never the accuracy side file."""

from pathlib import Path

from clickhouse_connect.driver.client import Client

from clickhouse.client import connect

SQL = Path(__file__).parent / "restatement.sql"

_HEADERS = [
    "campaign",
    "roas_as_reported",
    "roas_now",
    "roas_delta",
    "conversions_as_reported",
    "conversions_now",
    "revenue_delta",
]
_FLOAT_COLS = {"roas_as_reported", "roas_now", "roas_delta", "revenue_delta"}


def run(client: Client | None = None) -> list[tuple]:
    client = client or connect()
    return client.query(SQL.read_text()).result_rows


def _cell(header: str, value: object) -> str:
    if value is None:
        return "NULL"
    if header in _FLOAT_COLS:
        return f"{float(value):.4f}"
    return str(value)


def format_table(rows: list[tuple]) -> str:
    table = [_HEADERS] + [
        [_cell(h, v) for h, v in zip(_HEADERS, row, strict=True)] for row in rows
    ]
    widths = [max(len(r[i]) for r in table) for i in range(len(_HEADERS))]
    lines = [
        "  ".join(cell.rjust(widths[i]) for i, cell in enumerate(r)) for r in table
    ]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def main() -> None:
    print("restatement — metric as reported (pre-reconciliation) vs now")
    print(format_table(run()))


if __name__ == "__main__":
    main()
