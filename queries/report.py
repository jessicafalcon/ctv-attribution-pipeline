"""`make report` — the four advertiser metrics (ROAS, CPA, CVR, site-visit
rate) per campaign, printed as a table. Executes queries/report.sql against
ClickHouse; the metric math lives in the SQL (one source, no Python metric core
to drift from it). Reads FINAL on both tables (DECISIONS Phase 4)."""

from pathlib import Path

from clickhouse_connect.driver.client import Client

from clickhouse.client import connect

SQL = Path(__file__).parent / "report.sql"

_HEADERS = [
    "campaign",
    "spend",
    "exposures",
    "conversions",
    "purchases",
    "revenue",
    "roas",
    "cpa",
    "cvr",
    "site_visit_rate",
]
# columns rendered as fixed-precision floats (NULL where the SQL returned None)
_FLOAT_COLS = {"spend", "revenue", "roas", "cpa", "cvr", "site_visit_rate"}


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
    print("reporting v1 — four metrics per campaign (raw serving tables)")
    print(format_table(run()))


if __name__ == "__main__":
    main()
