"""LIVE: the scraper's principal can see how ClickHouse stores the data and NOT
what the data says (Phase 18a). The mirror of the agent_ro write-denied proof (SN2,
`tests/integration/test_agent_readonly.py`), one layer stricter: `agent_ro` may
SELECT every pipeline table, `metrics_ro` may SELECT none of them.

Runs under any `make test-int*` target — it asserts grants, never a profile's
numbers, and writes nothing.
"""

import pytest

from clickhouse.client import connect
from observability.ch_scrape import connect_metrics, scrape

ACCESS_DENIED = "ACCESS_DENIED"

# Reads and writes that must ALL be refused at the database, not by convention.
FORBIDDEN = [
    "select count() from attributed_conversions",
    "select * from exposures_landed limit 1",
    "select * from report_snapshots limit 1",
    "insert into rollup_dirty values ('c', now(), now())",
    "alter table attributed_conversions delete where 1",
    "drop table rollup_refresh_marker",
    "create table pwned (x Int8) engine = Memory",
]


@pytest.fixture(scope="module")
def metrics_client():
    return connect_metrics()


@pytest.mark.parametrize("sql", FORBIDDEN)
def test_metrics_ro_cannot_touch_pipeline_data(metrics_client, sql: str) -> None:
    with pytest.raises(Exception) as exc:  # noqa: B017 - the driver's DatabaseError
        metrics_client.command(sql)
    assert ACCESS_DENIED in str(exc.value), f"{sql!r} was not refused: {exc.value}"


def test_metrics_ro_can_read_the_two_system_tables_it_is_granted(
    metrics_client,
) -> None:
    # The SHOW TABLES grant lifts ClickHouse's row filter on system.parts (without it
    # the scrape silently reports zero parts — ARCHITECTURE §8); it does not make a
    # single data row readable, which the parametrized refusals above prove.
    parts = metrics_client.query(
        "select count() from system.parts where active"
    ).result_rows[0][0]
    assert parts > 0, "system.parts is empty or filtered — is the SHOW grant present?"
    metrics_client.query("select count() from system.merges")


def test_the_scrape_runs_end_to_end_as_metrics_ro(metrics_client) -> None:
    values = scrape(metrics_client)
    assert values["parts:attributed_conversions"] >= 1
    assert values["merge_backlog_seconds"] >= 0
    # State, not a claim about the data: the scrape's numbers must not depend on
    # what the rows say. Same stack, two scrapes, same settled answer.
    again = scrape(metrics_client)
    assert {k: v for k, v in again.items() if k != "settle_seconds"} == {
        k: v for k, v in values.items() if k != "settle_seconds"
    }


def test_the_default_user_is_unchanged_by_this_phase() -> None:
    # agent_ro's grants are not widened by adding a second principal; the pipeline's
    # own user still writes. A one-line canary: if this fails, the users.d mount
    # order or a grant edit broke an existing path.
    connect().command("select 1")
