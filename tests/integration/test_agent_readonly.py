"""Phase-9 LIVE read-only proof on a CLEAN shared_ip_spike-only stack
(`make test-int-agent`: make down && up && seed shared_ip_spike && run). NOT part of
the shared `make test-int` (tiny-only): profiles share conversion_id space, so a
shared stack interleaves ReplacingMergeTree rows (DECISIONS Phase 5).

Two things, both against the live database:
- Done-when 2 (the DB-enforced guarantee): the agent's `agent_ro` user CANNOT write —
  INSERT / ALTER / DROP / CREATE all raise ACCESS_DENIED. The read-only posture is
  enforced at ClickHouse, not by convention.
- SN2 / Ruling E: the WHOLE collector read path runs under `agent_ro` — `collect()`
  through `connect_agent()` builds a populated context — so the guarantee covers the
  collectors, not only the probes.

No LLM call, so no API tokens: the loop control flow is unit-tested with a mocked
client (tests/test_loop.py); this proves the database boundary is real."""

import os

import pytest
from confluent_kafka import Consumer

from agent import probes
from agent.run_context import collect
from clickhouse.client import connect_agent

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
PROFILE = "shared_ip_spike"

_WRITE_STATEMENTS = [
    ("INSERT", "insert into exposures_landed (exposure_id) values ('agent-probe')"),
    ("ALTER", "alter table exposures_landed add column _agent_probe Int8"),
    ("DROP", "drop table exposures_landed"),
    ("CREATE", "create table _agent_probe (x Int8) engine = Memory"),
]


@pytest.fixture(scope="module", autouse=True)
def _require_services() -> None:
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "agent-ro-probe"})
    try:
        consumer.list_topics(timeout=3)
    except Exception:
        pytest.skip("broker unreachable — run `make test-int-agent`")
    finally:
        consumer.close()


@pytest.mark.parametrize(
    "kind,sql", _WRITE_STATEMENTS, ids=[s[0] for s in _WRITE_STATEMENTS]
)
def test_agent_user_cannot_write(kind: str, sql: str) -> None:
    client = connect_agent()
    with pytest.raises(Exception) as exc:
        client.command(sql)
    message = str(exc.value).upper()
    # DB-enforced denial, not a parse/column error — the grant check fires first.
    assert "ACCESS_DENIED" in message or "NOT ENOUGH PRIVILEGES" in message, (
        f"{kind} was not access-denied: {exc.value}"
    )


def test_agent_user_can_read_the_whole_context_path() -> None:
    # SN2: collectors read via connect_agent(); a populated context proves SELECT works
    # under the SELECT-only user (report.sql / restatement.sql / readers all covered).
    ctx = collect(connect_agent(), PROFILE)
    assert ctx.processed > 0
    assert ctx.attributed > 0
    assert ctx.campaigns  # per-campaign metrics read through agent_ro
    # The shared-IP discriminator is present (this is the load-bearing fault profile).
    assert ctx.ip_clusters.ip_resolved_attributed > 0


def test_every_probe_executes_and_returns_typed_rows() -> None:
    # The live Test-step path: each probe runs as agent_ro (SELECT-only) and its rows
    # validate into the declared result model. Uses handles discovered from the DB —
    # the same handles the context surfaces to the agent. Would have caught the
    # toDate→str mapping bug before the live agent-run spent a token.
    client = connect_agent()
    cid = client.query(
        "select any(campaign_id) from exposures_landed final"
    ).result_rows[0][0]
    genre = client.query(
        "select any(program_genre) from exposures_landed final"
    ).result_rows[0][0]
    ip = client.query(
        "select ip from attributed_conversions final "
        "where attributed = 1 and resolution = 'ip' "
        "group by ip order by count() desc, ip limit 1"
    ).result_rows[0][0]
    cases = [
        ("ip_cluster_detail", {"ip": ip}),
        ("campaign_attributed_by_day", {"campaign_id": cid}),
        ("campaign_restatement", {"campaign_id": cid}),
        ("window_edge_distribution", {}),
        ("genre_reach_detail", {"genre": genre}),
    ]
    for name, params in cases:
        rows = probes.run(client, name, params)  # raises if a row fails validation
        assert isinstance(rows, list)
    day_rows = probes.run(client, "campaign_attributed_by_day", {"campaign_id": cid})
    assert all(isinstance(r.day, str) for r in day_rows)  # regression: date→str
