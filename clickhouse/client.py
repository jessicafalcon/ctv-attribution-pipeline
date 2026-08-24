"""ClickHouse connection factory and read helpers, shared by apply.py and the
engine sink. Connection params come from the environment (never hardcoded),
mirroring KAFKA_BROKER / SCHEMA_REGISTRY_URL elsewhere. clickhouse-connect
speaks the HTTP interface (port 8123), the only ClickHouse port compose
publishes to the host."""

import os

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def connect() -> Client:
    return get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "default"),
    )


def connect_agent() -> Client:
    """The SELECT-only handle for the WHOLE agent read path — both the Phase-8
    collectors (`agent/run_context.py`) and the Phase-9 probe dispatcher (Ruling E /
    SN2). `agent_ro` is a `users.d`-declared user granted only `SELECT ON default.*`
    (clickhouse/users.d/agent-ro.xml), so a write is refused at the database, not by
    convention. Passwordless, mirroring the stack's local-dev posture; overridable via
    CLICKHOUSE_AGENT_USER for a hardened deploy."""
    return get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_AGENT_USER", "agent_ro"),
        password=os.environ.get("CLICKHOUSE_AGENT_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "default"),
    )


def connect_cost() -> Client:
    """The query-cost writer's handle (`cost_rw`, Phase 18b). Granted exactly SELECT
    ON system.query_log and INSERT ON default.query_cost_daily
    (clickhouse/users.d/cost-rw.xml) — it reads the cost of the tagged report/restate/
    bench queries and writes query_cost_daily, and cannot read a pipeline row. A
    writer gets its own principal, not a wider metrics_ro. Passwordless local-dev
    posture; overridable via CLICKHOUSE_COST_USER for a hardened deploy."""
    return get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_COST_USER", "cost_rw"),
        password=os.environ.get("CLICKHOUSE_COST_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "default"),
    )


def read_attributed_decisions(client: Client) -> dict[str, tuple]:
    """The attribution DECISION per conversion_id from FINAL state: the columns
    the engine computes, robust to timestamp round-trip formatting. FINAL
    collapses ReplacingMergeTree rows so replays/duplicates read as one."""
    rows = client.query(
        """
        select conversion_id, household_id, exposure_id, attributed, assists, path,
            reason
        from attributed_conversions final
        order by conversion_id
        """
    ).result_rows
    return {r[0]: (r[1], r[2], bool(r[3]), tuple(r[4]), r[5], r[6]) for r in rows}


def count_exposures_final(client: Client) -> int:
    return client.query("select count() from exposures_landed final").result_rows[0][0]


def read_credited(client: Client) -> dict[str, tuple[str, str]]:
    """The engine's attributed decisions from FINAL state: conversion_id →
    (household_id, exposure_id), for attributed=true rows only. Feeds the
    accuracy eval (household-grain scoring)."""
    rows = client.query(
        """
        select conversion_id, household_id, exposure_id
        from attributed_conversions final
        where attributed = 1
        order by conversion_id
        """
    ).result_rows
    return {r[0]: (r[1], r[2]) for r in rows}


def read_exposure_households(client: Client) -> dict[str, str]:
    """exposure_id → household_id from exposures_landed FINAL, so the eval can
    map any exposure to its household."""
    rows = client.query(
        "select exposure_id, household_id from exposures_landed final "
        "order by exposure_id"
    ).result_rows
    return {r[0]: r[1] for r in rows}
