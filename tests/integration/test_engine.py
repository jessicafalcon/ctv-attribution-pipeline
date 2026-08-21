"""Live round-trip: seed → engine (resolve in-process) → ClickHouse, against the compose
stack. Skipped when the broker or ClickHouse is unreachable, so `make test`
stays green offline; runs under `make test-int` (CI from Phase 3).

Two assertions:
- attributed_conversions FINAL == the golden expected fixture, compared on the
  decision columns keyed by conversion_id (robust to timestamp round-trip;
  FINAL collapses ReplacingMergeTree rows so replays/duplicates read as one).
- exposures_landed FINAL count == distinct exposure count after TWO engine runs
  — the RMT-not-MergeTree idempotency guard (DECISIONS Phase 3).
"""

import json
import os
from collections import Counter
from pathlib import Path

import pytest
from confluent_kafka import Consumer

from clickhouse.client import connect, count_exposures_final, read_attributed_decisions
from producer.seed import main as seed_main
from streaming.dataflow import run_engine

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "tiny"
EXPECTED = FIXTURES / "expected" / "attributed.jsonl"


@pytest.fixture(scope="module", autouse=True)
def _require_services() -> None:
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "engine-int-probe"})
    try:
        consumer.list_topics(timeout=3)
    except Exception:
        pytest.skip("broker unreachable — run `make up` for integration tests")
    finally:
        consumer.close()
    try:
        connect().command("select 1")
    except Exception:
        pytest.skip("clickhouse unreachable — run `make up` for integration tests")


def _expected_decisions() -> dict[str, tuple]:
    out: dict[str, tuple] = {}
    for line in EXPECTED.read_text().splitlines():
        r = json.loads(line)
        out[r["conversion_id"]] = (
            r["household_id"],
            r["exposure_id"],
            bool(r["attributed"]),
            tuple(r["assists"]),
            r["path"],
            r["reason"],
        )
    return out


def test_engine_final_matches_expected_fixture() -> None:
    seed_main(["--profile", "tiny"])
    run_engine(BROKER)

    got = read_attributed_decisions(connect())
    assert got == _expected_decisions()
    # The Phase-16 reason column round-trips: 47 credited (null), 3 state-misses,
    # 5 shared-IP deferrals — read back live, not only written.
    assert Counter(d[5] for d in got.values()) == {
        None: 47,
        "state_miss": 3,
        "ambiguous_ip": 5,
    }


def test_exposures_landed_idempotent_over_two_runs() -> None:
    # Two full engine runs must not inflate exposures_landed: RMT collapses the
    # re-landings on (campaign_id, event_time, exposure_id).
    run_engine(BROKER)
    run_engine(BROKER)

    distinct = len(
        {
            json.loads(line)["exposure_id"]
            for line in (FIXTURES / "exposures.jsonl").read_text().splitlines()
        }
    )
    assert count_exposures_final(connect()) == distinct
