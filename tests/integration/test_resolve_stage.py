"""Live round-trip: seed → resolve stage → conversions_resolved, against the
compose stack. Skipped when no broker is reachable, so `make test` stays green
offline; runs under `make test-int` (CI from Phase 3).

Seeding is deterministic, so re-running appends byte-identical messages; the
stage drains the whole log from offset 0 (manual assignment, not group
offsets), so we compare the set of DISTINCT resolved payloads — residual or
duplicated topic rows collapse to the same bytes — against the golden expected
fixture's distinct set. We also assert the Kafka KEY of every resolved record
is its household_id (the load-bearing partition-local-join contract for
Phase 3) and that an ambiguous fan-out lands under distinct household keys.
"""

import json
import os
from pathlib import Path

import pytest
from confluent_kafka import Consumer

from producer.seed import main as seed_main
from resolve.stage import RESOLVED_TOPIC, _drain_messages, run_batch

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081")
EXPECTED = (
    Path(__file__).parent.parent.parent
    / "fixtures"
    / "tiny"
    / "expected"
    / "conversions_resolved.jsonl"
)


@pytest.fixture(scope="module", autouse=True)
def _require_broker() -> None:
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "resolve-int-probe"})
    try:
        consumer.list_topics(timeout=3)
    except Exception:
        pytest.skip("broker unreachable — run `make up` for integration tests")
    finally:
        consumer.close()


def _read_records() -> list[tuple[str, str]]:
    """(key, value) pairs from conversions_resolved, both decoded to str."""
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "resolve-int-reader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    try:
        return [
            (m.key().decode(), m.value().decode())
            for m in _drain_messages(consumer, RESOLVED_TOPIC)
        ]
    finally:
        consumer.close()


def test_stage_produces_expected_distinct_records() -> None:
    seed_main(["--profile", "tiny"])
    consumed, emitted = run_batch(BROKER, REGISTRY)
    assert consumed > 0 and emitted >= consumed

    records = _read_records()

    # (1) distinct resolved payloads match the golden fixture.
    assert {value for _, value in records} == set(EXPECTED.read_text().splitlines())

    # (2) every message key is the record's household_id — matchable events land
    # in the same partition as exposures (also keyed by household_id).
    for key, value in records:
        assert key == json.loads(value)["household_id"]

    # (3) an ambiguous conversion fans out across DISTINCT household keys, so
    # each candidate routes to its own partition.
    keys_by_conv: dict[str, set[str]] = {}
    cc_by_conv: dict[str, int] = {}
    for key, value in records:
        rec = json.loads(value)
        if rec["ambiguous"]:
            keys_by_conv.setdefault(rec["conversion_id"], set()).add(key)
            cc_by_conv[rec["conversion_id"]] = rec["candidate_count"]
    assert keys_by_conv, "expected at least one ambiguous fan-out in the fixture"
    for cid, keys in keys_by_conv.items():
        assert len(keys) == cc_by_conv[cid]
