"""Live round-trip: seed → resolve stage → conversions_resolved, against the
compose stack. Skipped when no broker is reachable, so `make test` stays green
offline; runs under `make test-int` (CI from Phase 3).

Seeding is deterministic, so re-running appends byte-identical messages; the
stage drains the whole log from offset 0 (manual assignment, not group
offsets), so we compare the set of DISTINCT resolved payloads — residual or
duplicated topic rows collapse to the same bytes — against the golden expected
fixture's distinct set.
"""

import os
from pathlib import Path

import pytest
from confluent_kafka import Consumer

from producer.seed import main as seed_main
from resolve.stage import RESOLVED_TOPIC, _drain, run_batch

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


def _distinct_resolved_payloads() -> set[str]:
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "resolve-int-reader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        return {v.decode() for v in _drain(consumer, RESOLVED_TOPIC)}
    finally:
        consumer.close()


def test_stage_produces_expected_distinct_records() -> None:
    seed_main(["--profile", "tiny"])
    consumed, emitted = run_batch(BROKER, REGISTRY)
    assert consumed > 0 and emitted >= consumed

    expected = {line for line in EXPECTED.read_text().splitlines()}
    assert _distinct_resolved_payloads() == expected
