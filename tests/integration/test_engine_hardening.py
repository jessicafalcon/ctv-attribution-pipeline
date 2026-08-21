"""Feature 3 — LIVE medium hardening proof against a CLEAN medium-only stack
(`make test-int-medium`: make down && up && seed medium && run medium). This is
NOT part of the shared `make test-int`, which stays tiny-only: tiny and medium
share conversion_id space (both start at c-000000), so a shared stack would
interleave their ReplacingMergeTree rows and pollute FINAL (DECISIONS Phase 5).
Isolation comes from the sanctioned `make down`, not a per-test TRUNCATE.

Asserts the three Done-when clauses live, against the pinned oracle baseline
that tests/test_medium_parity.py fixes offline:
- clause 1: FINAL P/R == oracle (precision 92/130, recall 1.0, wrong_hh 0);
- clause 2: eviction ran (join-state gauge > 0 and evictions > 0);
- clause 3: dedup fired (suppressed 63) AND FINAL row count is unchanged by a
  dedup-off run (transparency).
"""

import os

import pytest
from confluent_kafka import Consumer

from accuracy.run import load_truth
from accuracy.score import score
from clickhouse.client import (
    connect,
    read_attributed_decisions,
    read_credited,
    read_exposure_households,
)
from streaming import metrics
from streaming.dataflow import run_engine
from tests.pins import MEDIUM_HOT

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")


@pytest.fixture(scope="module", autouse=True)
def _require_services() -> None:
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "medium-int-probe"})
    try:
        consumer.list_topics(timeout=3)
    except Exception:
        pytest.skip("broker unreachable — run `make test-int-medium`")
    finally:
        consumer.close()
    try:
        connect().command("select 1")
    except Exception:
        pytest.skip("clickhouse unreachable — run `make test-int-medium`")


def _reset_engine_metrics() -> None:
    metrics.DEDUP_SUPPRESSED._value.set(0)
    metrics.EXPOSURES_EVICTED._value.set(0)
    metrics.reset_join_state_peak()


def _final_row_count() -> int:
    return len(read_attributed_decisions(connect()))


def test_medium_live_matches_oracle_evicts_and_dedups() -> None:
    _reset_engine_metrics()
    run_engine(BROKER)  # dedup on; re-drains the medium topics (idempotent)

    # Clause 1: live FINAL P/R == the pinned oracle baseline.
    client = connect()
    rep = score(
        read_credited(client),
        load_truth("medium"),
        read_exposure_households(client),
        "medium",
    )
    assert rep.recall == MEDIUM_HOT.recall and rep.caused_wrong_household == 0
    assert rep.precision == MEDIUM_HOT.precision
    assert (rep.credited, rep.truth_links, rep.household_correct) == (
        MEDIUM_HOT.credited,
        MEDIUM_HOT.truth,
        MEDIUM_HOT.correct,
    )

    # Clause 2: eviction ran — state built up and came back down.
    assert metrics.EXPOSURES_EVICTED._value.get() > 0
    assert metrics.JOIN_STATE_SIZE._value.get() > 0

    # Clause 3a: dedup fired.
    assert metrics.DEDUP_SUPPRESSED._value.get() == 70
    assert _final_row_count() == 132


def test_dedup_is_transparent_final_count_unchanged(monkeypatch) -> None:
    # Clause 3b: a dedup-OFF run yields the same FINAL row count — RMT collapses
    # the re-sends on read regardless, so dedup changes nothing but volume.
    before = _final_row_count()
    monkeypatch.setenv("ENGINE_DEDUP", "off")
    _reset_engine_metrics()
    run_engine(BROKER)
    assert metrics.DEDUP_SUPPRESSED._value.get() == 0  # dedup was off
    assert _final_row_count() == before == 132
