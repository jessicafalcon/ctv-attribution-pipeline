"""Phase-8 LIVE proof on a CLEAN shared_ip_spike-only stack
(`make test-int-shared-ip`: make down && up && seed shared_ip_spike && run).
NOT part of the shared `make test-int` (tiny-only): profiles share conversion_id
space, so a shared stack interleaves ReplacingMergeTree rows (DECISIONS Phase 5).

Two things, both against ClickHouse FINAL after `make run`:
- Row 20 (load-bearing): the shared-IP fault is OBSERVED live — the eval shows
  caused wrong-household misattributions (recall < 1), with the caused-side counts
  reconciliation-invariant (caused rows are all attributed, so the 90d pass never
  touches them; only organics could be recovered).
- Done-when 2: the collector builds a POPULATED, pydantic-valid AttributionContext
  from ClickHouse with zero LLM calls, and the shared-IP discriminator is present.
"""

import os

import pytest
from confluent_kafka import Consumer

from accuracy.run import load_truth
from accuracy.score import score
from agent.run_context import collect
from clickhouse.client import connect, read_credited, read_exposure_households

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
PROFILE = "shared_ip_spike"

# Pinned caused-side numbers (seed 0), like the long_delay/medium live pins:
# changing producer/profiles/shared_ip_spike.json means updating these in the SAME
# change. Reconciliation-invariant — the 80 caused conversions are all attributed
# on the hot path, so the 90d pass never re-opens them (caused_missed == 0).
_TRUTH_LINKS = 80
_HOUSEHOLD_CORRECT = 69
_CAUSED_WRONG_HOUSEHOLD = 11


@pytest.fixture(scope="module", autouse=True)
def _require_services() -> None:
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "ctx-int-probe"})
    try:
        consumer.list_topics(timeout=3)
    except Exception:
        pytest.skip("broker unreachable — run `make test-int-shared-ip`")
    finally:
        consumer.close()
    try:
        connect().command("select 1")
    except Exception:
        pytest.skip("clickhouse unreachable — run `make test-int-shared-ip`")


def test_shared_ip_fault_is_observed_live() -> None:
    # Row 20: recall < 1 because caused conversions are credited to the WRONG
    # (shared-IP) household — observed, not assumed.
    client = connect()
    report = score(
        read_credited(client),
        load_truth(PROFILE),
        read_exposure_households(client),
        PROFILE,
    )
    assert report.truth_links == _TRUTH_LINKS
    assert report.household_correct == _HOUSEHOLD_CORRECT
    assert report.caused_wrong_household == _CAUSED_WRONG_HOUSEHOLD
    assert report.recall < 1.0


def test_context_is_populated_and_shows_the_shared_ip_signal() -> None:
    ctx = collect(connect(), PROFILE)
    # Populated headline.
    assert ctx.processed > 0 and ctx.attributed > 0
    assert 0.0 < ctx.match_rate <= 1.0
    assert ctx.match_rate_by_day  # a per-day series exists
    # Every §4.2 section is filled from ClickHouse.
    assert len(ctx.campaigns) == 3  # n_campaigns
    assert len(ctx.genre_reach) == 4  # four genres, all with exposures
    assert all(g.exposures > 0 for g in ctx.genre_reach)
    assert ctx.window_edge.attributed_hot > 0
    assert len(ctx.window_edge.buckets) == 7
    assert len(ctx.restatements) == 3  # pre/post snapshots per campaign exist
    # The discriminator: this profile resolves many conversions via shared IPs.
    assert ctx.ip_clusters.ip_resolved_attributed > 0
    assert ctx.ip_clusters.ip_resolved_fraction > 0.1
    assert ctx.ip_clusters.max_candidate_count >= 2  # fan-out happened
    assert ctx.ip_clusters.top_clusters  # named shared IPs
