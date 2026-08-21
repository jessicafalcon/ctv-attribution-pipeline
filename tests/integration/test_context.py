"""Phase-8/16 LIVE proof on a CLEAN shared_ip_spike-only stack
(`make test-int-shared-ip`: make down && up && seed shared_ip_spike && run-hot,
then the reconcile pass runs IN this test so both sides are pinned). NOT part of
the shared `make test-int` (tiny-only): profiles share conversion_id space, so a
shared stack interleaves ReplacingMergeTree rows (DECISIONS Phase 5).

Three things, all against ClickHouse FINAL:
- Phase 16 hot: `caused_wrong_household == 0` — the hot path never guesses a
  shared-IP household; the 19 caused ambiguous conversions are deferred.
- Row 20 post-reconcile (load-bearing): the shared-IP fault is OBSERVED live —
  reconciliation's most-recent-exposure pick credits the correct household at
  least as often as the old hot reduce (69/80), and the 11 wrong-household
  credits it makes are the measured fault (recall < 1).
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
from reconcile import reconcile

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
PROFILE = "shared_ip_spike"

# Pinned caused-side numbers (seed 0), like the long_delay/medium live pins:
# changing producer/profiles/shared_ip_spike.json means updating these in the SAME
# change. Hot: 61 correct, 19 deferred (ambiguous_ip), 0 wrong. Post-reconcile:
# 69 correct, 11 wrong — the same pick the old hot reduce made, now made where
# every exposure is visible (Phase 16). Matches the offline proof in
# tests/test_reconcile.py.
_TRUTH_LINKS = 80
_HOT_CORRECT, _HOT_DEFERRED = 61, 19
_POST_CORRECT, _POST_WRONG_HOUSEHOLD = 69, 11


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


def _score():
    client = connect()
    return score(
        read_credited(client),
        load_truth(PROFILE),
        read_exposure_households(client),
        PROFILE,
    )


def test_hot_path_never_guesses_a_shared_ip_household() -> None:
    # Phase 16 Done-when: after `make run-hot`, wrong-household is 0 by
    # construction; the ambiguous caused conversions sit unattributed (deferred).
    report = _score()
    assert report.truth_links == _TRUTH_LINKS
    assert report.caused_wrong_household == 0
    assert (report.household_correct, report.caused_missed) == (
        _HOT_CORRECT,
        _HOT_DEFERRED,
    )


def test_shared_ip_fault_is_observed_live_after_reconcile() -> None:
    # Row 20: the reconcile pass picks a household per deferred conversion
    # (most-recent exposure across the IP's owners); recall < 1 because some of
    # those picks land on the WRONG shared-IP household — observed, not assumed.
    counts = reconcile.run(connect())
    assert counts["recovered"] >= _HOT_DEFERRED  # every deferral got a pick
    report = _score()
    assert report.caused_missed == 0
    assert report.household_correct == _POST_CORRECT  # ≥ the old hot reduce
    assert report.caused_wrong_household == _POST_WRONG_HOUSEHOLD
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
