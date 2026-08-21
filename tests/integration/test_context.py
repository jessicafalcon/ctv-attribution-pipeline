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
from lake.iceberg_catalog import configure
from reconcile import reconcile
from tests.pins import (
    SHARED_IP_HOT,
    SHARED_IP_POST,
    SHARED_IP_POST_WRONG_HOUSEHOLD,
)


@pytest.fixture(autouse=True, scope="module")
def _profile_lake():
    """This module runs the reconcile pass over the STACK's lake (the test pins
    post-reconcile numbers), so it binds the shared_ip_spike profile's lake like
    `make run PROFILE=shared_ip_spike` would — not a tmp lake, which would hold no
    candidates. It appends that profile's reconciled rows, exactly as the target's
    `make run` would have."""
    configure("shared_ip_spike")


BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
PROFILE = "shared_ip_spike"

# Pinned caused-side numbers (seed 0) come from tests/pins.py (SHARED_IP_HOT /
# SHARED_IP_POST), shared with the offline proofs in tests/test_reconcile.py and
# tests/test_post_reconcile_pins.py. Hot: 61 correct, 19 deferred (ambiguous_ip),
# 0 wrong. Post-reconcile: 69 correct, 11 wrong — the same pick the old hot reduce
# made, now made where every exposure is visible (Phase 16).
_HOT_DEFERRED = 19


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


# The stack arrives post-`make run-hot`. The hot report is captured FIRST (module
# scope), then the reconcile pass runs ONCE; every test names the fixture it needs,
# so the hot/post dependency is explicit rather than an accident of file order.
@pytest.fixture(scope="module")
def hot_report():
    return _score()


@pytest.fixture(scope="module")
def reconciled(hot_report):
    counts = reconcile.run(connect())
    return {"counts": counts, "report": _score()}


def test_hot_path_never_guesses_a_shared_ip_household(hot_report) -> None:
    # Phase 16 Done-when: after `make run-hot`, wrong-household is 0 by
    # construction; the ambiguous caused conversions sit unattributed (deferred).
    assert hot_report.caused_wrong_household == 0
    assert (
        hot_report.credited,
        hot_report.truth_links,
        hot_report.household_correct,
    ) == (SHARED_IP_HOT.credited, SHARED_IP_HOT.truth, SHARED_IP_HOT.correct)
    assert hot_report.caused_missed == _HOT_DEFERRED


def test_shared_ip_fault_is_observed_live_after_reconcile(reconciled) -> None:
    # Row 20: the reconcile pass picks a household per deferred conversion
    # (most-recent exposure across the IP's owners); recall < 1 because some of
    # those picks land on the WRONG shared-IP household — observed, not assumed.
    assert reconciled["counts"]["recovered"] >= _HOT_DEFERRED  # every deferral picked
    report = reconciled["report"]
    assert report.caused_missed == 0
    assert (report.credited, report.household_correct) == (
        SHARED_IP_POST.credited,
        SHARED_IP_POST.correct,
    )
    assert report.caused_wrong_household == SHARED_IP_POST_WRONG_HOUSEHOLD
    assert report.recall < 1.0


def test_context_is_populated_and_shows_the_shared_ip_signal(reconciled) -> None:
    # Needs the reconcile pass: report_snapshots (restatements) exist only after it,
    # and since Phase 16 an ambiguous row is attributed only on the reconciled path
    # (DECISIONS Phase 16 — ambiguous_attributed is structurally 0 hot-only).
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
