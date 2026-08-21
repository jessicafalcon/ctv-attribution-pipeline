"""Phase-6 LIVE reconciliation proof on a CLEAN long_delay-only stack
(`make test-int-long-delay`: make down && up && seed long_delay && run
long_delay — where `run` is resolve → engine → reconcile). NOT part of the shared
`make test-int` (tiny-only): tiny/medium/long_delay share conversion_id space, so
a shared stack interleaves ReplacingMergeTree rows (DECISIONS Phase 5).

Proves the Done-when against ClickHouse FINAL after `make run`:
- Gate 1 (recovery): hot-path misses are recovered — path='reconciled' rows
  exist, and household-grain recall is higher than the hot-only recall.
- Gate 2 (restatement): report_snapshots holds a pre (hot) and post (reconciled)
  snapshot per campaign, and the restatement query shows credited conversions /
  ROAS rising between them (recovery can only raise them).
- Idempotence: a second reconciliation pass recovers nothing new and leaves the
  reconciled row count and the restatement unchanged.
"""

import os

import pytest
from confluent_kafka import Consumer

from accuracy.run import load_truth
from accuracy.score import score
from clickhouse.client import connect, read_exposure_households
from queries import restatement
from reconcile import reconcile
from tests.pins import LONG_DELAY_HOT, LONG_DELAY_POST

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")


@pytest.fixture(scope="module", autouse=True)
def _require_services() -> None:
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "recon-int-probe"})
    try:
        consumer.list_topics(timeout=3)
    except Exception:
        pytest.skip("broker unreachable — run `make test-int-long-delay`")
    finally:
        consumer.close()
    try:
        connect().command("select 1")
    except Exception:
        pytest.skip("clickhouse unreachable — run `make test-int-long-delay`")


def _credited(path: str | None) -> dict[str, tuple[str, str]]:
    """conversion_id → (household_id, exposure_id) for attributed rows, optionally
    filtered to a single path ('hot' for the pre-reconciliation credited set)."""
    where = "attributed = 1" + (f" and path = '{path}'" if path else "")
    rows = (
        connect()
        .query(
            f"select conversion_id, household_id, exposure_id "
            f"from attributed_conversions final where {where} order by conversion_id"
        )
        .result_rows
    )
    return {r[0]: (r[1], r[2]) for r in rows}


def _reconciled_count() -> int:
    return (
        connect()
        .query(
            "select count() from attributed_conversions final where path = 'reconciled'"
        )
        .result_rows[0][0]
    )


def _score(credited: dict[str, tuple[str, str]]):
    client = connect()
    return score(
        credited,
        load_truth("long_delay"),
        read_exposure_households(client),
        "long_delay",
    )


# Pinned live numbers for long_delay (seed 6), like medium's in test_engine_
# hardening.py: changing producer/profiles/long_delay.json means updating these in
# the SAME change, never silently. Hot pass leaves 29 caused misses + 3 organic
# misses; reconciliation recovers the 29 caused (all to the correct household) and
# leaves the 3 organics (no in-90d exposure) unmatched.
_RECONCILED = 29
_HOT_CREDITED, _HOT_CORRECT = LONG_DELAY_HOT.credited, LONG_DELAY_HOT.correct  # 44/75
_POST_CREDITED, _POST_CORRECT = (
    LONG_DELAY_POST.credited,
    LONG_DELAY_POST.correct,
)  # 73/75
_TRUTH_LINKS = LONG_DELAY_HOT.truth


def test_recovery_lifts_recall_and_writes_reconciled_rows() -> None:
    # Gate 1. `make run` already reconciled once; the recovered misses are now
    # path='reconciled' in FINAL, while the hot-attributed set (path='hot') is the
    # pre-reconciliation credited set (invariant under reconciliation).
    assert _reconciled_count() == _RECONCILED

    hot = _score(_credited("hot"))
    post = _score(_credited(None))
    # Hot pass: genuine misses (recall < 1). Reconciliation recovers the caused
    # ones to their correct household → recall rises; the only remaining gap is
    # the 2 pre-existing shared-IP wrong-household hot attributions (not misses).
    assert (hot.credited, hot.household_correct, hot.truth_links) == (
        _HOT_CREDITED,
        _HOT_CORRECT,
        _TRUTH_LINKS,
    )
    assert (post.credited, post.household_correct, post.truth_links) == (
        _POST_CREDITED,
        _POST_CORRECT,
        _TRUTH_LINKS,
    )
    assert hot.recall == LONG_DELAY_HOT.recall  # 0.587 (44/75) — the docs pin
    assert post.recall == LONG_DELAY_POST.recall  # 0.973 (73/75) — the docs pin
    assert post.recall > hot.recall
    assert post.caused_missed == 0  # every recoverable caused miss was recovered


def test_restatement_shows_the_metric_rising_between_snapshots() -> None:
    # Gate 2. Two snapshots per campaign (pre hot base, post reconciled base+Δ);
    # the restatement query collapses them to a per-campaign before/after diff.
    rows = restatement.run()
    assert rows  # a row per campaign
    # columns: campaign, roas_as_reported, roas_now, roas_delta,
    #          conversions_as_reported, conversions_now, revenue_delta
    assert sum(r[4] for r in rows) == _HOT_CREDITED  # pre = hot credited
    assert sum(r[5] for r in rows) == _POST_CREDITED  # post = hot + recovered
    for r in rows:
        assert r[5] > r[4]  # every campaign recovered some conversions
        assert r[3] > 0  # every campaign's ROAS restated strictly up
        assert r[6] > 0  # positive revenue delta


def test_second_pass_is_idempotent() -> None:
    # A fresh reconciliation pass recovers nothing new (the 29 recoverable caused
    # misses were recovered by `make run`; only the 3 unmatched organics remain
    # candidates), and leaves the reconciled count and the restatement unchanged.
    before_restate = restatement.run()

    counts = reconcile.run(connect())
    assert counts["recovered"] == 0
    assert counts["candidates"] == 3  # the 3 organics with no in-90d exposure

    assert _reconciled_count() == _RECONCILED
    assert restatement.run() == before_restate


def _hot_attributed_rows() -> dict[str, tuple]:
    """The full identity of every hot-ATTRIBUTED row (attributed=1, path='hot')."""
    rows = (
        connect()
        .query(
            "select conversion_id, exposure_id, path, toString(processed_at), assists "
            "from attributed_conversions final where attributed = 1 and path = 'hot' "
            "order by conversion_id"
        )
        .result_rows
    )
    return {r[0]: (r[1], r[2], r[3], tuple(r[4])) for r in rows}


def test_hot_attributed_rows_are_untouched_by_a_pass() -> None:
    # The candidate WHERE (`attributed = 0 and path = 'hot'`) must never re-open a
    # hot-ATTRIBUTED row — re-attributing it over 90d picks the same last-touch but
    # would flip its path hot→reconciled for no change. Assert the hot-attributed
    # set is byte-identical across a pass (the direct guard the spec asked for; the
    # idempotence test covers it only indirectly via candidates==3).
    before = _hot_attributed_rows()
    assert len(before) == _HOT_CREDITED  # 83 hot-attributed rows
    reconcile.run(connect())
    assert _hot_attributed_rows() == before


def test_campaign_hourly_is_versioned_replace_not_summed() -> None:
    # The rollup is a versioned-replace RMT: each refresh rewrites ALL keys with a
    # higher reported_at, so FINAL holds exactly one row per (campaign_id, hour) —
    # never a sum of successive refreshes (which an insert-triggered summing MV
    # would produce and a correction would double-count).
    client = connect()
    dupes = client.query(
        "select campaign_id, hour, count() as n from campaign_hourly final "
        "group by campaign_id, hour having n > 1"
    ).result_rows
    assert dupes == []

    # And the recompute is correct: campaign_hourly summed over hours equals the
    # current (post-reconciliation) credited conversions computed directly from
    # FINAL — so the refresh regrouped without dropping or double-counting.
    rollup = client.query(
        "select campaign_id, sum(attributed_conversions) "
        "from campaign_hourly final group by campaign_id order by campaign_id"
    ).result_rows
    direct = client.query(
        "select e.campaign_id, count() from attributed_conversions a final "
        "inner join (select exposure_id, campaign_id from exposures_landed final) e "
        "on a.exposure_id = e.exposure_id "
        "where a.attributed = 1 group by e.campaign_id order by e.campaign_id"
    ).result_rows
    assert {r[0]: r[1] for r in rollup} == {r[0]: r[1] for r in direct}


def test_snapshot_period_is_the_fixed_sentinel() -> None:
    # `period` is a fixed sentinel this phase (campaign-total grain); day-grain
    # slots in later without a schema change (BACKLOG / agent phase).
    from reconcile.rollup import PERIOD

    periods = {
        r[0]
        for r in connect()
        .query("select distinct period from report_snapshots final")
        .result_rows
    }
    assert periods == {PERIOD}
