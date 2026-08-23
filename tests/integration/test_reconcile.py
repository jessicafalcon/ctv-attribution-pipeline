"""Phase-6 LIVE reconciliation proof on a CLEAN long_delay-only stack
(`make test-int-long-delay`: make down && lake-reset && up && seed long_delay && run
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

import json
import os
from pathlib import Path

import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer

from accuracy.run import load_truth
from accuracy.score import score
from clickhouse.client import connect, read_exposure_households
from lake.iceberg_catalog import configure
from queries import restatement
from reconcile import reconcile
from tests.pins import (
    LONG_DELAY_HOT,
    LONG_DELAY_POST,
    TODECIMAL_TRUNCATED_CENT_VALUES,
)


@pytest.fixture(autouse=True, scope="module")
def _profile_lake():
    """This module runs the reconcile pass IN-PROCESS over the STACK's lake (the
    clean long_delay stack `make test-int-long-delay` built), so it binds that
    profile's lake like `make run PROFILE=long_delay` does — there is no default
    root (review gate: an unset default produced a profile-mixed lake)."""
    configure("long_delay")


BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "tiny"


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
# the SAME change, never silently. Hot pass leaves 29 caused state-misses + 3
# ambiguous_ip deferrals (2 caused, 1 organic — Phase 16) + 3 organic misses;
# reconciliation recovers the 29 caused (all to the correct household) and picks
# a household for the 3 ambiguous (the 2 caused land on the wrong shared-IP
# household, exactly as the old hot reduce did), leaving the 3 organics (no
# in-90d exposure) unmatched. POST numbers are unchanged from before Phase 16.
_RECONCILED = 32
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
    # state-misses to their correct household → recall rises; the only remaining
    # gap is the 2 shared-IP conversions whose reconcile pick lands on the wrong
    # household (was a hot guess before Phase 16; same outcome, later).
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


def _versioned_money(client: Client) -> dict[str, tuple]:
    """Per-key money (and money-derived ratios) in both versioned tables, read at
    full precision — the merge-immune identity used by the two-pass pin (a
    ReplacingMergeTree merge may collapse twins at any moment; the money of the
    surviving row is what must not move)."""
    out: dict[str, tuple] = {}
    for r in client.query(
        "select campaign_id, hour, argMax(spend, reported_at), "
        "argMax(revenue, reported_at) from campaign_hourly final "
        "group by campaign_id, hour order by campaign_id, hour"
    ).result_rows:
        out[f"hourly:{r[0]}:{r[1]}"] = (r[2], r[3])
    for r in client.query(
        "select campaign_id, period, argMax(spend, reported_at), "
        "argMax(revenue, reported_at), argMax(roas, reported_at), "
        "argMax(cpa, reported_at) "
        "from report_snapshots final group by campaign_id, period order by campaign_id"
    ).result_rows:
        out[f"snap:{r[0]}:{r[1]}"] = (r[2], r[3], r[4], r[5])
    return out


def test_second_pass_is_idempotent() -> None:
    # A fresh reconciliation pass recovers nothing new (the 29 caused misses + 3
    # ambiguous were recovered by `make run`; only the 3 unmatched organics remain
    # candidates), and leaves the reconciled count and the restatement unchanged.
    client = connect()
    before_restate = restatement.run()
    before_money = _versioned_money(client)

    counts = reconcile.run(connect())
    assert counts["recovered"] == 0
    assert counts["candidates"] == 3  # the 3 organics with no in-90d exposure

    assert _reconciled_count() == _RECONCILED
    # Merge-immune two-pass pin FIRST (RUNBOOK incident 3): the money every key
    # holds after the second pass equals, EXACTLY, what it held before — whichever
    # twin the ReplacingMergeTree keeps. Decimal-summed money makes the twins
    # identical, so the survivor cannot differ. Ahead of the restatement check so
    # a regression is reported by the pin that names it.
    assert _versioned_money(client) == before_money
    assert restatement.run() == before_restate  # exact, not 6 dp


def test_second_pass_twins_are_byte_identical() -> None:
    # Direct twin identity for `report_snapshots`: the raw (un-FINAL) rows of the two
    # passes must be byte-identical per key. If a merge happened to collapse them
    # before the read there is nothing to compare and the test skips — the money pin
    # above is the always-on guard.
    #
    # `campaign_hourly` was RETIRED from this proof at the Phase-18a review gate. It
    # can no longer produce a twin: the loader refreshes only the keys a load touched,
    # so a second reconcile pass with nothing to recover writes ZERO rollup rows, and
    # the assert was passing vacuously (rows == keys by construction). Its content is
    # pinned properly now — `tests/integration/test_rollup_dirty.py` compares the
    # served rollup to a full rebuild, and `make rollup-bench` runs both into scratch
    # tables.
    client = connect()
    reconcile.run(connect())  # a further pass → one more twin per key
    table, key = "report_snapshots", "reported_at, campaign_id, period"
    cols = ", ".join(
        r[0]
        for r in client.query(
            "select name from system.columns where database = currentDatabase() "
            f"and table = '{table}' order by position"
        ).result_rows
    )
    # toString(tuple(...)): NULL-safe (a NULL cpa would make cityHash64 of
    # bare columns NULL and uniqExact skip the row)
    rows, distinct_keys, distinct_rows = client.query(
        f"select count(), uniqExact({key}), "
        f"uniqExact(cityHash64(toString(tuple({cols})))) from {table}"
    ).result_rows[0]
    assert distinct_rows == distinct_keys, f"{table}: a re-written row differs"
    if rows == distinct_keys:
        pytest.skip(
            "report_snapshots twins merged before the read; the money pin holds"
        )


def test_versioned_money_equals_the_source_sums_exactly() -> None:
    # Value-level pin (the twin pin alone let a truncating conversion through —
    # `make bench` caught it in CI): the stored money in both versioned tables
    # equals the source rows' money to the cent, per campaign, EXACTLY. The
    # source side deliberately MIRRORS the rollup's credit join and its Decimal
    # expression: this pins the numeric path (no loss between source and
    # versioned row), not the credit definition — that is `make bench`'s job.
    client = connect()
    # spend from exposures alone (a join to conversions would multiply an
    # exposure's spend by the conversions credited to it); revenue via the credit
    spend = dict(
        client.query(
            "select campaign_id, toFloat64(sum(toDecimal64(toString(spend), 4))) "
            "from exposures_landed final group by campaign_id"
        ).result_rows
    )
    revenue = dict(
        client.query(
            "select e.campaign_id, "
            "toFloat64(sum(toDecimal64(toString(a.revenue), 4))) "
            "from attributed_conversions as a final "
            "inner join exposures_landed as e final on a.exposure_id = e.exposure_id "
            "where a.attributed = 1 group by e.campaign_id"
        ).result_rows
    )
    src = {c: (spend[c], revenue.get(c, 0.0)) for c in spend}
    hourly = {
        r[0]: (r[1], r[2])
        for r in client.query(
            "select campaign_id, toFloat64(sum(toDecimal64(toString(spend), 4))), "
            "toFloat64(sum(toDecimal64(toString(revenue), 4))) "
            "from campaign_hourly final group by campaign_id"
        ).result_rows
    }
    snap = {
        r[0]: (r[1], r[2])
        for r in client.query(
            "select campaign_id, argMax(spend, reported_at), "
            "argMax(revenue, reported_at) "
            "from report_snapshots final group by campaign_id"
        ).result_rows
    }
    assert hourly == src, f"campaign_hourly money != source: {hourly} vs {src}"
    assert snap == src, f"report_snapshots money != source: {snap} vs {src}"


def test_string_decimal_path_is_exact_over_the_whole_cent_domain() -> None:
    # ClickHouse truncates toDecimal64(<Float64>) at the binary value (5.2 % of
    # 2-dp values lose a ten-thousandth); the toString path is exact for every
    # cent value 0.00 … 999.99 — the producer's domain (tests/test_money_domain.py).
    bad_direct, bad_string, n = (
        connect()
        .query(
            "select "
            "countIf(toDecimal64(x, 4) != toDecimal64(toString(x), 4)), "
            "countIf(toFloat64(toDecimal64(toString(x), 4)) != x), "
            "count() "
            "from (select round(number / 100, 2) as x from numbers(100000))"
        )
        .result_rows[0]
    )
    assert n == 100000
    assert bad_string == 0
    # Pinned exactly (tests/pins.py, cited in ARCHITECTURE §8): the image is
    # digest-pinned, so this is a property of THAT ClickHouse; an image bump is
    # allowed to move it — update the pin and §8 together. IEEE truncation is
    # arithmetic, not platform-specific, but the number is measured on arm64 and
    # asserted on amd64 in CI — if they ever disagree, CI says so.
    assert bad_direct == TODECIMAL_TRUNCATED_CENT_VALUES
    # … and the doubles the PRODUCER actually emits (Python round(x, 2) → JSON)
    # round-trip too, not only ClickHouse's own construction of the domain.
    fixture_values = sorted(
        {
            json.loads(ln)["spend"]
            for ln in (FIXTURES / "exposures.jsonl").read_text().splitlines()
        }
        | {
            json.loads(ln)["revenue"]
            for ln in (FIXTURES / "conversions.jsonl").read_text().splitlines()
        }
    )
    bad = (
        connect()
        .query(
            "select countIf(toFloat64(toDecimal64(toString(x), 4)) != x) "
            "from (select arrayJoin({vals:Array(Float64)}) as x)",
            parameters={"vals": fixture_values},
        )
        .result_rows[0][0]
    )
    assert bad == 0 and len(fixture_values) > 10


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
    assert len(before) == _HOT_CREDITED  # 80 hot-attributed rows
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
