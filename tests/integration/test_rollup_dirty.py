"""LIVE: the dirty set is the loader↔rollup contract (Phase 18a Done-when 2).

Runs under `make test-int-long-delay` — the reconcile-bearing stack. The gate moved
here from `make rollup-bench` at the review gate: a contract proven only by a `make`
target that neither CI nor `make test` runs is proven nowhere it matters.

The rule is `changed ⊆ dirty`: every key whose `campaign_hourly` row differs between a
pre- and a post-reconciliation FULL rebuild must be one the refresh recomputed. A
missed key serves a stale rollup while the full-refresh oracle still passes — the one
failure mode nothing else here can see. Equality is evidence, not the rule (a reload
re-records a day's exposure hours whether or not their aggregate moved, so the dirty
set is a lawful superset).
"""

import pytest

from clickhouse.client import connect
from queries.bench_common import round_row
from reconcile import rollup
from reconcile.reconcile import RECONCILE_DELTA_MS


@pytest.fixture(scope="module")
def client():
    return connect()


def _keyed(rows: list[tuple]) -> dict[tuple, tuple]:
    return {(r[0], r[1]): round_row(r[2:]) for r in rows}


def _changed_keys(client) -> set[tuple]:
    """Keys whose aggregate differs between the hot-only and both-paths rebuilds —
    i.e. the keys reconciliation moved. The hot-attributed set is invariant under
    reconciliation, which is what makes the hot-only rebuild a faithful 'before'."""
    before = _keyed(rollup.campaign_hourly_rows(client, hot_only=True))
    after = _keyed(rollup.campaign_hourly_rows(client))
    return {k for k in before.keys() | after.keys() if before.get(k) != after.get(k)}


def test_the_reconcile_pass_changed_something(client) -> None:
    # Guard against a vacuous gate: on a profile whose reconciliation restates nothing,
    # every assertion below would pass on empty sets.
    assert _changed_keys(client), (
        "no rollup key changed — run this on the long_delay stack (`make "
        "test-int-long-delay`), where reconciliation restates every campaign"
    )


def test_every_changed_key_was_refreshed(client) -> None:
    changed = _changed_keys(client)
    reconciled_stamp = client.query(
        "select min(processed_at) from attributed_conversions final "
        "where path = 'reconciled'"
    ).result_rows[0][0]
    refreshed = {
        (r[0], r[1])
        for r in client.query(
            "select campaign_id, hour from rollup_refreshed final "
            "where version >= {stamp:DateTime64(3)}",
            parameters={"stamp": reconciled_stamp},
        ).result_rows
    }
    missed = changed - refreshed
    assert not missed, (
        f"{len(missed)} changed key(s) never refreshed — the incremental rollup "
        f"serves stale rows for {sorted(missed)[:5]}"
    )


def test_the_served_rollup_equals_a_full_rebuild(client) -> None:
    # The end-to-end claim: after `make run`, what campaign_hourly SERVES is what a
    # single full rebuild would have produced. This is what the incremental path must
    # never break, and it is the pin behind the loader-side refresh (Phase 18a).
    served = _keyed(
        client.query(
            "select campaign_id, hour, spend, exposures, attributed_conversions, "
            "purchases, site_visits, revenue from campaign_hourly final"
        ).result_rows
    )
    oracle = _keyed(rollup.campaign_hourly_rows(client))
    differing = [k for k in oracle if served.get(k) != oracle.get(k)]
    assert not differing, f"served rollup differs from the oracle on {differing[:5]}"
    assert set(served) == set(oracle)


def test_the_pipeline_converges_to_nothing_dirty(client) -> None:
    # Every load refreshes the keys it touched, so a completed `make run` leaves no
    # key whose recorded version differs from the version the rollup was computed
    # against. A non-empty set here means a load skipped its refresh.
    assert rollup.dirty_keys(client) == []


def test_a_refresh_with_nothing_dirty_writes_nothing(client) -> None:
    before = client.query("select count() from campaign_hourly").result_rows[0][0]
    assert rollup.refresh_campaign_hourly(client, offset_ms=0) == 0
    after = client.query("select count() from campaign_hourly").result_rows[0][0]
    assert after == before


def test_every_served_row_carries_a_pass_stamp(client) -> None:
    """The hot load refreshes at offset 0 and the reconcile reload at
    RECONCILE_DELTA_MS, so every rollup row's `reported_at` is one of those two
    data-derived stamps — never a wall clock, and never a third value from some
    unaccounted refresh.

    NOT asserted here: that both stamps are simultaneously present. Superseded
    ReplacingMergeTree rows are collapsed by a background merge (and `make
    rollup-bench` legitimately re-refreshes every key at the post stamp), so "two
    distinct versions exist right now" is a transient physical fact, not a contract.
    The contract — which offset each load stamps — is pinned offline in
    `tests/test_touched_days.py`.
    """
    base = client.query(
        "select toUnixTimestamp64Milli(max(t)) from ("
        "select max(ingest_time) as t from exposures_landed final "
        "union all "
        "select max(ingest_time) as t from attributed_conversions final)"
    ).result_rows[0][0]
    stamps = {
        int(r[0])
        for r in client.query(
            "select distinct toUnixTimestamp64Milli(reported_at) from campaign_hourly"
        ).result_rows
    }
    assert stamps <= {base, base + RECONCILE_DELTA_MS}, (
        f"rollup rows carry a stamp that is neither pass: {sorted(stamps)} "
        f"(hot={base}, post={base + RECONCILE_DELTA_MS})"
    )
