"""Phase-6 prerequisite guard — the `long_delay` profile must actually produce
reconciliation candidates, or Phase 6's Done-when cannot fire.

The engine's only genuine hot-path miss is a caused conversion with **no
same-household exposure in the 7d window before it** — which happens when its
causal exposure is >7d earlier in event-time (and no nearer one exists). `medium`
and `tiny` keep every caused conversion inside 7d, so they make ZERO candidates;
`long_delay` straddles the 7d boundary so both paths (hot-attributed AND
reconciled) are non-trivially populated, while every miss stays recoverable
inside the 90d long window and arrival lateness stays within tolerance (so the
ONLY miss cause is the >7d state-miss, not a late-arrival drop).

Computed OFFLINE (generate → resolve → dedup → attribute at 7d), no services.
The counts are pinned like `medium`'s / `tiny`'s: changing
producer/profiles/long_delay.json means updating these assertions in the SAME
change, never silently.
"""

from datetime import timedelta

import pytest

from producer.config import load_profile
from producer.generate import generate
from resolve.index import GraphIndex
from resolve.resolver import resolve_stream
from streaming.attribute import ALLOWED_LATENESS, HOT_WINDOW, attribute, dedup_streams

LONG_WINDOW = timedelta(days=90)


@pytest.fixture(scope="module")
def hot():
    p = load_profile("long_delay")
    s = generate(p, p.seed)
    idx = GraphIndex.from_households(s.graph.households)
    resolved = resolve_stream(s.conversions, idx)
    exps, res, _ = dedup_streams(s.exposures, resolved)
    rows = {r.conversion_id: r for r in attribute(exps, res, HOT_WINDOW)}
    truth = {t.conversion_id: t.truth_exposure_id for t in s.truth_links}
    return {
        "profile": p,
        "stream": s,
        "exps": exps,
        "rows": rows,
        "truth": truth,
        "index": idx,
    }


def test_arrival_lateness_within_allowed_lateness(hot) -> None:
    # The ONLY reason a caused conversion is unattributed here is the >7d
    # state-miss — never an in-tolerance late arrival dropped by the engine
    # (there is no conversion-side lateness gate). Keep late.max within grace.
    late_max = timedelta(minutes=hot["profile"].events.late.max_minutes)
    assert late_max <= ALLOWED_LATENESS


def test_delay_range_straddles_hot_window_and_stays_recoverable(hot) -> None:
    lo, hi = hot["profile"].events.conversion_delay_minutes
    # min inside the hot window (a hot baseline exists), max past it (candidates
    # exist) but within the 90d long window (every long-delay miss is recoverable
    # — a >90d delay is permanently lost, a different test, out of scope here).
    assert timedelta(minutes=lo) <= HOT_WINDOW < timedelta(minutes=hi) <= LONG_WINDOW


def test_both_paths_populated_with_pinned_split(hot) -> None:
    rows, truth = hot["rows"], hot["truth"]
    caused = list(truth)
    hot_attr = [cid for cid in caused if rows[cid].attributed]
    hot_miss = [cid for cid in caused if not rows[cid].attributed]
    # Healthy split: a strong hot baseline the restatement reads against, and a
    # non-trivial candidate set to recover. (Note 29 << the 56 caused conversions
    # whose causal delay exceeds 7d — the rest are hot-attributed to a *different*
    # same-household exposure, so they never miss. That gap is why "recall rises
    # by exactly the recovered count" is scoped to caused, correctly-resolved.)
    # Phase 16: 2 caused shared-IP conversions are deferred (ambiguous_ip) instead
    # of guessed hot, so the hot baseline is 44 and the candidate set 31 (29
    # state-misses + 2 ambiguous).
    assert len(caused) == 75
    assert len(hot_attr) == 44
    assert len(hot_miss) == 31
    assert sum(rows[c].candidate_count > 1 for c in hot_miss) == 2


def test_every_caused_miss_is_recoverable_to_its_truth_household(hot) -> None:
    rows, truth = hot["rows"], hot["truth"]
    exp_time = {e.exposure_id: e.event_time for e in hot["exps"]}
    exp_hh = {e.exposure_id: e.household_id for e in hot["exps"]}
    recoverable = 0
    for cid, causal_exp in truth.items():
        row = rows[cid]
        if row.attributed:
            continue
        # The attributed row carries the conversion's event_time.
        within_long = row.event_time - exp_time[causal_exp] <= LONG_WINDOW
        # An ambiguous_ip placeholder's household is NOT the pick — reconciliation
        # re-enumerates the IP's owners, so the truth household must be among them.
        households = (
            hot["index"].owners_of[row.ip]
            if row.candidate_count > 1
            else [row.household_id]
        )
        recoverable += within_long and exp_hh[causal_exp] in households
    # All 31 caused misses (29 state-misses + 2 ambiguous) have their truth
    # household reachable within 90d — none is permanently lost.
    assert recoverable == 31


def test_deterministic(hot) -> None:
    p = hot["profile"]
    s2 = generate(p, p.seed)
    assert [e.exposure_id for e in s2.exposures] == [
        e.exposure_id for e in hot["stream"].exposures
    ]
    assert [c.conversion_id for c in s2.conversions] == [
        c.conversion_id for c in hot["stream"].conversions
    ]
    assert {t.conversion_id for t in s2.truth_links} == set(hot["truth"])
