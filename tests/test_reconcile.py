"""Phase-6 reconciliation — pure matcher + version derivation, no services.

`reconcile()` reuses the hot engine's last-touch leaf at a 90d window, so a
conversion whose causing exposure is >7d back (hot-missed) but ≤90d back is
recovered; one with no in-90d exposure is left alone; recovered rows are stamped
path=reconciled with a version strictly above the hot row's; a second pass is a
no-op. The DB-side "only hot-unattributed rows are candidates" WHERE and the
rollup/restatement live behavior are proven in tests/integration/test_reconcile.py.
"""

from datetime import UTC, datetime, timedelta

from producer.models import Exposure, ResolvedConversion
from reconcile.reconcile import (
    LONG_WINDOW,
    RECONCILE_DELTA,
    reconcile,
    reconciled_at_for,
)
from streaming.attribute import HOT_WINDOW

T0 = datetime(2026, 8, 1, tzinfo=UTC)


def _exposure(eid: str, household: str, day: float) -> Exposure:
    t = T0 + timedelta(days=day)
    return Exposure(
        exposure_id=eid,
        event_time=t,
        ingest_time=t,
        campaign_id="camp-00",
        household_id=household,
        ip="10.0.0.1",
        app_id="app-01",
        program_genre="news",
        spend=0.05,
    )


def _candidate(cid: str, household: str, day: float) -> ResolvedConversion:
    t = T0 + timedelta(days=day)
    return ResolvedConversion(
        conversion_id=cid,
        event_time=t,
        ingest_time=t,
        device_id="d-1",
        ip="10.0.0.1",
        conversion_type="purchase",
        revenue=42.0,
        order_id=f"o-{cid}",
        household_id=household,
        resolution="device",
        ambiguous=False,
        candidate_count=1,
    )


def test_long_delay_miss_is_recovered_over_90d() -> None:
    # Exposure 20 days before the conversion: outside the 7d hot window (a genuine
    # hot miss), inside the 90d long window (recoverable).
    exp = _exposure("e-1", "H1", day=0)
    conv = _candidate("c-1", "H1", day=20)
    assert conv.event_time - exp.event_time > HOT_WINDOW  # was a hot miss
    at = reconciled_at_for(T0 + timedelta(days=25))

    out = reconcile([conv], {"H1": [exp]}, LONG_WINDOW, at)

    assert len(out) == 1
    row = out[0]
    assert row.conversion_id == "c-1"
    assert row.attributed is True
    assert row.exposure_id == "e-1"
    assert row.path == "reconciled"
    assert row.processed_at == at
    assert row.household_id == "H1"


def test_candidate_with_no_in_window_exposure_is_left_alone() -> None:
    # No exposure in the household at all → not recovered, and NOT emitted (it
    # stays as its hot unattributed row). This is what makes a second pass a no-op.
    conv = _candidate("c-2", "H2", day=20)
    out = reconcile([conv], {}, LONG_WINDOW, reconciled_at_for(T0))
    assert out == []


def test_exposure_older_than_90d_is_not_recovered() -> None:
    # >90d back: outside the long window too → permanently unattributed here.
    exp = _exposure("e-3", "H3", day=0)
    conv = _candidate("c-3", "H3", day=120)
    out = reconcile([conv], {"H3": [exp]}, LONG_WINDOW, reconciled_at_for(T0))
    assert out == []


def test_second_pass_is_a_no_op() -> None:
    exp = _exposure("e-1", "H1", day=0)
    conv = _candidate("c-1", "H1", day=20)
    at = reconciled_at_for(T0 + timedelta(days=25))
    first = reconcile([conv], {"H1": [exp]}, LONG_WINDOW, at)
    # A real second pass re-selects only still-unattributed rows; the recovered
    # one is no longer a candidate, so it is not passed in again → nothing new.
    second = reconcile([], {"H1": [exp]}, LONG_WINDOW, at)
    assert second == []
    # And re-running the SAME input is deterministic (idempotent rows).
    again = reconcile([conv], {"H1": [exp]}, LONG_WINDOW, at)
    assert [r.model_dump() for r in again] == [r.model_dump() for r in first]


def test_reconciled_at_is_strictly_after_the_base_by_the_delta() -> None:
    base = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
    at = reconciled_at_for(base)
    assert at == base + RECONCILE_DELTA
    assert at > base
    # A hot row's processed_at is a conversion's ingest_time, which is ≤ base
    # (base is the max ingest_time), so the reconciled version always wins RMT.
    hot_processed_at = base
    assert at > hot_processed_at
