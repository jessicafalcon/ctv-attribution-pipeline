"""Spec D6: the load is driven by the days a landing TOUCHED, never the wall clock.

`land()` / `land_attributed()` return the set of event_time days they wrote — a
late row (ingested weeks later) lands in its OLD event day and that day is what
gets reloaded — and both drivers (`streaming.dataflow.main`, `reconcile.run`) hand
exactly that set to `orchestration.load.materialize_load`. Offline: tmp lake; the
broker, ClickHouse and Dagster are monkeypatched at their seams.
"""

from datetime import UTC, datetime, timedelta

import pytest

import orchestration.load
import reconcile.reconcile as rc
import streaming.dataflow as df
from lake.land_attributed import land_attributed
from lake.land_exposures import land
from producer.models import AttributedConversion, Exposure

T = datetime(2026, 8, 10, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))


def _exp(eid: str, event: datetime, ingest: datetime | None = None) -> Exposure:
    return Exposure(
        exposure_id=eid,
        event_time=event,
        ingest_time=ingest or event + timedelta(minutes=1),
        campaign_id="c",
        household_id="h-1",
        ip="1",
        app_id="a",
        program_genre="g",
        spend=1.0,
    )


def _row(cid: str, event: datetime, hh: str = "h-1", **over) -> AttributedConversion:
    base = dict(
        conversion_id=cid,
        event_time=event,
        ingest_time=event + timedelta(minutes=1),
        device_id="d",
        ip="1",
        conversion_type="purchase",
        revenue=1.0,
        order_id=None,
        household_id=hh,
        resolution="device",
        ambiguous=False,
        candidate_count=1,
        exposure_id=None,
        assists=[],
        attributed=False,
        path="hot",
        processed_at=event + timedelta(minutes=1),
        reason="state_miss",
        candidate_households=[],
    )
    return AttributedConversion(**{**base, **over})


def test_land_returns_the_event_time_days_it_wrote() -> None:
    assert land([_exp("e-1", T), _exp("e-2", T + timedelta(days=2))]) == {
        "2026-08-10",
        "2026-08-12",
    }
    assert land([]) == set()
    assert land_attributed([_row("c-1", T + timedelta(days=5))]) == {"2026-08-15"}


def test_a_late_row_touches_its_old_event_day_not_its_ingest_day() -> None:
    late = _exp(
        "e-late",
        event=datetime(2026, 8, 5, 3, tzinfo=UTC),
        ingest=T + timedelta(days=10),
    )
    assert land([late]) == {"2026-08-05"}  # the day to RELOAD, not 2026-08-20


def test_engine_driver_loads_exactly_the_touched_days(monkeypatch, capsys) -> None:
    run = df.EngineRun(
        exposures=[_exp("e-1", T), _exp("e-2", T + timedelta(days=1))],
        rows=[_row("c-1", T + timedelta(days=3))],
        resolved=1,
        suppressed=0,
    )
    seen: list[set[str]] = []
    monkeypatch.setattr(df, "run_engine", lambda broker: run)
    monkeypatch.setattr(
        orchestration.load,
        "materialize_load",
        lambda days: seen.append(set(days)) or {"exposures": 0, "attributed": 0},
    )
    df.main(["--profile", "tiny"])
    assert seen == [{"2026-08-10", "2026-08-11", "2026-08-13"}]
    assert "3 day(s)" in capsys.readouterr().out


def test_reconcile_driver_loads_exactly_the_days_its_corrections_touched(
    monkeypatch,
) -> None:
    # Lake: one hot-unattributed candidate (day 08-10) whose exposure (08-01) is
    # outside the 7d hot window but inside 90d → recovered; its event day is what
    # gets reloaded. No ClickHouse: the version base and finalize are stubbed.
    land([_exp("e-1", datetime(2026, 8, 1, tzinfo=UTC))])
    land_attributed([_row("c-1", T)])
    seen: list[set[str]] = []
    monkeypatch.setattr(rc, "apply_ddl", lambda: None)
    monkeypatch.setattr(rc, "connect", lambda: object())
    monkeypatch.setattr(rc, "_max_ingest", lambda client: T + timedelta(days=1))
    monkeypatch.setattr(rc, "finalize", lambda client: None)
    monkeypatch.setattr(rc.metrics, "observe_restatement", lambda v: None)
    monkeypatch.setattr(rc, "_restatement_abs_delta", lambda client: 0.0)
    monkeypatch.setattr(
        orchestration.load,
        "materialize_load",
        lambda days: seen.append(set(days)) or {"exposures": 0, "attributed": 0},
    )
    counts = rc.run()
    assert counts == {"candidates": 1, "recovered": 1, "still_missing": 0}
    assert seen == [{"2026-08-10"}]
