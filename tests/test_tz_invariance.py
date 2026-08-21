"""RUNBOOK incident 3 guard: the reconcile write path gives the same bytes on a
machine in any timezone.

For ten phases the reconciled rows' event_time/ingest_time were shifted by the
machine's UTC offset (clickhouse-connect interprets a NAIVE datetime on insert as
local time, while it reads a DateTime64(3,'UTC') column back naive-UTC) and every
offset-invariant guard passed. This test asks the determinism policy's question
— "could this step give a different answer on a re-run?" — for "on a different
machine": the same candidates, shaped exactly as the naive-UTC read-back, run
through reconcile → lake → loader values under TZ=UTC and under a non-UTC zone,
must serialize byte-identically, and every datetime handed to the ClickHouse
client must be tz-aware UTC. Offline: tmp lake, no services.
"""

import time
from datetime import UTC, datetime, timedelta

import pytest

from lake.land_attributed import land_attributed
from lake.load_serving import _attributed_values, _exposure_values
from lake.read_attributed import read_current
from producer.models import Exposure, ResolvedConversion
from reconcile.reconcile import LONG_WINDOW, reconcile, reconciled_at_for

T0 = datetime(2026, 8, 1, 12, 2, 51, 841000)  # NAIVE UTC — the read-back shape


def _candidate(cid: str, hh: str, days: float) -> ResolvedConversion:
    t = T0 + timedelta(days=days)
    return ResolvedConversion(
        conversion_id=cid,
        event_time=t,
        ingest_time=t + timedelta(minutes=1),
        device_id="d-1",
        ip="10.0.0.1",
        conversion_type="purchase",
        revenue=9.5,
        order_id=None,
        household_id=hh,
        resolution="device",
        ambiguous=False,
        candidate_count=1,
    )


def _exposure(eid: str, hh: str, days: float) -> Exposure:
    t = T0 + timedelta(days=days)
    return Exposure(
        exposure_id=eid,
        event_time=t,
        ingest_time=t + timedelta(minutes=1),
        campaign_id="c-1",
        household_id=hh,
        ip="10.0.0.1",
        app_id="a",
        program_genre="g",
        spend=1.0,
    )


def _write_path(monkeypatch, tmp_root: str) -> tuple[list[str], list[object]]:
    """reconcile (naive-UTC inputs) → land → lake read → loader values, serialized."""
    monkeypatch.setenv("LAKE_ROOT", tmp_root)
    exps = {"H1": [_exposure("e-1", "H1", 0.0)]}
    recovered = reconcile(
        [_candidate("c-1", "H1", 20.0)], exps, LONG_WINDOW, reconciled_at_for(T0)
    )
    assert recovered and recovered[0].path == "reconciled"
    land_attributed(recovered)
    rows = read_current()
    values = [_attributed_values(r) for r in rows] + [
        _exposure_values(e) for e in exps["H1"]
    ]
    flat = [v for row in values for v in row]
    serialized = [v.isoformat() if isinstance(v, datetime) else repr(v) for v in flat]
    return serialized, [v for v in flat if isinstance(v, datetime)]


@pytest.fixture
def _tz():
    """A MonkeyPatch whose undo runs BEFORE tzset: the pytest `monkeypatch`
    fixture finalizes after its dependents, so a `tzset()` in a dependent
    fixture would run while TZ is still the last zone and leak it into the rest
    of the session (review gate). Own the order here."""
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()
    time.tzset()


def test_reconcile_write_path_is_byte_identical_across_machine_timezones(
    tmp_path, _tz
) -> None:
    outputs = {}
    for zone in ("UTC", "America/Denver", "Asia/Kolkata"):
        _tz.setenv("TZ", zone)
        time.tzset()
        serialized, datetimes = _write_path(_tz, str(tmp_path / zone))
        assert datetimes, "the path must hand datetimes to the client"
        assert all(
            d.tzinfo is not None and d.utcoffset() == timedelta(0) for d in datetimes
        ), f"{zone}: a naive or non-UTC datetime reached the client boundary"
        outputs[zone] = serialized
    assert outputs["UTC"] == outputs["America/Denver"] == outputs["Asia/Kolkata"]
    # and the content is the instant the candidate carried, not a shifted one
    assert (T0 + timedelta(days=20)).replace(tzinfo=UTC).isoformat() in outputs["UTC"]


def test_a_naive_datetime_is_taken_as_utc_by_the_loader_whatever_tz() -> None:
    # The property the rows above rely on, stated directly.
    from lake.load_serving import _utc

    assert _utc(T0) == T0.replace(tzinfo=UTC)
