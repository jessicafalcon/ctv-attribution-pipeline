"""`make replay-serving` (Done-when 5): truncate the serving tables, reload every
day the lake holds, stamp eval_meta — all in one process (`lake.destructive
replay`). Offline: tmp lake, stub ClickHouse client, Dagster load captured at its
seam. Also the Makefile side: the recipe is one line and passes --yes only under
a command-line CONFIRM=yes.
"""

import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import orchestration.replay as rp
from lake.land_exposures import land
from producer.models import Exposure
from tests._env import clean_env

REPO_ROOT = Path(__file__).parent.parent


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))


class _Client:
    def __init__(self):
        self.commands: list[str] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)


def _exp(eid: str, event: datetime) -> Exposure:
    return Exposure(
        exposure_id=eid,
        event_time=event,
        ingest_time=event + timedelta(minutes=1),
        campaign_id="c",
        household_id="h-1",
        ip="1",
        app_id="a",
        program_genre="g",
        spend=1.0,
    )


def _stub(monkeypatch, client: _Client, loaded: list[set[str]]) -> None:
    monkeypatch.setattr(rp, "apply_ddl", lambda: None)
    monkeypatch.setattr(rp, "connect", lambda: client)
    import orchestration.run as run

    monkeypatch.setattr(
        run,
        "materialize_load",
        lambda days: loaded.append(set(days)) or {"exposures": 1, "attributed": 0},
    )


def test_replay_truncates_both_tables_then_reloads_every_lake_day(monkeypatch) -> None:
    t = datetime(2026, 8, 1, 12, tzinfo=UTC)
    land([_exp("e-1", t), _exp("e-2", t + timedelta(days=4))])
    client, loaded = _Client(), []
    _stub(monkeypatch, client, loaded)
    rp.truncate_and_reload(rp.lake_days())
    assert client.commands == [
        "truncate table exposures_landed",
        "truncate table attributed_conversions",
        "truncate table eval_meta",  # a stale marker over a half-loaded DB, never
    ]
    # derived state is NOT touched by a replay (the next reconcile pass refreshes
    # it) — the documented boundary, pinned
    derived = ("campaign_hourly", "report_snapshots")
    assert not any(d in c for c in client.commands for d in derived)
    assert loaded == [{"2026-08-01", "2026-08-05"}]


def test_destructive_replay_refuses_an_out_of_calendar_day_before_truncate(
    monkeypatch,
) -> None:
    # Round 7: narrowing the refusal tuple once let an UnknownPartitionError
    # escape from materialize_load AFTER the TRUNCATE. Now the lake's days are
    # checked against the static calendar before the prompt; nothing is truncated.
    # (replay() directly: main() would refuse the test's own LAKE_ROOT first; the
    # CLI mapping RefusalError → one line + exit 1 is pinned in test_destructive.)
    import lake.destructive as d
    import lake.iceberg_catalog as cat

    monkeypatch.setattr(cat, "_profile", "tiny")
    land([_exp("e-1", datetime(2020, 1, 1, 12, tzinfo=UTC))])  # outside the calendar
    client, loaded = _Client(), []
    _stub(monkeypatch, client, loaded)
    with pytest.raises(d.RefusalError, match="2020-01-01.*outside the static calendar"):
        d.replay(yes=True)
    assert client.commands == [] and loaded == []  # no truncate, no load


def test_replay_refuses_an_empty_lake_before_touching_clickhouse(monkeypatch) -> None:
    # the CLI refuses BEFORE truncate (tests/test_destructive.py); the library
    # act refuses too, so no caller can truncate to reload nothing
    client, loaded = _Client(), []
    _stub(monkeypatch, client, loaded)
    assert rp.lake_days() == set()
    with pytest.raises(rp.EmptyLakeError):
        rp.truncate_and_reload(set())
    assert client.commands == [] and loaded == []


def _dry_run(*args: str) -> str:
    return subprocess.run(
        ["make", "-n", "replay-serving", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=clean_env(),
    ).stdout


def test_make_replay_serving_is_one_process_and_gates_confirm() -> None:
    out = _dry_run("PROFILE=long_delay")
    lines = [ln for ln in out.splitlines() if ln.strip()]
    assert len(lines) == 1 and re.search(
        r'lake\.destructive replay --profile "long_delay"\s*$', lines[0]
    )
    assert "--yes" not in out  # prompts unless CONFIRM=yes on the command line
    assert "--yes" in _dry_run("PROFILE=long_delay", "CONFIRM=yes")


def test_destructive_replay_stamps_eval_meta_after_the_load(monkeypatch) -> None:
    # D8: the marker is written IN the replay process, after the reload — never
    # a second recipe line that `make -i` could run after a refusal (round 4).
    import clickhouse.write_marker as wm
    import lake.destructive as d
    import lake.iceberg_catalog as cat

    events: list[str] = []
    monkeypatch.setattr(cat, "_profile", "long_delay")
    monkeypatch.setattr(rp, "lake_days", lambda: {"2026-08-01"})
    monkeypatch.setattr(
        rp,
        "truncate_and_reload",
        lambda days: events.append("load") or {"exposures": 1, "attributed": 1},
    )
    monkeypatch.setattr(wm, "write_marker", lambda p: events.append(f"marker:{p}"))
    d.replay(yes=True)
    assert events == ["load", "marker:long_delay"]
