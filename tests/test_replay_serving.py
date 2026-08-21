"""`make replay-serving` (Done-when 5): truncate the two serving tables, reload every
day the lake holds, stamp eval_meta. Offline: tmp lake, stub ClickHouse client,
Dagster load captured at its seam. Also the Makefile side: the target stamps the
marker for its PROFILE and passes --confirm only under CONFIRM=yes.
"""

import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import orchestration.replay as rp
from lake.land_exposures import land
from producer.models import Exposure

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
    rp.replay(confirm=True)
    assert client.commands == [
        "truncate table exposures_landed",
        "truncate table attributed_conversions",
    ]
    assert loaded == [{"2026-08-01", "2026-08-05"}]


def test_replay_refuses_an_empty_lake_before_touching_clickhouse(monkeypatch) -> None:
    client, loaded = _Client(), []
    _stub(monkeypatch, client, loaded)
    with pytest.raises(rp.EmptyLakeError, match="refusing to truncate"):
        rp.replay(confirm=True)
    assert client.commands == [] and loaded == []


def test_replay_prompt_aborts_without_yes(monkeypatch) -> None:
    land([_exp("e-1", datetime(2026, 8, 1, 12, tzinfo=UTC))])
    client, loaded = _Client(), []
    _stub(monkeypatch, client, loaded)
    monkeypatch.setattr("builtins.input", lambda prompt: "no")
    with pytest.raises(SystemExit):
        rp.replay(confirm=False)
    assert client.commands == []


def _dry_run(*args: str) -> str:
    return subprocess.run(
        ["make", "-n", "replay-serving", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def test_make_replay_serving_stamps_eval_meta_and_gates_confirm() -> None:
    out = _dry_run("PROFILE=long_delay")
    assert re.search(r'orchestration\.run replay --profile "long_delay"\s*$', out, re.M)
    assert "--confirm" not in out  # prompts unless CONFIRM=yes
    assert re.search(r'write_marker --profile "long_delay"', out)  # D8: stamps
    assert "--confirm" in _dry_run("PROFILE=long_delay", "CONFIRM=yes")
