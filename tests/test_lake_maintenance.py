"""Phase-17 lake hygiene (spec D10), offline: compaction leaves the rows
unchanged and reduces files to one per (day, bucket); snapshot expiry keeps the
current snapshot; `lake_days` discovers partitions from the data.
"""

from datetime import UTC, datetime, timedelta

import pytest

from lake import iceberg_catalog as cat
from lake.land_exposures import land
from lake.maintenance import days_with_small_files, maintain
from lake.read_exposures import read_exposures_for_days
from orchestration.replay import lake_days
from producer.models import Exposure


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))


def _exp(i: int, day: int) -> Exposure:
    t = datetime(2026, 8, day, 12, tzinfo=UTC)
    return Exposure(
        exposure_id=f"e-{i}",
        event_time=t,
        ingest_time=t,
        campaign_id="c",
        household_id=f"h-{i}",
        ip="1",
        app_id="a",
        program_genre="g",
        spend=1.0,
    )


def _files_per_partition(table) -> dict:
    return {
        (
            p["partition"]["event_time_day"].isoformat(),
            p["partition"]["household_bucket"],
        ): p["file_count"]
        for p in table.inspect.partitions().to_pylist()
    }


def test_compaction_rewrites_to_one_file_per_partition_with_rows_unchanged() -> None:
    batch = [_exp(i, 1) for i in range(16)] + [_exp(100, 2)]
    for _ in range(3):  # three appends → three files in every touched bucket
        land(batch)
    table = cat.ensure_exposures()
    assert max(_files_per_partition(table).values()) == 3
    assert days_with_small_files(table) == ["2026-08-01", "2026-08-02"]
    physical_before = table.scan().to_arrow().num_rows
    before = read_exposures_for_days(["2026-08-01", "2026-08-02"])

    out = maintain(table, max_age=timedelta(days=365), now=datetime.now(UTC))

    table = cat.ensure_exposures()
    assert out["compacted_days"] == 2
    assert set(_files_per_partition(table).values()) == {1}
    assert table.scan().to_arrow().num_rows == physical_before  # no dedup on rewrite
    assert read_exposures_for_days(["2026-08-01", "2026-08-02"]) == before


def test_expiry_drops_old_snapshots_but_keeps_the_current_one() -> None:
    for i in range(3):
        land([_exp(i, 1)])
    table = cat.ensure_exposures()
    assert len(table.metadata.snapshots) == 3
    out = maintain(
        table, max_age=timedelta(0), now=datetime.now(UTC) + timedelta(days=1)
    )
    table = cat.ensure_exposures()
    assert out["expired_snapshots"] >= 2
    assert len(table.metadata.snapshots) >= 1
    assert len(read_exposures_for_days(["2026-08-01"])) == 3  # rows intact


def test_lake_days_come_from_the_data() -> None:
    land([_exp(1, 1), _exp(2, 5)])
    assert lake_days() == {"2026-08-01", "2026-08-05"}
