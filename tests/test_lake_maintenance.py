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


def test_compaction_on_the_attributed_table_keeps_rows_and_schema() -> None:
    # raw.attributed_conversions is the schema-risky one (two list columns, three
    # nullables) — the rewrite (`overwrite` with a day filter) must round-trip it.
    from lake.land_attributed import land_attributed
    from lake.read_attributed import read_current
    from tests.test_lake_attributed import _row

    t = datetime(2026, 8, 1, 12, tzinfo=UTC)
    rows = [
        _row("c-1", event_time=t, order_id="o-1"),
        _row(
            "c-2",
            event_time=t,
            hh="h-b",
            resolution="ip",
            ambiguous=True,
            candidate_count=2,
            exposure_id=None,
            assists=[],
            attributed=False,
            reason="ambiguous_ip",
            candidate_households=["h-a", "h-b"],
        ),
    ]
    for _ in range(3):
        land_attributed(rows)
    table = cat.ensure_attributed()
    before = [r.model_dump() for r in read_current()]
    out = maintain(table, max_age=timedelta(days=365), now=datetime.now(UTC))
    assert out["compacted_days"] == 1
    table = cat.ensure_attributed()
    assert set(_files_per_partition(table).values()) == {1}
    assert [r.model_dump() for r in read_current()] == before
    assert [f.name for f in table.schema().fields] == list(before[0].keys())


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


def test_expiry_is_metadata_only_on_this_pyiceberg() -> None:
    # The known limitation, pinned (BACKLOG 45): pyiceberg 0.11.1 has no
    # remove_orphan_files, so compaction + expiry do NOT shrink the directory —
    # orphaned Parquet stays. When a pyiceberg bump adds it, the `raises` below
    # fails: wire it into lake_maintenance, then flip this test to assert the
    # on-disk count FALLS and close the BACKLOG row.
    import glob
    import os

    from pyiceberg.table.maintenance import MaintenanceTable

    for i in range(3):
        land([_exp(i, 1)])
    root = os.environ["LAKE_ROOT"]
    on_disk = lambda: len(glob.glob(f"{root}/**/*.parquet", recursive=True))  # noqa: E731
    before = on_disk()
    table = cat.ensure_exposures()
    maintain(table, max_age=timedelta(0), now=datetime.now(UTC) + timedelta(days=1))
    assert on_disk() >= before  # nothing reclaimed (rewrite adds, expiry removes none)
    with pytest.raises(AttributeError):
        MaintenanceTable.remove_orphan_files  # noqa: B018


def test_lake_days_come_from_the_data() -> None:
    land([_exp(1, 1), _exp(2, 5)])
    assert lake_days() == {"2026-08-01", "2026-08-05"}
