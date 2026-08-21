"""Phase-17 lake-of-record unit tests (offline: local Iceberg + DuckDB, no services).

- the attributed row survives the Iceberg round-trip field-for-field, naive-UTC-ms
  (the same fidelity gate tests/test_lake_roundtrip.py gives exposures);
- the lake is an append-only log and `read_current` is the argMax(processed_at)
  read (spec D4): a hot row + its later reconciled row → the reconciled one;
  exact re-lands collapse;
- both raw tables carry `day × bucket(N, household_id)` with the SAME recorded N
  (spec D7), and a lake created under another layout is refused, not re-specced.
"""

from datetime import UTC, datetime, timedelta

import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform

from lake import iceberg_catalog as cat
from lake.land_attributed import land_attributed
from lake.read_attributed import read_current
from producer.models import AttributedConversion


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))


T = datetime(2026, 8, 1, 12, 0, 0, 123_000, tzinfo=UTC)


def _row(cid: str, hh: str = "h-1", **over) -> AttributedConversion:
    base = dict(
        conversion_id=cid,
        event_time=T,
        ingest_time=T + timedelta(minutes=5),
        device_id="d-1",
        ip="10.0.0.1",
        conversion_type="purchase",
        revenue=19.99,
        order_id=None,
        household_id=hh,
        resolution="device",
        ambiguous=False,
        candidate_count=1,
        exposure_id="e-1",
        assists=["e-0"],
        attributed=True,
        path="hot",
        processed_at=T + timedelta(minutes=5),
        reason=None,
        candidate_households=[],
    )
    return AttributedConversion(**{**base, **over})


def _naive_ms(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None)


def test_attributed_row_round_trips_field_for_field() -> None:
    deferred = _row(
        "c-2",
        hh="h-a",
        resolution="ip",
        ambiguous=True,
        candidate_count=2,
        exposure_id=None,
        assists=[],
        attributed=False,
        reason="ambiguous_ip",
        candidate_households=["h-a", "h-b"],
        order_id="o-7",
    )
    land_attributed([_row("c-1"), deferred])
    back = {r.conversion_id: r for r in read_current()}
    assert set(back) == {"c-1", "c-2"}
    for original in (_row("c-1"), deferred):
        got = back[original.conversion_id]
        for k in AttributedConversion.model_fields:
            want = getattr(original, k)
            if k in ("event_time", "ingest_time", "processed_at"):
                want = _naive_ms(want)
            assert getattr(got, k) == want, k
        assert got.event_time.tzinfo is None  # the clickhouse-connect shape


def test_read_current_is_argmax_processed_at_over_an_append_only_log() -> None:
    hot = _row(
        "c-1", attributed=False, exposure_id=None, assists=[], reason="state_miss"
    )
    land_attributed([hot])
    land_attributed([hot])  # an exact re-land accumulates in the lake …
    reconciled = hot.model_copy(
        update={
            "attributed": True,
            "exposure_id": "e-9",
            "reason": None,
            "path": "reconciled",
            "processed_at": hot.processed_at + timedelta(seconds=1),
        }
    )
    land_attributed([reconciled])
    physical = cat.ensure_attributed().scan().to_arrow().num_rows
    assert physical == 3  # … the log keeps every append (spec D4)
    (current,) = read_current()  # … and the read collapses to the latest version
    assert (current.path, current.exposure_id, current.attributed) == (
        "reconciled",
        "e-9",
        True,
    )


def test_day_filter_prunes_by_the_conversions_event_time() -> None:
    land_attributed([_row("c-1"), _row("c-2", event_time=T + timedelta(days=3))])
    assert [r.conversion_id for r in read_current(days=["2026-08-04"])] == ["c-2"]
    assert [
        r.conversion_id for r in read_current(min_event_time=T + timedelta(days=1))
    ] == ["c-2"]


def test_both_raw_tables_share_the_bucket_layout_and_count() -> None:
    exp, att = cat.ensure_exposures(), cat.ensure_attributed()
    assert cat.bucket_count_of(exp) == cat.bucket_count_of(att) == cat.BUCKET_COUNT
    for table, hh_id in ((exp, 5), (att, 9)):
        fields = table.spec().fields
        assert [(f.name, str(f.transform)) for f in fields] == [
            ("event_time_day", "day"),
            ("household_bucket", f"bucket[{cat.BUCKET_COUNT}]"),
        ]
        assert fields[1].source_id == hh_id  # bucketed on household_id


def test_a_lake_under_another_layout_is_refused_not_respecced() -> None:
    # A Phase-12 lake: day-only, no bucket property. Appending into it would make
    # the partition-local join silently wrong — refuse, name make lake-reset.
    catalog = cat.connect_catalog()
    catalog.create_namespace_if_not_exists(cat.NAMESPACE)
    catalog.create_table(
        cat.EXPOSURES_TABLE,
        schema=cat.EXPOSURE_SCHEMA,
        partition_spec=PartitionSpec(
            PartitionField(2, 1000, DayTransform(), "event_time_day")
        ),
    )
    with pytest.raises(cat.LakeLayoutError, match="make lake-reset"):
        cat.ensure_exposures()
