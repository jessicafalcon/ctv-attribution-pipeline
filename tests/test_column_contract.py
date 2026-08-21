"""Phase-17 column contract (spec D3): the attributed row has ONE shape, in ONE
order, everywhere it is written or read back — the pydantic model, the ClickHouse
sink's column list, the DDL, and reconcile's read-back select. 19 columns. The
Iceberg `raw.attributed_conversions` schema joins this assertion when it lands
(D7/D3 commit). tests/test_sink.py pins the SET; this pins the ORDER."""

import re
from pathlib import Path

from lake.iceberg_catalog import ATTRIBUTED_SCHEMA, EXPOSURE_SCHEMA
from lake.land_attributed import _ARROW_SCHEMA as _ATTRIBUTED_ARROW
from lake.land_exposures import _ARROW_SCHEMA as _EXPOSURE_ARROW
from producer.models import AttributedConversion, Exposure
from reconcile.reconcile import _CANDIDATE_COLS
from streaming.sink import _ATTRIBUTED_COLS, _EXPOSURE_COLS

DDL = Path(__file__).parent.parent / "clickhouse" / "ddl.sql"


def _ddl_columns(table: str) -> list[str]:
    body = re.search(
        rf"create table if not exists {table}\s*\((.*?)\)\s*engine",
        DDL.read_text(),
        re.S,
    ).group(1)
    return [line.split()[0] for line in body.strip().splitlines() if line.strip()]


def test_attributed_conversions_is_19_columns_in_model_order() -> None:
    model = list(AttributedConversion.model_fields)
    assert len(model) == 19
    assert model[-2:] == ["reason", "candidate_households"]
    assert _ATTRIBUTED_COLS == model
    assert _ddl_columns("attributed_conversions") == model
    assert [c.strip() for c in _CANDIDATE_COLS.split(",")] == model
    # the lake copy (spec D3): Iceberg schema and its arrow writer, same order
    assert [f.name for f in ATTRIBUTED_SCHEMA.fields] == model
    assert _ATTRIBUTED_ARROW.names == model
    # nullable exactly where the model is Optional
    optional = {"order_id", "exposure_id", "reason"}
    assert {f.name for f in ATTRIBUTED_SCHEMA.fields if not f.required} == optional
    assert {f.name for f in _ATTRIBUTED_ARROW if f.nullable} == optional


def test_exposures_landed_matches_the_exposure_model_order() -> None:
    model = list(Exposure.model_fields)
    assert _EXPOSURE_COLS == model
    assert _ddl_columns("exposures_landed") == model
    assert [f.name for f in EXPOSURE_SCHEMA.fields] == model
    assert _EXPOSURE_ARROW.names == model
