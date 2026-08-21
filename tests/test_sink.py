"""Schema-contract drift guard: the hand-maintained ClickHouse column lists — the
loader's (lake/load_serving.py, the one product writer) and the test oracle's
(tests/oracle.py, the pre-Phase-17 direct sink's value mapping) — must match the
pydantic models they map. A field added to a model but not to a writer would
drift silently and only surface in the opt-in integration test — this pins it
offline."""

from lake.load_serving import ATTRIBUTED_COLS, EXPOSURE_COLS
from producer.models import AttributedConversion, Exposure
from tests.oracle import _ATTRIBUTED_COLS, _EXPOSURE_COLS


def test_attributed_cols_match_model_fields() -> None:
    assert set(ATTRIBUTED_COLS) == set(AttributedConversion.model_fields)
    assert set(_ATTRIBUTED_COLS) == set(AttributedConversion.model_fields)


def test_exposure_cols_match_model_fields() -> None:
    assert set(EXPOSURE_COLS) == set(Exposure.model_fields)
    assert set(_EXPOSURE_COLS) == set(Exposure.model_fields)
