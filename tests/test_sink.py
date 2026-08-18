"""Schema-contract drift guard: the ClickHouse sink's hand-maintained column
lists must match the pydantic models they insert. A field added to a model but
not to the sink (or vice versa) would drift silently and only surface in the
opt-in integration test — this pins it offline."""

from producer.models import AttributedConversion, Exposure
from streaming.sink import _ATTRIBUTED_COLS, _EXPOSURE_COLS


def test_attributed_cols_match_model_fields() -> None:
    assert set(_ATTRIBUTED_COLS) == set(AttributedConversion.model_fields)


def test_exposure_cols_match_model_fields() -> None:
    assert set(_EXPOSURE_COLS) == set(Exposure.model_fields)
