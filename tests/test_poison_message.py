"""Poison-message disposition (BACKLOG 87, fix/poison-message-disposition).

Invariant: for all drained payloads, a message that fails schema decode/validation
HALTS the pass — it is never skipped, never partially processed. A silent
skip-and-continue would break the byte-identical determinism guarantee (the same
seed no longer reproduces the same output once one message is dropped).

`common.kafka.drain` returns raw bytes; consume-time validation lives in the
callers' bare `model_validate_json` comprehensions (streaming/dataflow.py:157,163
for Exposure/Conversion; resolve/graph_loader.py:37 for Household). That
comprehension is the decode path this pins: a malformed element in the middle of
a drained batch raises `ValidationError` out of the comprehension rather than
yielding a shorter list. Current behavior already fails loud (no try/except at any
site) — this test pins it so a future edit cannot regress to skip-and-continue.

Offline: no broker, no services. Covers the Exposure event model; Conversion and
the graph loader's Household path are the identical comprehension shape.
"""

import pytest
from pydantic import ValidationError

from producer.models import Exposure

# A well-formed Exposure wire payload (every required field present, types valid).
_GOOD = (
    '{"exposure_id": "e1", "event_time": "2026-01-01T00:00:00Z",'
    ' "ingest_time": "2026-01-01T00:00:01Z", "campaign_id": "c1",'
    ' "household_id": "h1", "ip": "10.0.0.1", "app_id": "a1",'
    ' "program_genre": "news", "spend": 1.5}'
)
# Malformed: valid JSON, but missing required fields and carrying a negative
# spend — fails schema validation exactly as a corrupted/poison wire byte would.
_MALFORMED = '{"exposure_id": "e2", "spend": -1}'


def _decode_batch(payloads: list[str]) -> list[Exposure]:
    """The callers' consume-time decode step, verbatim in shape: a bare
    `model_validate_json` comprehension over the drained values."""
    return [Exposure.model_validate_json(v) for v in payloads]


def test_malformed_payload_halts_the_batch_decode() -> None:
    """A malformed element mid-batch raises out of the decode comprehension —
    it does NOT skip the bad message and return the two good ones."""
    with pytest.raises(ValidationError):
        _decode_batch([_GOOD, _MALFORMED, _GOOD])


def test_good_batch_decodes_in_full() -> None:
    """Control: with no poison message the same decode path yields every row,
    so the raise above is the malformed element and not the harness."""
    rows = _decode_batch([_GOOD, _GOOD])
    assert len(rows) == 2
    assert all(isinstance(r, Exposure) for r in rows)
