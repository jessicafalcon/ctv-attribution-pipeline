"""Canonical serialization: same model → same bytes, always.

Sorted keys + compact separators so byte-identity across runs is a property
of the data, not of dict ordering.
"""

import json

from pydantic import BaseModel


def canonical_bytes(model: BaseModel) -> bytes:
    return json.dumps(
        model.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()


def jsonl(models: list) -> str:
    return "".join(canonical_bytes(m).decode() + "\n" for m in models)
