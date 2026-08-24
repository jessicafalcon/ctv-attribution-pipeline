"""LIVE: schema-registry compatibility is BACKWARD (Phase 18b, Done-when 3) — a
real data contract, not the Phase-2 `NONE`. Adding an optional field is accepted
and the Phase-1 golden fixtures still validate against the new consumer; removing a
required field is rejected (409). Verified against the pinned Redpanda registry,
which rejects the removal as PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL (the models
are `additionalProperties: false`).

Uses throwaway subjects and deletes them before and after, so it never disturbs the
pipeline's real `<topic>-value` subjects. Runs under any `make test-int*` target
(needs the registry up); asserts the contract, never a profile's numbers.
"""

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from producer.models import Exposure
from producer.schemas import register_subject, set_compatibility

REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081")
_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
_FIXTURE = Path(__file__).parents[2] / "fixtures" / "tiny" / "exposures.jsonl"


class ExposureV2(Exposure):
    """The old consumer plus one optional field — the BACKWARD-safe evolution."""

    creative_id: str | None = None


def _reg(path: str, body: dict | None = None, method: str = "GET") -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{REGISTRY}{path}",
        data=data,
        headers={"Content-Type": _CONTENT_TYPE},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def _post_schema(subject: str, schema: dict) -> tuple[int, dict]:
    body = {"schema": json.dumps(schema), "schemaType": "JSON"}
    return _reg(f"/subjects/{subject}/versions", body, method="POST")


@pytest.fixture
def throwaway_subject(request: pytest.FixtureRequest) -> str:
    subject = f"schema-compat-{request.node.name}-value"
    _reg(f"/subjects/{subject}", method="DELETE")  # clear any prior versions
    yield subject
    _reg(f"/subjects/{subject}", method="DELETE")


def test_backward_accepts_an_optional_field_and_old_fixtures_still_validate(
    throwaway_subject: str,
) -> None:
    # The real registration path posts BACKWARD then the base schema.
    base_id = register_subject(REGISTRY, throwaway_subject, Exposure)
    assert isinstance(base_id, int)

    # Adding an optional field is BACKWARD-safe: the registry accepts the new version.
    v2_id = register_subject(REGISTRY, throwaway_subject, ExposureV2)
    assert isinstance(v2_id, int)

    # The Phase-1 golden exposures (written under the OLD schema) still validate
    # against both the old model and the new consumer (optional field → default None).
    rows = [json.loads(line) for line in _FIXTURE.read_text().splitlines() if line]
    assert rows, "golden exposure fixture is empty"
    for row in rows:
        Exposure.model_validate(row)
        assert ExposureV2.model_validate(row).creative_id is None


def test_backward_rejects_removing_a_required_field(throwaway_subject: str) -> None:
    set_compatibility(REGISTRY, throwaway_subject)  # BACKWARD
    status, _ = _post_schema(throwaway_subject, Exposure.model_json_schema())
    assert status == 200

    # Drop a required field. Under BACKWARD on a closed content model the registry
    # refuses the new version with 409.
    broken = json.loads(json.dumps(Exposure.model_json_schema()))
    broken["properties"].pop("campaign_id")
    broken["required"] = [f for f in broken["required"] if f != "campaign_id"]
    status, body = _post_schema(throwaway_subject, broken)
    assert status == 409, body
