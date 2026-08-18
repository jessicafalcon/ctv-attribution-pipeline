"""Schema parity for the resolve stage's own output subject: what gets
registered under conversions_resolved-value must equal
ResolvedConversion.model_json_schema(). The producer topics get this guard in
tests/test_schemas.py; the resolve stage owns this subject (deliberately not in
producer TOPIC_MODELS — it produces, not seeds, this topic), so it needs its
own. Mocked registry — no live services."""

import contextlib
import io
import json
from typing import Any

import pytest

from producer.models import ResolvedConversion
from producer.schemas import register_subject
from resolve.stage import RESOLVED_SUBJECT


def test_registered_resolved_schema_matches_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posts: list[dict[str, Any]] = []

    def fake_urlopen(req: Any, timeout: float | None = None) -> Any:
        if req.get_method() == "POST":
            posts.append(json.loads(req.data))
        return contextlib.closing(io.BytesIO(json.dumps({"id": 7}).encode()))

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    schema_id = register_subject(
        "http://registry.test", RESOLVED_SUBJECT, ResolvedConversion
    )

    assert schema_id == 7
    assert len(posts) == 1
    assert posts[0]["schemaType"] == "JSON"
    assert json.loads(posts[0]["schema"]) == ResolvedConversion.model_json_schema()
