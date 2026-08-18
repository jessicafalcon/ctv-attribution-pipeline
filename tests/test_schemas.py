import contextlib
import io
import json
import urllib.error
from typing import Any

import pytest
from pydantic import ValidationError

from producer.config import load_profile
from producer.generate import generate
from producer.schemas import TOPIC_MODELS, register_schemas, topic_schema
from producer.serialize import canonical_bytes


def test_every_topic_has_a_model_derived_schema() -> None:
    assert set(TOPIC_MODELS) == {"exposures", "conversions", "device_graph"}
    for topic, model in TOPIC_MODELS.items():
        schema = topic_schema(topic)
        assert schema == model.model_json_schema()
        assert set(schema["required"]) == set(model.model_fields)


def test_generated_events_validate_against_their_models() -> None:
    stream = generate(load_profile("tiny"), 42)
    for topic, events in [
        ("exposures", stream.exposures),
        ("conversions", stream.conversions),
        ("device_graph", stream.graph.households),
    ]:
        for event in events:
            TOPIC_MODELS[topic].model_validate_json(canonical_bytes(event))


def test_extra_fields_are_rejected() -> None:
    stream = generate(load_profile("tiny"), 42)
    payload = json.loads(canonical_bytes(stream.exposures[0]))
    payload["surprise"] = 1
    with pytest.raises(ValidationError):
        TOPIC_MODELS["exposures"].model_validate(payload)


def test_register_schemas_sets_none_then_posts_each_subject(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, dict[str, Any]]] = []

    def fake_urlopen(req: Any, timeout: float | None = None) -> Any:
        assert timeout is not None
        calls.append((req.full_url, req.get_method(), json.loads(req.data)))
        return contextlib.closing(io.BytesIO(json.dumps({"id": len(calls)}).encode()))

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    ids = register_schemas("http://registry.test")

    # Each subject: PUT compatibility NONE, then POST the version — in that order.
    assert [(url, method) for url, method, _ in calls] == [
        step
        for t in TOPIC_MODELS
        for step in [
            (f"http://registry.test/config/{t}-value", "PUT"),
            (f"http://registry.test/subjects/{t}-value/versions", "POST"),
        ]
    ]
    posts = [(url, body) for url, method, body in calls if method == "POST"]
    for (_, body), topic in zip(posts, TOPIC_MODELS, strict=True):
        assert body["schemaType"] == "JSON"
        assert json.loads(body["schema"]) == topic_schema(topic)
    puts = [body for _, method, body in calls if method == "PUT"]
    assert all(body == {"compatibility": "NONE"} for body in puts)
    assert set(ids) == {f"{t}-value" for t in TOPIC_MODELS}


def test_register_schemas_propagates_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req: Any, timeout: float | None = None) -> Any:
        raise urllib.error.URLError("registry down")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    with pytest.raises(urllib.error.URLError):
        register_schemas("http://registry.test")


def test_register_schemas_rejects_non_http_url() -> None:
    with pytest.raises(ValueError):
        register_schemas("ftp://registry.test")
