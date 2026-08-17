import json

import pytest
from pydantic import ValidationError

from producer.config import load_profile
from producer.generate import generate
from producer.schemas import TOPIC_MODELS, topic_schema
from producer.serialize import canonical_bytes


def test_every_topic_has_a_model_derived_schema():
    assert set(TOPIC_MODELS) == {"exposures", "conversions", "device_graph"}
    for topic, model in TOPIC_MODELS.items():
        schema = topic_schema(topic)
        assert schema == model.model_json_schema()
        assert set(schema["required"]) == set(model.model_fields)


def test_generated_events_validate_against_their_models():
    stream = generate(load_profile("tiny"), 42)
    for topic, events in [
        ("exposures", stream.exposures),
        ("conversions", stream.conversions),
        ("device_graph", stream.graph.households),
    ]:
        for event in events:
            TOPIC_MODELS[topic].model_validate_json(canonical_bytes(event))


def test_extra_fields_are_rejected():
    stream = generate(load_profile("tiny"), 42)
    payload = json.loads(canonical_bytes(stream.exposures[0]))
    payload["surprise"] = 1
    with pytest.raises(ValidationError):
        TOPIC_MODELS["exposures"].model_validate(payload)
