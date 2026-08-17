"""JSON Schemas generated from the pydantic models and registered per topic.

Registry subjects follow the `<topic>-value` convention. Registration uses the
registry's plain HTTP API (stdlib urllib) — the confluent-kafka schema-registry
extra would pull extra dependencies for one POST per topic (see DECISIONS.md).
"""

import json
import urllib.request

from pydantic import BaseModel

from producer.models import Conversion, Exposure, Household

TOPIC_MODELS: dict[str, type[BaseModel]] = {
    "exposures": Exposure,
    "conversions": Conversion,
    "device_graph": Household,
}


def topic_schema(topic: str) -> dict:
    return TOPIC_MODELS[topic].model_json_schema()


def register_schemas(registry_url: str) -> dict[str, int]:
    """Register every topic schema; returns subject → schema id. Raises on failure."""
    ids: dict[str, int] = {}
    for topic in TOPIC_MODELS:
        subject = f"{topic}-value"
        body = json.dumps(
            {"schema": json.dumps(topic_schema(topic)), "schemaType": "JSON"}
        ).encode()
        req = urllib.request.Request(
            f"{registry_url}/subjects/{subject}/versions",
            data=body,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            ids[subject] = json.load(resp)["id"]
    return ids
