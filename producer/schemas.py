"""JSON Schemas generated from the pydantic models and registered per topic.

Registry subjects follow the `<topic>-value` convention. Registration uses the
registry's plain HTTP API (stdlib urllib) — the confluent-kafka schema-registry
extra would pull extra dependencies for one POST per topic (see DECISIONS.md).

Every subject is set to compatibility NONE before its schema is posted. The
registry's global default is BACKWARD; under it, re-registering a *changed*
model during dev (add/rename a field) is rejected with 409 and fails the seed
or the engine. This is single-writer dev infra with no schema-evolution
story yet, so per-subject NONE lets a model change re-register freely. Tighten
per subject when schema evolution becomes a v1+ concern (ARCHITECTURE out-of-
scope). Verified against Redpanda: PUT /config/<subject> works before the
subject's first version exists.
"""

import json
import urllib.request
from typing import Any

from pydantic import BaseModel

from producer.models import Conversion, Exposure, Household

TOPIC_MODELS: dict[str, type[BaseModel]] = {
    "exposures": Exposure,
    "conversions": Conversion,
    "device_graph": Household,
}

REQUEST_TIMEOUT_S = 10
_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def topic_schema(topic: str) -> dict[str, Any]:
    return TOPIC_MODELS[topic].model_json_schema()


def _request(url: str, body: dict[str, Any], method: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": _REGISTRY_CONTENT_TYPE},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as resp:
        return json.load(resp)


def _check_url(registry_url: str) -> None:
    if not registry_url.startswith(("http://", "https://")):
        raise ValueError(f"registry url must be http(s), got {registry_url!r}")


def set_compatibility(registry_url: str, subject: str, level: str = "NONE") -> None:
    """Set subject-level compatibility (dev default NONE — see module docstring)."""
    _check_url(registry_url)
    _request(f"{registry_url}/config/{subject}", {"compatibility": level}, method="PUT")


def register_subject(registry_url: str, subject: str, model: type[BaseModel]) -> int:
    """Set compatibility, then register the model's JSON Schema. Returns schema id."""
    _check_url(registry_url)
    set_compatibility(registry_url, subject)
    body = {"schema": json.dumps(model.model_json_schema()), "schemaType": "JSON"}
    return _request(f"{registry_url}/subjects/{subject}/versions", body, method="POST")[
        "id"
    ]


def register_schemas(registry_url: str) -> dict[str, int]:
    """Register every producer topic schema; returns subject → schema id."""
    return {
        f"{topic}-value": register_subject(registry_url, f"{topic}-value", model)
        for topic, model in TOPIC_MODELS.items()
    }
