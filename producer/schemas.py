"""JSON Schemas generated from the pydantic models and registered per topic.

Registry subjects follow the `<topic>-value` convention. Registration uses the
registry's plain HTTP API (stdlib urllib) — the confluent-kafka schema-registry
extra would pull extra dependencies for one POST per topic (see DECISIONS.md).

Every subject is set to compatibility BACKWARD before its schema is posted
(Phase 18b), matching the registry's own global default and making the registry a
real data contract: a consumer on the newest schema can read data written under any
older one, so a producer may ADD an optional field but the registry 409s the removal
or rename of a required one. (Phase 2 used per-subject NONE — register anything —
because single-writer dev seeding had no evolution story yet; BACKWARD replaces it
now that the contract is the point. `_compat_level` is the single place the mode is
named.) Verified against Redpanda: PUT /config/<subject> works before the subject's
first version exists.
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


def _compat_level() -> str:
    """The schema-registry compatibility mode set on every subject (Phase 18b).

    BACKWARD: a consumer built on the LATEST schema can read data written under any
    OLDER registered schema — so adding an optional field (or widening) is accepted,
    while removing or renaming a required field is rejected at registration (409). The
    single place the mode is named, so the data-contract decision lives in one line."""
    return "BACKWARD"


def set_compatibility(
    registry_url: str, subject: str, level: str | None = None
) -> None:
    """Set subject-level compatibility (BACKWARD since Phase 18b — see module
    docstring). `level` overrides `_compat_level()` for callers that need it."""
    _check_url(registry_url)
    body = {"compatibility": level or _compat_level()}
    _request(f"{registry_url}/config/{subject}", body, method="PUT")


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
