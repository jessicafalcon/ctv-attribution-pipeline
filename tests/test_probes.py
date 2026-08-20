"""Probe registry — structural, no services. Proves the probe contract without a
stack: every probe binds its parameters server-side (no f-string of a param, so no
SQL-injection surface), the tool schemas are stable and typed, and the dispatcher
maps rows to the declared result model. The live read path is
tests/integration/test_agent_readonly.py."""

import re
from types import SimpleNamespace

from agent import probes
from agent.probes import NoParam

# {name:Type} placeholders — clickhouse-connect server-side binding.
_PLACEHOLDER = re.compile(r"\{(\w+):\w+\}")


def test_registry_has_the_five_probes_in_stable_order() -> None:
    assert list(probes.REGISTRY) == [
        "ip_cluster_detail",
        "campaign_attributed_by_day",
        "campaign_restatement",
        "window_edge_distribution",
        "genre_reach_detail",
    ]


def test_every_probe_sql_aliases_cover_its_result_fields() -> None:
    # CR-2 guard: name-based mapping (probes.run zips column_names → model) is only
    # order-safe if every result-model field is produced as a SQL column alias. Assert
    # each model field name appears as an `as <name>` alias, so a dropped/renamed alias
    # is a located test failure here, not a silent mismap or a runtime-only surprise.
    alias = re.compile(r"\bas\s+(\w+)", re.IGNORECASE)
    for probe in probes.REGISTRY.values():
        aliases = set(alias.findall(probe.sql))
        fields = set(probe.result.model_fields)
        assert fields <= aliases, f"{probe.name}: fields {fields - aliases} not aliased"


def test_submit_finding_is_not_a_probe() -> None:
    # The terminal tool lives in the loop, never the probe registry.
    assert "submit_finding" not in probes.REGISTRY


def test_every_probe_binds_params_server_side_no_fstring() -> None:
    for probe in probes.REGISTRY.values():
        placeholders = set(_PLACEHOLDER.findall(probe.sql))
        declared = set(probe.params.model_fields)
        # Every placeholder references a declared param (nothing unbound), and there
        # is no `{` left that isn't a typed placeholder (no f-string interpolation).
        assert placeholders <= declared, probe.name
        stray = re.findall(r"\{(?!\w+:\w+\})", probe.sql)
        assert not stray, f"{probe.name} has a non-placeholder brace"
        if probe.params is NoParam:
            assert not placeholders, f"{probe.name} takes no params but binds some"
        else:
            # Each declared param is actually used in the SQL.
            assert declared == placeholders, probe.name


def test_tool_schemas_are_typed_and_named() -> None:
    schemas = probes.tool_schemas()
    assert len(schemas) == 5
    for schema in schemas:
        assert set(schema) == {"name", "description", "input_schema"}
        assert schema["input_schema"]["type"] == "object"


def test_run_validates_params_and_maps_rows() -> None:
    captured = {}

    class FakeClient:
        def query(self, sql, parameters=None):
            captured["sql"] = sql
            captured["parameters"] = parameters
            return SimpleNamespace(
                result_rows=[("h-1", 5, 3), ("h-2", 2, 2)],
                column_names=[
                    "household_id",
                    "attributed_conversions",
                    "max_candidate_count",
                ],
            )

    rows = probes.run(FakeClient(), "ip_cluster_detail", {"ip": "1.2.3.4"})
    # Params bound, not interpolated: the value is passed via `parameters`, the SQL
    # keeps its placeholder.
    assert captured["parameters"] == {"ip": "1.2.3.4"}
    assert "{ip:String}" in captured["sql"]
    assert rows[0].household_id == "h-1"
    assert rows[0].attributed_conversions == 5
    assert rows[0].max_candidate_count == 3


def test_run_rejects_unknown_probe() -> None:
    import pytest

    with pytest.raises(KeyError):
        probes.run(object(), "nope", {})
