"""The agent loop — driven by a MOCKED Anthropic client, zero tokens. Proves the
control flow the live run can't assert deterministically (Ruling C): probe dispatch,
the ≥1-probe contract, the escalation paths, the round budget, and that the request
carries the cached prefix + adaptive thinking + effort."""

import json
from pathlib import Path
from types import SimpleNamespace

from agent import loop
from agent.context import (
    AttributionContext,
    IpCluster,
    IpClusterStats,
    WindowEdgeStats,
)


def make_ctx(ip: str = "9.9.9.9") -> AttributionContext:
    return AttributionContext(
        profile="shared_ip_spike",
        processed=100,
        attributed=80,
        match_rate=0.8,
        match_rate_by_day=[],
        campaigns=[],
        restatements=[],
        window_edge=WindowEdgeStats(
            attributed_hot=80,
            buckets=[],
            near_boundary_count=2,
            near_boundary_fraction=0.025,
        ),
        ip_clusters=IpClusterStats(
            attributed=80,
            ip_resolved_attributed=34,
            ambiguous_attributed=11,
            ip_resolved_fraction=0.42,
            max_candidate_count=3,
            top_clusters=[
                IpCluster(ip=ip, attributed_conversions=11, max_candidate_count=3)
            ],
        ),
        genre_reach=[],
    )


def _tool_use(name: str, inp: dict, id_: str = "t") -> SimpleNamespace:
    return SimpleNamespace(type="tool_use", name=name, id=id_, input=inp)


def _usage(cache_read: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        cache_read_input_tokens=cache_read, input_tokens=10, output_tokens=5
    )


class _FakeMessages:
    def __init__(self, turns: list) -> None:
        self.turns = list(turns)
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        content, usage = self.turns.pop(0)
        return SimpleNamespace(content=content, usage=usage)


class _FakeAnthropic:
    def __init__(self, turns: list) -> None:
        self.messages = _FakeMessages(turns)


class _FakeCH:
    def query(self, sql, parameters=None):
        return SimpleNamespace(
            result_rows=[("h-1", 11, 3)],
            column_names=[
                "household_id",
                "attributed_conversions",
                "max_candidate_count",
            ],
        )


_VALID = {
    "profile": "shared_ip_spike",
    "top_hypothesis": "device_graph_mismatch",
    "ranked": [
        {
            "hypothesis": "device_graph_mismatch",
            "evidence_for": ["ip_resolved_fraction 0.42 elevated"],
            "evidence_against": [],
            "weight": 0.9,
        }
    ],
    "ruled_out": ["real_performance_change"],
    "recommended_action": "hold ROAS pending review",
    "verdict": "CONFIDENT",
    "probes_run": [],  # model's self-report — the loop overrides with what ran
}


def test_happy_path_probe_then_finding() -> None:
    turns = [
        ([_tool_use("ip_cluster_detail", {"ip": "9.9.9.9"}, "a")], _usage(0)),
        ([_tool_use("submit_finding", _VALID, "b")], _usage(cache_read=1234)),
    ]
    run = loop.run_agent(_FakeCH(), make_ctx(), _FakeAnthropic(turns))
    assert run.finding.verdict == "CONFIDENT"
    assert run.finding.top_hypothesis == "device_graph_mismatch"
    # probes_run reflects what the loop ACTUALLY ran, not the model's empty claim.
    assert run.finding.probes_run == ["ip_cluster_detail"]
    assert run.probes_run == ["ip_cluster_detail"]
    # Turn-2 cache_read is observable (the ≥1-probe contract guarantees a turn 2).
    assert len(run.usages) == 2
    assert run.usages[1].cache_read_input_tokens == 1234


def test_request_carries_cached_prefix_thinking_and_effort() -> None:
    turns = [
        ([_tool_use("ip_cluster_detail", {"ip": "9.9.9.9"}, "a")], _usage()),
        ([_tool_use("submit_finding", _VALID, "b")], _usage()),
    ]
    fake = _FakeAnthropic(turns)
    loop.run_agent(_FakeCH(), make_ctx(), fake)
    call = fake.messages.calls[0]
    assert call["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert call["thinking"] == {"type": "adaptive"}
    assert call["output_config"]["effort"] == loop.AGENT_EFFORT
    tool_names = {t["name"] for t in call["tools"]}
    assert "submit_finding" in tool_names and "ip_cluster_detail" in tool_names


def test_empty_probe_submission_is_rejected_once_then_recovers() -> None:
    turns = [
        ([_tool_use("submit_finding", _VALID, "a")], _usage()),  # no probe yet
        ([_tool_use("ip_cluster_detail", {"ip": "9.9.9.9"}, "b")], _usage()),
        ([_tool_use("submit_finding", _VALID, "c")], _usage()),
    ]
    run = loop.run_agent(_FakeCH(), make_ctx(), _FakeAnthropic(turns))
    assert run.finding.verdict == "CONFIDENT"
    assert run.finding.probes_run == ["ip_cluster_detail"]


def test_two_empty_submissions_escalate() -> None:
    turns = [
        ([_tool_use("submit_finding", _VALID, "a")], _usage()),
        ([_tool_use("submit_finding", _VALID, "b")], _usage()),
    ]
    run = loop.run_agent(_FakeCH(), make_ctx(), _FakeAnthropic(turns))
    assert run.finding.verdict == "AMBIGUOUS_NEEDS_HUMAN"
    assert run.finding.probes_run == []


def test_malformed_finding_escalates_but_keeps_probe_record() -> None:
    bad = {**_VALID, "verdict": "MAYBE"}
    turns = [
        ([_tool_use("ip_cluster_detail", {"ip": "9.9.9.9"}, "a")], _usage()),
        ([_tool_use("submit_finding", bad, "b")], _usage()),
    ]
    run = loop.run_agent(_FakeCH(), make_ctx(), _FakeAnthropic(turns))
    assert run.finding.verdict == "AMBIGUOUS_NEEDS_HUMAN"
    assert run.probes_run == ["ip_cluster_detail"]


def test_bad_probe_params_do_not_crash_or_count_as_a_probe() -> None:
    turns = [
        ([_tool_use("ip_cluster_detail", {"wrong": "x"}, "a")], _usage()),  # bad params
        ([_tool_use("ip_cluster_detail", {"ip": "9.9.9.9"}, "b")], _usage()),
        ([_tool_use("submit_finding", _VALID, "c")], _usage()),
    ]
    run = loop.run_agent(_FakeCH(), make_ctx(), _FakeAnthropic(turns))
    assert run.finding.verdict == "CONFIDENT"
    assert run.finding.probes_run == ["ip_cluster_detail"]  # only the valid call


def test_round_budget_exhaustion_escalates() -> None:
    turns = [([_tool_use("ip_cluster_detail", {"ip": "9.9.9.9"}, "a")], _usage())] * 3
    run = loop.run_agent(_FakeCH(), make_ctx(), _FakeAnthropic(turns), max_rounds=3)
    assert run.finding.verdict == "AMBIGUOUS_NEEDS_HUMAN"
    assert "budget" in run.finding.recommended_action


def test_model_stops_without_a_tool_call_escalates() -> None:
    turns = [([SimpleNamespace(type="text", text="all good")], _usage())]
    run = loop.run_agent(_FakeCH(), make_ctx(), _FakeAnthropic(turns))
    assert run.finding.verdict == "AMBIGUOUS_NEEDS_HUMAN"


# The EXACT submit_finding payload from the live run that exposed FT-1: the model
# collapsed the whole finding into a stringified `ranked` array (DECISIONS Phase 9).
_MALFORMED_PAYLOAD = json.loads(
    (
        Path(__file__).parent / "fixtures" / "malformed_submit_finding_input.json"
    ).read_text()
)


def test_captured_malformed_payload_escalates_via_app_side_net() -> None:
    # strict:true now prevents this shape at the source; this asserts the app-side net
    # (model_validate → escalation) still catches it if strict ever regresses —
    # belt-and-suspenders, and a permanent guard against this exact recurrence.
    assert isinstance(_MALFORMED_PAYLOAD["ranked"], str)  # the malformation
    block = SimpleNamespace(input=_MALFORMED_PAYLOAD)
    finding = loop._finalize(make_ctx(), block, ["ip_cluster_detail"])
    assert finding.verdict == "AMBIGUOUS_NEEDS_HUMAN"
    assert finding.top_hypothesis == "upstream_data_change"  # escalation default
    assert finding.probes_run == ["ip_cluster_detail"]
    assert "validation" in finding.ranked[0].evidence_for[0].lower()


def test_submit_finding_tool_is_strict_and_ref_free() -> None:
    # Guards Fix A: a future edit that drops strict or reintroduces $ref/$defs (which
    # let the malformation through) fails here, offline.
    tool = loop.submit_finding_tool()
    assert tool["strict"] is True
    blob = json.dumps(tool["input_schema"])
    assert "$ref" not in blob and "$defs" not in blob
