"""`make agent-eval` stands up a CLEAN stack per scenario — and since Phase 17 a
clean stack is a clean lake (review gate, round 6: this was the one populate
path without the reset; the chain guard could not see a subprocess arg list).
No make is run: subprocess.run is captured."""

import pytest

from agent.eval import run_eval
from agent.eval import scenarios as scen
from agent.eval.scoring import Outcome, RepResult, ScenarioResult, SweepResult


def test_stand_up_profile_resets_the_lake_between_down_and_up(monkeypatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        run_eval.subprocess, "run", lambda cmd, check: calls.append(cmd)
    )
    run_eval.stand_up_profile("late_burst")
    assert calls == [
        ["make", "down"],
        ["make", "lake-reset", "CONFIRM=yes", "PROFILE=late_burst"],
        ["make", "up"],
        ["make", "seed", "PROFILE=late_burst"],
        ["make", "run", "PROFILE=late_burst"],
    ]


# --- write_results: splice between sentinels, fail loud when absent ----------

# A RESULTS-like fixture with a `## ` section AFTER the agent-eval block — the exact
# case the old split-on-marker-to-EOF writer silently dropped (findings 3, 4).
_HEAD = "# Results\n\nsome earlier prose.\n\n"
_TRAILING = "\n\n## A later section\n\nthis must survive the splice.\n"
_STUB_SECTION = (
    f"{run_eval._START}\n\n## Agent eval\n\nfresh block body.\n\n{run_eval._END}"
)


def _results_fixture() -> str:
    old_block = f"{run_eval._START}\n\n## Agent eval\n\nstale body.\n\n{run_eval._END}"
    return _HEAD + old_block + _TRAILING


def test_write_results_splices_between_sentinels_keeping_head_and_tail(
    monkeypatch, tmp_path
) -> None:
    results = tmp_path / "RESULTS.md"
    results.write_text(_results_fixture())
    monkeypatch.setattr(run_eval, "RESULTS_PATH", results)

    run_eval.write_results(_STUB_SECTION)
    out = results.read_text()

    # Exact prefix/suffix survival — not substring: the head and the trailing `##`
    # section are byte-preserved around the spliced block.
    assert out.startswith(_HEAD)
    assert out.endswith(_TRAILING)
    assert _STUB_SECTION in out
    assert "stale body." not in out  # the old block is replaced, not appended past


def test_write_results_is_byte_idempotent(monkeypatch, tmp_path) -> None:
    results = tmp_path / "RESULTS.md"
    results.write_text(_results_fixture())
    monkeypatch.setattr(run_eval, "RESULTS_PATH", results)

    run_eval.write_results(_STUB_SECTION)
    first = results.read_text()
    run_eval.write_results(_STUB_SECTION)
    assert results.read_text() == first


def test_write_results_fails_loud_when_markers_absent(monkeypatch, tmp_path) -> None:
    results = tmp_path / "RESULTS.md"
    results.write_text("# Results\n\nno sentinels here.\n")
    monkeypatch.setattr(run_eval, "RESULTS_PATH", results)

    with pytest.raises(SystemExit):
        run_eval.write_results(_STUB_SECTION)


@pytest.mark.parametrize(
    "text",
    [
        f"# Results\n\n{run_eval._START}\n\nonly the start marker.\n",
        f"# Results\n\nonly the end marker.\n\n{run_eval._END}\n",
    ],
)
def test_write_results_fails_loud_when_one_marker_present(
    monkeypatch, tmp_path, text: str
) -> None:
    # A half-sentinel file must raise the actionable SystemExit ("seed the block
    # skeleton"), never a raw ValueError from text.index — the guard is `or`, not
    # `and` (mutation survivor closed).
    results = tmp_path / "RESULTS.md"
    results.write_text(text)
    monkeypatch.setattr(run_eval, "RESULTS_PATH", results)

    with pytest.raises(SystemExit):
        run_eval.write_results(_STUB_SECTION)


# --- render_section: pure, sentinel-wrapped, deterministic ------------------


def _minimal_sweep() -> SweepResult:
    """A valid SweepResult over the real scenario catalog (near_miss needs both
    real_lift and shared_ip_spike present) — synthetic reps, no live sweep."""
    scenarios = [
        ScenarioResult(
            scenario=s,
            reps=[
                RepResult(
                    scenario=s.name,
                    outcome=Outcome.CORRECT_ABSTENTION,
                    verdict="AMBIGUOUS_NEEDS_HUMAN",
                    top_hypothesis="x",
                    probes_run=(),
                )
            ],
            headline={"match_rate": 1.0, "ip_resolved_fraction": 0.1},
        )
        for s in scen.SCENARIOS
    ]
    return SweepResult(scenarios=scenarios)


def test_render_section_wraps_in_sentinels_and_is_pure() -> None:
    sweep = _minimal_sweep()
    section = run_eval.render_section(sweep, capture_date="2026-08-23")
    assert section.startswith(run_eval._START)
    assert section.endswith(run_eval._END)
    assert "2026-08-23" in section
    # Same inputs + same date → byte-identical (no clock read inside render_section).
    assert run_eval.render_section(sweep, capture_date="2026-08-23") == section
