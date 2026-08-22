"""`make agent-eval` stands up a CLEAN stack per scenario — and since Phase 17 a
clean stack is a clean lake (review gate, round 6: this was the one populate
path without the reset; the chain guard could not see a subprocess arg list).
No make is run: subprocess.run is captured."""

from agent.eval import run_eval


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
