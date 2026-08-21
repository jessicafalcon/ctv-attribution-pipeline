"""Makefile guards (offline: `make -n` only, nothing runs).

Two bugs that lived on main since Phase 2 / Phase 5 and surfaced in the Phase-16
review: (1) `SOURCE ?= fixtures  # comment` carried the whitespace before the
inline comment into the value, so a bare `make resolve` passed
`--source "fixtures  "` and exited 2; (2) `test-int-medium` ran `run-hot` without
`PROFILE=medium`, stamping `eval_meta` as tiny over a medium database — the
BACKLOG-43 profile guard then compared tiny to tiny and let `make eval` print
nonsense instead of failing loudly."""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent


def _dry_run(target: str) -> str:
    return subprocess.run(
        ["make", "-n", target],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def test_make_resolve_default_source_has_no_trailing_whitespace() -> None:
    out = _dry_run("resolve")
    assert '--source "fixtures"' in out
    assert '"fixtures ' not in out


def test_every_isolated_live_target_stamps_its_own_profile() -> None:
    # Each clean-stack target seeds profile P and must populate with PROFILE=P, so
    # the eval_meta marker the populate path writes names the data actually loaded.
    for target, profile in [
        ("test-int-medium", "medium"),
        ("test-int-long-delay", "long_delay"),
        ("test-int-shared-ip", "shared_ip_spike"),
        ("test-int-agent", "shared_ip_spike"),
        ("test-int-lakehouse", "long_delay"),
    ]:
        out = _dry_run(target)
        populate = [
            line
            for line in out.splitlines()
            if line.startswith(("make run", "make run-hot", "make lake-land"))
            or " run PROFILE" in line
            or " run-hot PROFILE" in line
            or " lake-land PROFILE" in line
        ]
        assert populate, f"{target}: no populate step found in dry run"
        for line in populate:
            assert f"PROFILE={profile}" in line, f"{target}: {line!r}"
