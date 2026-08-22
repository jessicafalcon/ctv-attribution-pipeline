"""The subprocess tests share ONE scrub list (tests/_env.py) — pinned, so a
helper cannot grow its own set again (review gate, round 5)."""

import inspect

import tests._env as env
import tests.test_destructive as td
import tests.test_makefile as tm


def test_both_subprocess_helpers_use_the_shared_scrub_list() -> None:
    assert td.clean_env is env.clean_env and tm.clean_env is env.clean_env
    for mod in (td, tm):
        src = inspect.getsource(mod)
        assert "os.environ" not in src, f"{mod.__name__} builds its own environment"
    assert {"LAKE_ROOT", "PYTEST_CURRENT_TEST", "CONFIRM", "MAKEFLAGS"} <= env.SCRUBBED
