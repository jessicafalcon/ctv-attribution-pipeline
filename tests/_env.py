"""The ONE scrub list for tests that run real recipes or the destructive CLI in a
subprocess (tests/test_makefile.py, tests/test_destructive.py). A test that could
rmtree a developer's real lake through an exported variable is the bug category
this repo says must never exist — so both tests drop the same variables, from one
constant, and cannot drift (review gate, round 4).
"""

import os

SCRUBBED = frozenset(
    {
        "LAKE_ROOT",  # a tmp-lake override would make the real recipe act on it
        "PYTEST_CURRENT_TEST",  # … and is honoured only under pytest
        "CONFIRM",
        "MAKEFLAGS",
        "MFLAGS",
        "MAKELEVEL",  # a sub-make (CI: `make test` → `make -n`) prints Entering/Leaving
        "SOURCE",
        "PROFILE",
    }
)


def clean_env(**extra: str) -> dict[str, str]:
    return {**{k: v for k, v in os.environ.items() if k not in SCRUBBED}, **extra}
