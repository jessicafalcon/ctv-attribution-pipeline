"""Session-wide guards (Phase-17 review gate, round 3).

1. Integration tests run ONLY under the `make test-int*` targets, which export
   CTV_INT=1. A bare `pytest` (the run-tests hook makes it routine) used to seed
   tiny into whatever broker was up and re-stamp `eval_meta=tiny` over a
   long_delay DB — the same hazard class as an unbounded `rm -rf`. Without the
   marker every `tests/integration` test is SKIPPED, loudly.
2. The lake binding is process-global (`lake.iceberg_catalog._profile`); a test
   that binds a profile (a driver test calling `main(["--profile", …])`) must not
   leave it bound, or a later test that forgot its tmp-lake fixture would write to
   the real data/lake/<profile> instead of raising `LakeRootUnset`. Reset per
   test. CONFIRM / MAKEFLAGS are scrubbed so the Makefile guard tests see a clean
   environment.
"""

import os

import pytest

from lake import iceberg_catalog as cat

INTEGRATION_MARKER = "CTV_INT"


def pytest_collection_modifyitems(config, items):
    if os.environ.get(INTEGRATION_MARKER) == "1":
        return
    skip = pytest.mark.skip(
        reason=f"integration tests run under `make test-int*` "
        f"({INTEGRATION_MARKER}=1): they seed the live broker and stamp eval_meta"
    )
    for item in items:
        if "tests/integration/" in str(item.fspath).replace(os.sep, "/"):
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def _unbind_lake_and_scrub_env(monkeypatch):
    monkeypatch.setattr(cat, "_profile", None)
    for var in ("CONFIRM", "MAKEFLAGS", "DAGSTER_PROFILE"):
        monkeypatch.delenv(var, raising=False)
