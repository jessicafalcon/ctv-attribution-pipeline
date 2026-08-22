"""BACKLOG 37 — the docs guard's trace check must be partial-rename-proof.
The old substring check (`needle in text`) let `_canonicalize` keep matching after
a rename to `_canonicalize_tables`; `scripts/check_docs.py::token_present` matches
an exact token. Offline, no services."""

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "check_docs", Path(__file__).parent.parent / "scripts" / "check_docs.py"
)
check_docs = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(check_docs)


def test_partial_rename_is_a_failure() -> None:
    renamed = "def _canonicalize_tables(client):\n    ...\n"
    assert "_canonicalize" in renamed  # the substring check would have passed
    assert not check_docs.token_present("_canonicalize", renamed)


def test_exact_token_still_matches() -> None:
    src = "from queries.bench import _canonicalize, _measure\nbench._canonicalize(c)\n"
    assert check_docs.token_present("_canonicalize", src)
    assert check_docs.token_present("_measure", src)
    assert not check_docs.token_present("_measur", src)


def test_every_trace_resolves_today() -> None:
    errors: list[str] = []
    check_docs.check_traces(errors)
    assert errors == []


def test_backticked_link_text_is_still_a_link() -> None:
    # The first cut stripped code spans before scanning and so dropped every
    # link whose text is `code` — the deliberately-broken-anchor negative test
    # passed. Pinned: found by the Phase-19 hand-run negative tests.
    assert check_docs._links("see [`docs/X.md`](docs/X.md#a) now") == ["docs/X.md#a"]
    assert check_docs._links("[plain](docs/X.md)") == ["docs/X.md"]
    assert check_docs._links("a `PartitionSpec(bucket[8](household_id))` spec") == []
    assert check_docs._links("[ext](https://x.y) [same](#anchor)") == []
