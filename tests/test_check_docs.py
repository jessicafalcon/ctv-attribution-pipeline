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


def test_link_outside_the_repo_is_rejected() -> None:
    root = check_docs.ROOT
    assert not check_docs._inside_root(
        "../../../outside.md", (root / "docs" / "../../../outside.md").resolve()
    )
    assert not check_docs._inside_root("/etc/passwd", (root / "/etc/passwd").resolve())
    assert check_docs._inside_root(
        "../README.md", (root / "docs" / "../README.md").resolve()
    )


def test_heading_inside_a_fence_is_not_an_anchor(tmp_path) -> None:
    md = tmp_path / "x.md"
    md.write_text("## Real\n\n```bash\n## not a heading\n# comment\n```\n")
    assert check_docs._anchors(md) == {"real"}


def test_historical_mentions_are_skipped_by_the_target_scan() -> None:
    text = (
        "| ~~`make gone-target`~~ DONE |\n"
        "row `make also-gone` <!-- historical -->\n"
        "live `make still-here`\n"
    )
    living = check_docs._living(text)
    assert "gone-target" not in living
    assert "also-gone" not in living
    assert "still-here" in living


def test_derived_block_regexes_anchor_on_labels_not_cells() -> None:
    for _label, name, block_re, _readme_re in check_docs._DERIVED:
        assert not any(
            ch.isdigit()
            for ch in block_re.replace("_2a", "")
            .replace("Lever 1", "")
            .replace("Lever 3", "")
        ), name
