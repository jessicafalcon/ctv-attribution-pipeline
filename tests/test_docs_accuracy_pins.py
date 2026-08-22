"""BACKLOG 36 — the docs accuracy tables must not silently drift from the pinned
integration numbers. Parses the household-grain accuracy table in README.md and
docs/RESULTS.md and asserts every cell equals the single source of truth in
`tests/pins.py` (which the live integration suites also assert against). A pin
change moves this guard in lockstep; a docs typo or a README/RESULTS drift fails
here. Scope: the accuracy TABLES only — numbers restated in doc prose are not
covered (see tests/pins.py). Pure/offline — no services.
"""

from pathlib import Path

from tests.pins import (
    LONG_DELAY_HOT,
    LONG_DELAY_POST,
    MEDIUM_HOT,
    TINY_HOT,
    TODECIMAL_TRUNCATED_CENT_VALUES,
)

REPO_ROOT = Path(__file__).parent.parent
README = REPO_ROOT / "README.md"
RESULTS = REPO_ROOT / "docs" / "RESULTS.md"

# (README "profile (path)" label, RESULTS profile, RESULTS path, pin, precision shown)
ROWS = [
    ("`tiny` (hot)", "tiny", "hot", TINY_HOT, True),
    ("`medium` (hot)", "medium", "hot", MEDIUM_HOT, True),
    ("`long_delay` (hot only)", "long_delay", "hot only", LONG_DELAY_HOT, False),
    (
        "`long_delay` (post-reconcile)",
        "long_delay",
        "post-reconcile",
        LONG_DELAY_POST,
        False,
    ),
]

_DASHES = {"—", "–"}  # em / en dash only; long_delay precision is not reported


def _table_rows(path: Path) -> list[list[str]]:
    """Every markdown table row in the file as a list of stripped cells."""
    out = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("|"):
            out.append([c.strip() for c in stripped.strip("|").split("|")])
    return out


def _counts_ok(cells: tuple[str, str, str], pin, where: str) -> None:
    want = (str(pin.credited), str(pin.truth), str(pin.correct))
    assert cells == want, f"{where}: counts {cells} != {want}"


def _precision_ok(cell: str, pin, shown: bool, where: str) -> None:
    if shown:
        assert cell == f"{pin.precision:.3f}", f"{where}: precision {cell!r}"
    else:
        assert cell in _DASHES, f"{where}: precision not a dash: {cell!r}"


def _recall_ok(cell: str, pin, where: str) -> None:
    assert cell == f"{pin.recall:.3f}", f"{where}: recall {cell!r}"


def test_readme_accuracy_table_matches_pins() -> None:
    # README cols: profile | credited | truth | correct | precision | recall | what
    rows = _table_rows(README)
    for label, _profile, _path, pin, shown in ROWS:
        found = [r for r in rows if r and r[0] == label]
        assert len(found) == 1, f"README: want one row {label!r}, got {len(found)}"
        c = found[0]
        _counts_ok((c[1], c[2], c[3]), pin, f"README {label}")
        _precision_ok(c[4], pin, shown, f"README {label}")
        _recall_ok(c[5], pin, f"README {label}")


def test_results_accuracy_table_matches_pins() -> None:
    # RESULTS cols: profile|path|credited|truth|correct|precision|recall|wrong-hh
    rows = _table_rows(RESULTS)
    for _label, profile, path, pin, shown in ROWS:
        found = [
            r
            for r in rows
            if len(r) >= 7 and r[0].strip("`") == profile and r[1] == path
        ]
        assert len(found) == 1, (
            f"RESULTS: want one ({profile}, {path}), got {len(found)}"
        )
        c = found[0]
        _counts_ok((c[2], c[3], c[4]), pin, f"RESULTS {profile}/{path}")
        _precision_ok(c[5], pin, shown, f"RESULTS {profile}/{path}")
        _recall_ok(c[6], pin, f"RESULTS {profile}/{path}")


def test_architecture_cites_the_pinned_todecimal_truncation_count() -> None:
    text = (Path(__file__).parent.parent / "docs" / "ARCHITECTURE.md").read_text()
    assert f"{TODECIMAL_TRUNCATED_CENT_VALUES:,} of the 100,000" in text
