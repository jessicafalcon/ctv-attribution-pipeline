"""Offline unit tests for the rollup-bench read-side reporting (rollup_bench.py).

Pure functions only — no ClickHouse. The measured numbers are the LIVE gate
(`make rollup-bench PROFILE=bench_large` against a populated stack); here we pin the
granule classification and the granule-derived read verdict/mechanism that produce the
committed RESULTS block, so a flip like `marks > 2` → `marks > 1` (which would emit the
wrong prose into docs/RESULTS.md on the next run) fails offline instead of silently.
"""

from queries import rollup_bench as rb


def _cost(read_rows: int, written_rows: int) -> dict:
    return {"read_rows": read_rows, "written_rows": written_rows}


def _m(
    *,
    profile: str,
    total_keys: int,
    changed: int,
    dirty: int,
    granules: list,
    full: dict,
    incremental: dict,
) -> dict:
    return {
        "profile": profile,
        "total_keys": total_keys,
        "changed": changed,
        "dirty": dirty,
        "over_refresh": dirty - changed,
        "equal_sets": dirty == changed,
        "granules": granules,
        "full": full,
        "incremental": incremental,
    }


def _bench_large() -> dict:
    """The committed measurement: multi-granule, reads unchanged (a documented neg)."""
    return _m(
        profile="bench_large",
        total_keys=165,
        changed=156,
        dirty=156,
        granules=[
            ("attributed_conversions", 25168, 5),
            ("exposures_landed", 55000, 8),
        ],
        full=_cost(135168, 165),
        incremental=_cost(135168, 156),
    )


def _long_delay() -> dict:
    """The lighter single-granule run: 2 marks per table, reads unchanged."""
    return _m(
        profile="long_delay",
        total_keys=340,
        changed=19,
        dirty=19,
        granules=[
            ("attributed_conversions", 115, 2),
            ("exposures_landed", 360, 2),
        ],
        full=_cost(835, 340),
        incremental=_cost(835, 19),
    )


def test_multi_granule_classifies_by_mark_count():
    # A merged single 8192-row granule reads back as 2 marks (one granule + boundary),
    # so > 2 marks == more than one granule.
    assert rb._multi_granule(_bench_large()) is True  # 8 and 5 marks
    assert rb._multi_granule(_long_delay()) is False  # 2 and 2 marks
    # Boundary: exactly 2 marks is still one granule; 3 crosses.
    assert rb._multi_granule({"granules": [("t", 8192, 2)]}) is False
    assert rb._multi_granule({"granules": [("t", 8193, 3)]}) is True
    # Any table over one granule flips it, even if another sits inside one.
    assert rb._multi_granule({"granules": [("a", 1, 2), ("b", 99999, 8)]}) is True


def test_read_finding_multi_granule_negative():
    verdict, mechanism = rb._read_finding(_bench_large())
    assert verdict == "unchanged (multi-granule)"
    # The mechanism names the real cause: the dirty set spans every granule.
    assert "156 of 165" in mechanism
    assert "every granule" in mechanism
    assert "ABSENT from" in mechanism  # pruning would need the opposite


def test_read_finding_single_granule():
    verdict, mechanism = rb._read_finding(_long_delay())
    assert verdict == "unchanged (single granule)"
    assert "ONE 8,192-row granule" in mechanism


def test_read_finding_read_win_when_incremental_reads_fewer():
    win = _bench_large()
    win["incremental"] = _cost(90000, 156)  # fewer than full's 135168
    verdict, mechanism = rb._read_finding(win)
    assert verdict == "read win"
    assert "90,000" in mechanism and "135,168" in mechanism
    assert "prunes granules" in mechanism


def test_render_is_deterministic_and_carries_the_measured_numbers():
    m = _bench_large()
    section = rb.render(m)
    assert rb.render(m) == section  # deterministic — byte-stable on re-run
    assert section.startswith(rb._START)
    assert section.rstrip().endswith(rb._END)
    # Provenance is profile-derived, not a hardcoded long_delay literal.
    assert "make rollup-bench PROFILE=bench_large" in section
    assert "PROFILE=long_delay" not in section
    # The two measured numbers and the granule evidence.
    assert "165" in section and "156" in section
    assert "135,168" in section
    assert "55,000 rows in 8 marks" in section
    assert "25,168 rows in 5 marks" in section
    # The read-side verdict and its mechanism, granule-derived.
    assert "unchanged (multi-granule)" in section
    assert (
        "the dirty keys span every granule" not in section
    )  # that phrasing is README's
    assert "at least one dirty key falls in every granule" in section
    # No read assert claimed; write direction is the asserted one.
    assert "unchanged — printed, **not** asserted" in section
    assert "**asserted** (direction only)" in section


def test_render_read_cell_flips_on_a_read_win():
    win = _bench_large()
    win["incremental"] = _cost(90000, 156)
    section = rb.render(win)
    assert "fewer — printed, **not** asserted" in section
    assert "read win" in section


def test_render_single_granule_profile_reads_honestly():
    section = rb.render(_long_delay())
    assert "make rollup-bench PROFILE=long_delay" in section
    assert "unchanged (single granule)" in section
    assert "ONE 8,192-row granule" in section


def test_format_report_verdict_matches_render():
    m = _bench_large()
    report = rb.format_report(m)
    assert rb.format_report(m) == report  # deterministic
    verdict, mechanism = rb._read_finding(m)
    assert verdict in report
    assert mechanism in report
