"""Offline unit tests for the query-cost-lever harness (queries/measure_levers.py).

Pure functions only — no ClickHouse. The measured before/after numbers are the
LIVE gate (`make cost-levers` against bench_large); here we cover the SQL-block
parsing, the magnitude-free direction predicates, row-equality, and the
deterministic RESULTS renderer.
"""

from queries import measure_levers as ml


def _m(read_rows: int, read_bytes: int, rows: list) -> dict:
    return {"read_rows": read_rows, "read_bytes": read_bytes, "rows": rows}


def test_parse_blocks_covers_every_named_query_and_drops_comments():
    blocks = ml._parse_blocks(ml.SQL_PATH.read_text())
    expected = {
        "setup_projection",
        "add_projection",
        "materialize_projection",
        "add_idx_genre",
        "add_idx_ip",
        "lever1_query",
        "lever2a_final",
        "lever2a_argmax",
        "lever2b_genre",
        "lever2b_ip",
        "lever3_where",
        "lever3_prewhere",
    }
    assert expected <= set(blocks)
    # Every query block is real SQL, not stray comment lines.
    assert blocks["lever1_query"].lower().startswith("select")
    assert blocks["lever3_prewhere"].lower().startswith("select")
    assert "prewhere" in blocks["lever3_prewhere"].lower()
    assert "--" not in blocks["lever1_query"]  # comment lines were dropped


def test_reduces_bytes_is_magnitude_free_and_strict():
    before, after = _m(100, 1000, []), _m(100, 400, [])
    assert ml._reduces_bytes(before, after) is True
    assert ml._reduces_bytes(after, before) is False
    assert ml._reduces_bytes(before, before) is False  # equal is NOT a reduction


def test_pruned_rows_detects_granule_skipping():
    no_idx, pruned = _m(55000, 9, []), _m(8192, 9, [])
    assert ml._pruned_rows(no_idx, pruned) is True
    assert ml._pruned_rows(no_idx, no_idx) is False  # no prune -> negative result holds


def test_rows_equal_rounds_floats_to_six_dp():
    a = _m(1, 1, [(5428, 154945.1800001)])
    b = _m(9, 9, [(5428, 154945.1800002)])
    assert ml._rows_equal(a, b) is True
    c = _m(9, 9, [(5428, 154945.18001)])
    assert ml._rows_equal(a, c) is False


def test_ratio_formats_and_guards_zero():
    assert ml._ratio(1000, 400) == "2.50x"
    assert ml._ratio(400, 400) == "1.00x"
    assert ml._ratio(400, 0) == "n/a"


def _synthetic_out() -> dict:
    return {
        "sizes": {"attributed_conversions": 25168, "exposures_landed": 55000},
        "ip_value": "100.64.0.273",
        "lever1": {
            "before": _m(25168, 427856, [(5428, 154945.18)]),
            "after": _m(16384, 278528, [(5428, 154945.18)]),
        },
        "lever2a": {
            "final": _m(25168, 226512, [(25076, 714022.95)]),
            "argmax": _m(25168, 855712, [(25076, 714022.95)]),
        },
        "lever2b": {
            "genre": {
                "no_idx": _m(55000, 783671, [(13751,)]),
                "with_idx": _m(55000, 783671, [(13751,)]),
            },
            "ip": {
                "no_idx": _m(55000, 1045213, [(157,)]),
                "with_idx": _m(55000, 1045213, [(157,)]),
            },
        },
        "lever3": {
            "before": _m(25168, 8061895, [(5428, 43424, 54413, 21880, 777416)]),
            "after": _m(25168, 6660392, [(5428, 43424, 54413, 21880, 777416)]),
        },
    }


def test_render_is_deterministic_and_carries_the_measured_numbers():
    out = _synthetic_out()
    section = ml.render(out)
    assert ml.render(out) == section  # deterministic — byte-stable on re-run
    assert section.startswith(ml._START)
    assert section.rstrip().endswith(ml._END)
    # winners labelled and their bytes present
    assert "Lever 1 — projection" in section and "WINS" in section
    assert "427,856" in section and "278,528" in section
    assert "Lever 3 — PREWHERE" in section
    assert "8,061,895" in section and "6,660,392" in section
    # negative result labelled and explained
    assert "DOCUMENTED NEGATIVE RESULT" in section
    assert "855,712" in section  # argMax reads MORE
    assert "| 0 |" in section  # 0 granules skipped
    assert "157 of 55,000" in section
    assert "clustering, not selectivity" in section
    # size headline derived from count() (round(n/8192) granules), not literals
    assert "25,168 rows ≈ 3 granules" in section
    assert "55,000 ≈ 7 granules" in section
    assert "ip = '100.64.0.273'" in section  # dynamic ip value, not a hardcoded literal


def test_render_size_headline_tracks_the_sizes_dict():
    out = _synthetic_out()
    out["sizes"] = {"attributed_conversions": 40960, "exposures_landed": 81920}
    section = ml.render(out)
    assert "40,960 rows ≈ 5 granules" in section  # round(40960/8192)=5
    assert "81,920 ≈ 10 granules" in section  # round(81920/8192)=10
