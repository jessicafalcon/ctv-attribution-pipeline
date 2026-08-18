"""Offline unit tests for the pure accuracy scorer — synthetic cases for each
outcome, plus a fixture-pinned case that reproduces the headline numbers
(household-grain precision 0.673, recall 1.000) with no services."""

import json
from pathlib import Path

import pytest

from accuracy.score import format_report, score

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"


def test_score_synthetic_outcomes() -> None:
    exposure_household = {"e1": "hA", "e2": "hB", "e3": "hA"}
    # c1 caused by e1 (hA); engine credited (hA, e2) → correct household, exact miss
    # c2 caused by e2 (hB); engine credited (hA, e3) → WRONG household
    # c3 organic (no truth); engine credited (hA, e1) → organic false positive
    # c4 caused by e1 (hA); not credited          → caused missed
    credited = {"c1": ("hA", "e2"), "c2": ("hA", "e3"), "c3": ("hA", "e1")}
    truth = {"c1": "e1", "c2": "e2", "c4": "e1"}

    r = score(credited, truth, exposure_household, profile="synthetic")

    assert r.credited == 3
    assert r.truth_links == 3
    assert r.household_correct == 1
    assert r.precision == pytest.approx(1 / 3)
    assert r.recall == pytest.approx(1 / 3)
    assert r.caused_wrong_household == 1
    assert r.organic_credited == 1
    assert r.caused_missed == 1
    assert r.exact_exposure_correct == 0
    assert r.exact_exposure_match_rate == pytest.approx(0.0)


def test_score_empty_is_zero_not_crash() -> None:
    r = score({}, {}, {}, profile="empty")
    assert r.precision == 0.0
    assert r.recall == 0.0
    assert r.f1 == 0.0


def _fixture_inputs() -> tuple[dict, dict, dict]:
    exposures = [
        json.loads(line)
        for line in (FIXTURES / "exposures.jsonl").read_text().splitlines()
    ]
    exposure_household = {e["exposure_id"]: e["household_id"] for e in exposures}
    attributed = [
        json.loads(line)
        for line in (FIXTURES / "expected" / "attributed.jsonl")
        .read_text()
        .splitlines()
    ]
    credited = {
        a["conversion_id"]: (a["household_id"], a["exposure_id"])
        for a in attributed
        if a["attributed"]
    }
    truth = {
        json.loads(line)["conversion_id"]: json.loads(line)["truth_exposure_id"]
        for line in (FIXTURES / "truth_links.jsonl").read_text().splitlines()
    }
    return credited, truth, exposure_household


def test_score_pins_tiny_fixture_numbers() -> None:
    credited, truth, exposure_household = _fixture_inputs()
    r = score(credited, truth, exposure_household, profile="tiny")

    assert r.credited == 52
    assert r.truth_links == 35
    assert r.household_correct == 35
    assert r.precision == pytest.approx(35 / 52)  # 0.6731
    assert r.recall == pytest.approx(1.0)
    assert r.caused_missed == 0
    assert r.caused_wrong_household == 0
    # organic over-credit is the sole driver of tiny's sub-1.0 precision, NOT
    # shared-IP misattribution (DECISIONS Phase 4)
    assert r.organic_credited == 17
    # exact exposure-id is a diagnostic only; expected low for a last-touch engine
    assert r.exact_exposure_correct == 3
    assert r.exact_exposure_match_rate == pytest.approx(3 / 52)


def test_format_report_is_printable() -> None:
    credited, truth, exposure_household = _fixture_inputs()
    text = format_report(score(credited, truth, exposure_household))
    assert "household grain" in text
    assert "diagnostic" in text
