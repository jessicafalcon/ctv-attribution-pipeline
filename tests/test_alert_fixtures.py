"""The promtool fixtures are generated, and what they assert must not be a COPY.

Phase-18a review gate: the synthetic fixture carried a hand-copied version of
`PartCountHigh`'s summary annotation, and promtool (which compares annotations
exactly) failed the moment the rule's wording was corrected — a second source of
truth in the one place the repo is strictest about provenance. The generator now
reads the summary out of `alerts.yml` and applies promtool's own substitutions.
"""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).parent.parent
_SPEC = importlib.util.spec_from_file_location(
    "gen_alert_fixtures", ROOT / "observability" / "gen_alert_fixtures.py"
)
gen = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gen)

RULES = (ROOT / "observability" / "rules" / "alerts.yml").read_text()


def test_the_synthetic_annotation_is_derived_from_the_rule() -> None:
    summary = gen.rendered_summary("PartCountHigh", table="t-x", value=999)
    assert "t-x has 999 active parts" in summary
    assert "{{" not in summary  # every template placeholder was substituted
    # …and the prose comes from the rule itself, not from this test or the generator
    tail = summary.split("active parts — ", 1)[1].split(".")[0]
    assert tail in " ".join(RULES.split())


def test_the_workload_alerts_are_separated_from_the_threshold_one() -> None:
    # The distinction the phase turned on: a knob-driven rule proves BOTH sides from
    # real captures; a threshold-driven one proves silence from captures and firing
    # synthetically. A new rule has to declare which kind it is.
    assert "PartCountHigh" not in gen.WORKLOAD_ALERTS
    assert set(gen.ALERTS) == set(gen.WORKLOAD_ALERTS) | {"PartCountHigh"}
    for name in gen.ALERTS:
        assert f"- alert: {name}" in RULES


def test_the_synthetic_threshold_follows_the_rule_not_a_tuned_number() -> None:
    # 150 is ClickHouse's parts_to_delay_insert default; the fixture fires one above
    # it. If someone lowers the rule to make a capture fire, this fails.
    assert f"clickhouse_active_parts > {gen.PART_THRESHOLD}" in RULES
