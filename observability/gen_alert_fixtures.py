"""Generate the promtool alert-rule test fixture from REAL captured stage
registries — the crux of the Phase-7 promtool decision (specs/phase-7.md fix #1):
the threshold-crossing numbers come from `make metrics-capture`, never
hand-authored. The .prom inputs live under data/out/<profile>/metrics/
(gitignored); the generated fixture is committed and read by `make test-alerts`.

Regenerate after capturing both profiles on clean single-profile stacks (they
share conversion_id space — DECISIONS Phase 5):

    make down && make lake-reset CONFIRM=yes && make up \
        && make seed PROFILE=tiny && make metrics-capture PROFILE=tiny
    make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up \
        && make seed PROFILE=long_delay && make metrics-capture PROFILE=long_delay
    (a CLEAN lake each time — since Phase 17 the reconcile candidates are the lake's
    current rows, so a capture over a populated lake does not reproduce)
    uv run python observability/gen_alert_fixtures.py
"""

from pathlib import Path

from prometheus_client.parser import text_string_to_metric_families

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "observability/rules/tests/alerts_test.yml"
# The one fixture whose numbers are NOT captured; the file name says so, and so does
# its header. See the PartCountHigh block in alerts.yml and DECISIONS Phase 18a.
OUT_SYNTHETIC = ROOT / "observability/rules/tests/alerts_synthetic_test.yml"

# The five unlabelled series the Phase-7 four alerts read.
SERIES = [
    "resolve_input_backlog",
    "engine_conversions_attributed_total",
    "engine_conversions_processed_total",
    "engine_watermark_lag_seconds",
    "reconcile_restatement_roas_abs_delta",
]

# Labelled series (Phase 18a): one sample per table, so PartCountHigh — which fires
# PER table — is evaluated against every table the capture holds, not an aggregate
# that could hide one.
LABELLED = ["clickhouse_active_parts"]

# The workload alerts: fired by a producer knob, so a real capture can prove both
# sides. PartCountHigh is not one of them — no capture reaches the server's
# parts_to_delay_insert threshold, so the real fixtures prove its SILENCE and the
# synthetic one proves its firing.
WORKLOAD_ALERTS = [
    "ConsumerLag",
    "WatermarkStall",
    "MatchRateOutOfBand",
    "RestatementMagnitude",
]

ALERTS = [*WORKLOAD_ALERTS, "PartCountHigh"]

# ClickHouse's own parts_to_delay_insert default, the threshold in alerts.yml. The
# synthetic fixture sits one part above it — the only hand-chosen number in either
# file, and it is a consequence of the server's default, not a tuning choice.
PART_THRESHOLD = 150

# Capture profile → (metrics dir, alerts expected to fire). long_delay's knobs
# (volume, 300-min late injector, >7d causal delays, long-window recovery) trip
# all four; tiny trips only RestatementMagnitude — since Phase 16 its 5 shared-IP
# conversions are deferred hot and credited by the reconcile pass, which restates
# a campaign ROAS by ~12.9 (> the 1.0 threshold); the other three stay silent on
# tiny, so each of those is proven to fire on a real knobbed run AND stay silent
# on another. The fire/silent expectation is the test's claim; promtool
# independently checks the RULES produce it from the captured numbers (no
# circularity — the generator never evaluates a threshold).
PROFILES = [
    ("long_delay", "data/out/long_delay/metrics", WORKLOAD_ALERTS),
    ("tiny", "data/out/tiny/metrics", ["RestatementMagnitude"]),
]


def read_labelled(metrics_dir: Path) -> dict[str, dict[str, float]]:
    """{series → {table → value}} for the labelled series. Missing is NOT an error
    here in the way it is for SERIES: a capture predating the scraper simply has no
    clickhouse_*.prom, and the caller says so loudly."""
    out: dict[str, dict[str, float]] = {s: {} for s in LABELLED}
    for prom in sorted(metrics_dir.glob("*.prom")):
        for family in text_string_to_metric_families(prom.read_text()):
            for sample in family.samples:
                if sample.name in LABELLED and "table" in sample.labels:
                    out[sample.name][sample.labels["table"]] = sample.value
    return out


def read_series(metrics_dir: Path) -> dict[str, float]:
    """Parse the stage .prom dumps and pull the five series (exact-name match, so
    counter `_created` companions are ignored). Missing any → the capture didn't
    run; fail loudly rather than emit a fixture with stale/absent numbers."""
    vals: dict[str, float] = {}
    for prom in sorted(metrics_dir.glob("*.prom")):
        for family in text_string_to_metric_families(prom.read_text()):
            for sample in family.samples:
                if sample.name in SERIES:
                    vals[sample.name] = sample.value
    missing = [s for s in SERIES if s not in vals]
    if missing:
        raise SystemExit(f"{metrics_dir}: missing {missing} — run make metrics-capture")
    return vals


def _fmt(v: float) -> str:
    """Preserve the captured value exactly; render integers without a `.0`."""
    return repr(int(v)) if v == int(v) else repr(v)


def render() -> str:
    lines = [
        "# GENERATED by observability/gen_alert_fixtures.py from REAL captured",
        "# stage registries (make metrics-capture) — do NOT hand-edit the values.",
        "# Provenance: specs/phase-7.md fix #1. Regenerate after a capture.",
        "rule_files:",
        "  - ../alerts.yml",
        "evaluation_interval: 1m",
        "tests:",
    ]
    for name, mdir, expected in PROFILES:
        vals = read_series(ROOT / mdir)
        claim = f"fires {', '.join(expected)}" if expected else "all silent"
        lines += [
            f"  # {name}: {claim} (real captured values, held across the window)",
            "  - interval: 1m",
            "    input_series:",
        ]
        for s in SERIES:
            # `+0x15` holds the captured value for 16 one-minute steps, so the
            # rules' `for: 5m` is satisfied well before the 10m eval.
            lines += [
                f"      - series: '{s}'",
                f"        values: '{_fmt(vals[s])}+0x15'",
            ]
        labelled = read_labelled(ROOT / mdir)
        for series, by_table in labelled.items():
            if not by_table:
                raise SystemExit(
                    f"{mdir}: no {series} samples — run make metrics-capture with the "
                    "Phase-18a scraper (observability/ch_scrape.py)"
                )
            for table, value in sorted(by_table.items()):
                lines += [
                    f"      - series: '{series}{{table=\"{table}\"}}'",
                    f"        values: '{_fmt(value)}+0x15'",
                ]
        lines.append("    alert_rule_test:")
        for alert in ALERTS:
            lines += ["      - eval_time: 10m", f"        alertname: {alert}"]
            if alert in expected:
                lines += [
                    "        exp_alerts:",
                    "          - exp_labels:",
                    "              severity: warning",
                ]
            else:
                lines.append("        exp_alerts: []")
    return "\n".join(lines) + "\n"


def render_synthetic() -> str:
    """The PartCountHigh FIRING case. Synthetic on purpose and labelled as such: the
    threshold is ClickHouse's parts_to_delay_insert default (150), and no profile this
    repo runs leaves a table anywhere near it — the real captures read 5 parts max, so
    there is no capture that can fire this rule. Inventing a LOWER threshold to make a
    capture fire would be inventing the number; inventing an input to prove the
    server's number fires is honest, provided the file says so. The silence side is
    proven by the real captures in alerts_test.yml."""
    return "\n".join(
        [
            "# GENERATED by observability/gen_alert_fixtures.py — SYNTHETIC INPUT.",
            "# The only fixture in this repo whose numbers are not captured from a",
            "# real run: PartCountHigh's threshold is the SERVER's",
            f"# parts_to_delay_insert default ({PART_THRESHOLD}) and no profile here",
            "# leaves a table near it (real captures: 5 active parts max, see",
            "# alerts_test.yml, which proves the SILENCE side). This file proves only",
            "# that the rule fires when a table does cross the server's own limit.",
            "rule_files:",
            "  - ../alerts.yml",
            "evaluation_interval: 1m",
            "tests:",
            "  - interval: 1m",
            "    input_series:",
            "      - series: "
            "'clickhouse_active_parts{table=\"attributed_conversions\"}'",
            f"        values: '{PART_THRESHOLD + 1}+0x15'",
            "    alert_rule_test:",
            "      - eval_time: 10m",
            "        alertname: PartCountHigh",
            "        exp_alerts:",
            "          - exp_labels:",
            "              severity: warning",
            "              table: attributed_conversions",
            "            exp_annotations:",
            "              summary: >-",
            f"                attributed_conversions has {PART_THRESHOLD + 1} active"
            " parts — ClickHouse delays inserts from 150 (parts_to_delay_insert) and"
            " rejects them at 300 (parts_to_throw_insert). Threshold is the SERVER's"
            " default, not a number measured between our profiles: real captures read"
            " 5 parts max (see alerts_test.yml). Check background merges before"
            " writing more.",
            "",
        ]
    )


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render())
    OUT_SYNTHETIC.write_text(render_synthetic())
    print(f"wrote {OUT.relative_to(ROOT)}")
    print(f"wrote {OUT_SYNTHETIC.relative_to(ROOT)}  (synthetic: PartCountHigh fires)")


if __name__ == "__main__":
    main()
