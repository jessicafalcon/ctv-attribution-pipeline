# Phase 7 — Benchmark and observability (CHECKPOINT)

Contract for the `phase-7-benchmark-observability` branch. Source: `docs/PHASES.md`
→ Phase 7, `docs/ARCHITECTURE.md` §3.3 "Reporting" / "Observability", and the
Phase-5/6 BACKLOG rows due here (join-state current gauge; co-view re-defer).

Two deliverables, both gated: a naive-vs-optimized benchmark, and four alert rules
each triggerable by a producer knob. Plus Grafana dashboards as JSON.

## The central constraint

The pipeline stages (resolve/engine/reconcile) are short-lived **host batch
processes** — `make run` runs each once and it exits in seconds. Their Prometheus
metrics never survive a 15s scrape, and nothing scrapes them live. So "an alert
fires live in Prometheus" needs continuous-follow infra we are deliberately
deferring. Alerts are therefore proven by `promtool test rules`, not a live scrape
— but rigorously (fix #1 below).

## DONE command

```
make down && make up && make seed PROFILE=long_delay && make run && \
make bench && make test-alerts && make test && make lint
```

- `make bench` prints the naive-vs-optimized table (latency, rows read, bytes read)
  and asserts both queries return the same metric rows (Gate 1).
- `make test-alerts` runs `promtool check rules` + `test rules` from the pinned
  prometheus image; all four alerts fire on long_delay's captured values and stay
  silent on tiny's (Gate 2).
- `make test` (offline, incl. the new metric unit tests) + `make lint` green;
  gate-0 tiny golden byte-identical (metrics are side-effect-free observers).

## Done-when

1. **Benchmark.** `make bench` reports naive (full `FINAL` scan-and-join of
   `attributed_conversions` ⋈ `exposures_landed`, `queries/report.sql`) vs optimized
   (`campaign_hourly` rollup, `queries/bench.sql`): latency, rows read, bytes read,
   with a written explanation in `docs/RESULTS.md`. The two return identical metric
   rows (asserted, rounded — fix #2).
2. **Alerts triggerable by a producer knob.** Four rules — `ConsumerLag`,
   `WatermarkStall`, `MatchRateOutOfBand`, `RestatementMagnitude` — each proven to
   fire on a real captured metric value that a named knob produces, and silent on a
   profile that doesn't produce it.
3. **Grafana dashboards as JSON**, provisioned.
4. Gate 0, `make test`, `make lint` green.

## The four folded fixes (PINNED — do not re-litigate)

**Fix #1 — fixture provenance (the crux the promtool decision rests on).** The
promtool `input_series` values MUST come from a **real live stage run**, never
hand-authored. Each stage's `--metrics-out PATH` calls
`prometheus_client.write_to_textfile` on **its own registry after its real run**;
`make metrics-capture PROFILE=<p>` orchestrates the three stages into
`data/out/<p>/metrics/*.prom`; `observability/gen_alert_fixtures.py` parses those
and writes `observability/rules/tests/alerts_test.yml`. **Do NOT recompute metrics
through the offline oracle/replay** — that makes the fixture reflect the
generator's arithmetic, not the stage's (circular). `resolve_input_backlog` (needs
a real consumer) and `reconcile_restatement_roas_abs_delta` (needs ClickHouse
FINAL) are inherently live-stack, like `test-int-long-delay`.

**Fix #2 — bench equality.** `bench.sql` sums the rollup components per campaign
**then** divides (never sums hourly ratios). Verified: `campaign_hourly` is
refreshed from the SAME definitions as `report.sql` (both `FINAL`, both paths,
`attributed = 1`, exposure-id join — `reconcile/rollup.py`). Assert equality
**rounded to 6 dp**, not raw floats (sum-of-hourly-sums vs single sum differ at ulp
scale). Query cache off on both.

**Fix #3 — WatermarkStall wording.** Keep the alert name (PHASES.md's word), but
`engine_watermark_lag_seconds` measures **peak event→ingest lateness**, which is
NOT a true watermark-advance stall (a batch drain has no advancing watermark to
stall). Recorded in DECISIONS as a batch proxy; true stall detection is a
continuous-mode signal, deferred. (STOP-on-spec-mismatch surfaced, not worked
around.)

**Fix #4 — pinned promtool image.** `make test-alerts` runs promtool from the
**digest-pinned** prometheus image already in compose
(`prom/prometheus:v3.1.0@sha256:6559acbd…`), never a floating tag (CLAUDE.md
digest-pin rule; a floating tag is a determinism hole and a security-reviewer flag).

## New metrics (one at a time, each with a unit test)

| Metric | Type | Meaning | Backs alert |
|---|---|---|---|
| `resolve_input_backlog` | Gauge | messages cleared at drain start (batch consumer-lag proxy) | ConsumerLag |
| `engine_watermark_lag_seconds` | Gauge | peak `ingest−event` lateness (fix #3) | WatermarkStall |
| `engine_join_state_current` | Gauge | current post-eviction occupancy (rises AND falls) | (dashboard; closes BACKLOG 25) |
| `reconcile_restatement_roas_abs_delta` | Gauge | max \|Δroas\| across campaigns pre/post reconcile | RestatementMagnitude |

Match rate needs no new metric: `engine_conversions_attributed_total /
engine_conversions_processed_total`.

## Alert set + knob mapping (thresholds between real captured runs)

Captured tiny (non-firing) vs long_delay (fires all four):

| Alert | Expr | tiny → long_delay | Producer knob |
|---|---|---|---|
| `ConsumerLag` | `resolve_input_backlog > 100` | 62 → 129 | volume |
| `WatermarkStall` | `engine_watermark_lag_seconds > 14400` | 10367 → 17931 | late injector max |
| `MatchRateOutOfBand` | ratio outside [0.80, 0.98] | 0.945 → 0.722 | hot-misses (>7d causal delay) |
| `RestatementMagnitude` | `reconcile_restatement_roas_abs_delta > 1.0` | 0.0 → 27.03 | long-window recovery |

Alerts carry `severity: warning`, `for: 5m`; **no annotations yet** — they only
surface in an Alertmanager notification, and live firing is the deferred push path
(BACKLOG). The fixture holds each value for 16 one-minute steps and evaluates at
10m, asserting fire (long_delay) / silent (tiny).

## Scope (files)

- Metrics: `resolve/metrics.py`, `streaming/metrics.py` (+ engine-side peak
  lateness in `streaming/dataflow.py`, pure core untouched — BACKLOG 24),
  `reconcile/metrics.py` + `reconcile/reconcile.py` (`_restatement_abs_delta`).
- `--metrics-out` on the three stage `main()`s; `Makefile` `metrics-capture`.
- `observability/rules/alerts.yml`, `observability/gen_alert_fixtures.py`,
  `observability/rules/tests/alerts_test.yml` (generated), `Makefile` `test-alerts`
  (+ `PROM_IMAGE`), `observability/prometheus.yml` + compose rules mount.
- `queries/bench.sql`, `queries/bench.py`, `Makefile` `bench`, `docs/RESULTS.md`,
  `docs/SCALING.md` note.
- `observability/grafana/dashboards/attribution.json` +
  `provisioning/dashboards/dashboards.yml`, datasource `uid`, compose dashboards
  mount.
- Unit tests: backlog, watermark-lag + join-state-current, restatement gauge.
- Records: this spec, `DECISIONS.md`, `BACKLOG.md` (25 done, 26 re-anchored, new
  live-firing row), `CLAUDE.md` status + commands.

## Review & stack risk

- **code-reviewer + functionality-tester** at the finish line (mandatory).
  **security-reviewer TRIGGERED** — changes touch prometheus/grafana/compose
  config (service config + volume mounts). No `.env`/credential/CI-workflow change,
  but compose-exposure surface changed, so run it.
- **coherence-auditor** at phase exit (mandatory) + BACKLOG review (25 due, 26 due).

## Out of scope (deferred, recorded)

- Live Alertmanager firing (Pushgateway / node_exporter textfile) → new BACKLOG row
  (Phase 9/10 webhook decision).
- Co-view read-time factor → BACKLOG 26 re-anchored to the Phase-10 near-miss demo
  with a hard stop. NOT implemented here.
- Continuous Kafka follow → unchanged existing deferrals.
- Live dashboard screenshots → need the deferred push path; Phase 7 commits correct
  dashboard JSON, not a live render.
