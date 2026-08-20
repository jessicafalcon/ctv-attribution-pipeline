# RESULTS.md

Measured outcomes, three ways the pipeline is validated: **attribution accuracy**
against ground truth, the **rollup benchmark**, and the **agent eval**. The accuracy
numbers are deterministic (seed-reproducible, asserted by the integration tests) and
reproduce via `make eval`; the benchmark is captured from a real `make bench` run; the
agent-eval spread is measured over live invocations (non-reproducible by construction,
see below).

## Attribution accuracy — engine output vs the truth side file

Household-grain precision and recall (ARCHITECTURE §4.3), scored in the eval harness
(`accuracy/`) by joining `attributed_conversions` FINAL from ClickHouse against the
truth-link **side file** (`data/truth/<profile>/`) — truth never enters the database
(determinism / truth-isolation, N1). Household grain is deliberate: the engine is
last-touch, so scoring exact `exposure_id` would measure last-touch-vs-causal
coincidence (a model property), not attribution quality; household grain isolates the
real failure mode, wrong-household (shared-IP) attribution.

| profile | path | credited | truth | correct | precision | recall | wrong-hh |
|---|---|---|---|---|---|---|---|
| `tiny` | hot | 52 | 35 | 35 | 0.673 | 1.000 | 0 |
| `medium` | hot | 130 | 92 | 92 | 0.708 | 1.000 | 0 |
| `long_delay` | hot only | 83 | 75 | 44 | — | 0.587 | — |
| `long_delay` | post-reconcile | 112 | 75 | 73 | — | 0.973 | — |

- **Precision below 1.0 on `tiny`/`medium` is last-touch *organic over-credit*, not a
  bug.** 17 organic `tiny` conversions (no truth link) are credited to a
  coincidentally-recent in-window exposure. Wrong-household count is **0** on both
  clean profiles — shared-IP misattribution is a fault-profile story (see the agent
  eval and `shared_ip_spike`), not a property of the clean runs. Recall is 1.000: the
  hot path misses no caused conversion whose exposure is inside the 7-day window.
  Exact-`exposure_id` match on `tiny` is a labeled diagnostic only (3/52 = 0.058),
  never the headline.
- **`long_delay` is the reconciliation story.** Its caused conversions arrive days
  late, so their exposures have aged out of the 7-day hot window — the hot pass misses
  them and recall sits at **0.587** (44/75). The periodic reconciliation pass re-runs
  the same pure attribution leaf at 90 days against `exposures_landed`, recovers **29**
  caused misses to their correct household, and lifts recall to **0.973** (73/75) —
  credited 83 → 112. The 2 residuals are wrong-household shared-IP attributions:
  reconciliation recovered every recoverable caused miss (`caused_missed=0`), but 2
  conversions resolve to the wrong household through a shared IP — an error
  reconciliation cannot fix, which is what caps recall at 0.973. This is the
  recall-buys-at-some-precision-cost trade
  the long tail exists to make, and it is the headline reconciliation number.

## Benchmark — naive full scan vs optimized rollup

The same four-metric advertiser report (ROAS, CPA, CVR, site-visit rate per
campaign) run two ways over the `long_delay` profile after a full pipeline pass
(`make down && make up && make seed PROFILE=long_delay && make run && make bench`):

- **naive** — full `FINAL` scan-and-join of the raw serving tables
  (`queries/report.sql`: `attributed_conversions` ⋈ `exposures_landed`).
- **optimized** — the pre-aggregated `campaign_hourly` rollup (`queries/bench.sql`).

Both return the identical metric rows (the benchmark asserts equality to 6 dp), and
the benchmark reads **both sides at merged steady state** — it runs `OPTIMIZE ...
FINAL` on all three read tables before measuring (see below), so the figures are the
form a scheduled rollup actually serves in production, not a transient just-refreshed
state.

| metric | naive (full scan) | optimized (rollup) | naive/opt |
|---|---|---|---|
| rows read | 835 | 340 | 2.5× |
| bytes read | 25,686 | 21,760 | 1.2× |
| latency (ms, median) | 4.17 | 2.29 | 1.8× |

### Why each change works

- **Rows read (2.5×).** The naive query reads every attributed conversion **and**
  every landed exposure, then merges the ReplacingMergeTree parts (`FINAL`) to drop
  superseded/duplicate rows before the join. The optimized query reads
  `campaign_hourly`, which holds one pre-aggregated row per `(campaign, hour)` — the
  join and the aggregation were already done once, at rollup-refresh time, so the
  read touches far fewer rows.
- **Bytes read (1.2×).** Fewer rows, and narrower ones: the rollup stores only the
  numeric components it sums (spend, counts, revenue), whereas the naive scan must
  read the wide raw rows including the string ids it joins on. The byte ratio is
  smaller than the row ratio because `campaign_hourly` is keyed
  `(campaign_id, hour)` — a low-cardinality profile has few hour buckets, so the
  rollup is not as many-times smaller in bytes as in rows here.
- **Latency (~1.8×).** Fewer rows/bytes and no join at read time. At this profile
  scale latency is noisy (single-digit ms, cache and scheduling jitter dominate),
  so the **rows/bytes read are the reliable signal** — they are deterministic and
  cache-independent (from ClickHouse's `X-ClickHouse-Summary`), and both queries run
  with the query cache off.
- **Why the numbers moved (and why the benchmark now OPTIMIZEs first).** An earlier
  capture reported the naive side at 864 rows / 42 KB and a 1.6× byte win. That was a
  measurement artifact: `campaign_hourly` and `attributed_conversions` are
  versioned-replace ReplacingMergeTrees, and a `FINAL` scan physically **reads every
  un-merged version-part** before collapsing it — so read_rows/read_bytes counted
  transient part-bloat (a full rollup copy per refresh, a superseded row per
  reconciled conversion) that background merges had not yet collapsed. CI, running
  `make bench` right after a test that refreshes the rollup two extra times, measured
  the rollup at 1020 rows and the benchmark actually printed the rollup reading
  **more** rows than the naive scan (0.8×) — the "rollup wins" headline did not
  reproduce in CI's run-state. The benchmark now canonicalizes **both** sides to
  merged steady state (`OPTIMIZE ... FINAL`) before measuring, so read_rows reflects
  logical table size on both sides, the numbers are deterministic on re-run, and a
  magnitude-free **direction assert** (`optimized read_rows < naive`) fails the run
  if the rollup ever stops being the smaller read.

### Honesty boundary

These are small-profile numbers; the rollup already wins, but the win is modest
because the raw tables are small. The structural point is what scales: the naive
scan grows with **every** conversion and exposure, while the rollup grows only with
the number of distinct `(campaign, hour)` buckets. At 50k–500k msgs/sec that gap is
the difference between a full-history scan and a bounded rollup read — see
`SCALING.md`. The numbers here are reported as measured; the profile was not tuned
to inflate the optimized win.

## Observability — alert rules

Four Alertmanager rules cover the deterministic conditions, each proven by
`make test-alerts` — `promtool check rules` + `test rules` from the digest-pinned
Prometheus image against **real captured registries** (`make metrics-capture` dumps
each stage's terminal Prometheus registry from a knobbed run;
`observability/gen_alert_fixtures.py` bakes those numbers into the test fixture, so
the threshold-crossing values come from a real stage run, never hand-authored):

| alert | expr | fires when |
|---|---|---|
| `ConsumerLag` | `resolve_input_backlog > 100` | resolve stage falls behind |
| `WatermarkStall` | `engine_watermark_lag_seconds > 14400` | peak arrival lateness > 4h |
| `MatchRateOutOfBand` | match rate outside band | share of conversions attributed jumps/drops |
| `RestatementMagnitude` | `reconcile_restatement_roas_abs_delta > 1.0` | reconciliation moves a period's ROAS materially |

Two honesty boundaries on what this proves:

- **These four detect *operational* faults** (lag, watermark stall, match-rate move,
  restatement magnitude), **not attribution inflation.** Catching a
  plausible-but-wrong number — a ROAS that looks fine but is inflated by a
  device-graph mismatch — is the **agent's** job (ARCHITECTURE §4), which is the
  exact reason the agent earns its place. A green alert board does not mean the
  numbers are right.
- **The rules discriminate; they do not isolate.** The fixtures prove each rule
  **fires on the anomalous profile (`long_delay`) and stays silent on the clean one
  (`tiny`)** — a discrimination between a knobbed profile and a baseline. They do
  **not** claim single-knob isolation (one knob → one alert): `long_delay` trips more
  than one rule, and that is expected. The proven claim is "the anomalous profile
  trips the alerts, the clean profile does not," nothing stronger.

## Agent eval — fault → diagnosis

Every fault profile plus the no-fault baseline, run 5× (30 live invocations), scored against the pure rubric in `agent/eval/scoring.py` (unit-tested offline — the live sweep only supplies the LLM outputs). The agent is non-reproducible by construction (temperature is unset on the Claude-5 family, DECISIONS Phase 9), so each cell reports a spread over reps, not a single-run claim; the reps measure residual stability.

### Fault → top hypothesis → correct?

| scenario | kind | expected outcome | correct | verdict spread | top-hypothesis spread |
|---|---|---|---|---|---|
| `shared_ip_spike` | fault_recall | CONFIDENT device_graph_mismatch | 5/5 | 5× CONFIDENT | 5× device_graph_mismatch |
| `real_lift` | negative_confirmation | abstain or CONFIDENT real_performance_change (never device_graph_mismatch) | 5/5 | 5× CONFIDENT | 5× real_performance_change |
| `late_burst` | fault_recall | CONFIDENT late_arrival_distortion | 5/5 | 5× CONFIDENT | 5× late_arrival_distortion |
| `co_view_bug` | capability_boundary | abstain (capability boundary) | 5/5 | 5× AMBIGUOUS_NEEDS_HUMAN | 5× co_view_inflation |
| `duplicate_flood` | control | abstain (control) | 5/5 | 5× AMBIGUOUS_NEEDS_HUMAN | 3× upstream_data_change, 2× real_performance_change |
| `no_fault_baseline` | control | abstain (control) | 5/5 | 5× AMBIGUOUS_NEEDS_HUMAN | 5× real_performance_change |

**False-positive rate (controls `duplicate_flood`, `no_fault_baseline`): 0/10 = 0%.** The two abstentions are distinct: `duplicate_flood` and `no_fault_baseline` abstain because nothing is wrong (the FP controls); `co_view_bug` abstains because the fault is undiagnosable from serving data by design (a labeled capability boundary — the co-view adjusted factor is a DECISIONS won't-do), so it is not folded into the FP rate.

### Near-miss pair — a genuine lift vs shared-IP inflation

| profile | ip_resolved_fraction | match_rate | top-hypothesis spread | verdict spread |
|---|---|---|---|---|
| `real_lift` | 0.061 | 1.000 | 5× real_performance_change | 5× CONFIDENT |
| `shared_ip_spike` | 0.420 | 0.992 | 5× device_graph_mismatch | 5× CONFIDENT |

Both profiles raise reported ROAS, but the discriminator is `ip_resolved_fraction` — elevated on `shared_ip_spike` (wrong-household inflation → `device_graph_mismatch`), flat on `real_lift` (a genuine lift → `real_performance_change`). The agent must tell them apart on that named number, not on "ROAS went up".

### Per-profile live context headline (deterministic, LLM-free)

The discriminator each scenario turns on, captured once per profile from ClickHouse (FG2 — BACKLOG 31). These are seed-reproducible, so the row is the cross-profile live pin, not the non-reproducible verdicts above.

| scenario | match_rate | ip_resolved_fraction | ambig | max_cand | max\|Δroas\| | near_edge |
|---|---|---|---|---|---|---|
| `shared_ip_spike` | 0.992 | 0.420 | 25 | 3 | 0.000 | 0.000 |
| `real_lift` | 1.000 | 0.061 | 0 | 1 | 0.000 | 0.000 |
| `late_burst` | 1.000 | 0.065 | 1 | 2 | 26.604 | 0.000 |
| `co_view_bug` | 0.988 | 0.067 | 1 | 3 | 0.000 | 0.000 |
| `duplicate_flood` | 0.984 | 0.074 | 0 | 1 | 0.000 | 0.000 |
| `no_fault_baseline` | 0.977 | 0.039 | 0 | 1 | 0.000 | 0.000 |

### Honesty boundary

These are small-profile results reported as measured. `co_view_bug`'s abstention is a **labeled capability boundary**, not a gap papered over: the co-view *adjusted* factor is a DECISIONS won't-do (BACKLOG 26 — the honest per-genre expected baseline does not exist in serving data, and sourcing it from the producer's multiplier would couple reporting to generation parameters). The agent correctly declines to diagnose from noise. Verdict/hypothesis stability across reps is a measurement, never a gated assertion (the AI edge is carved out of the byte-identical guarantee, CLAUDE.md).
