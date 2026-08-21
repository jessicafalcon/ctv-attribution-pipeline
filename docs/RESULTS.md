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
| `tiny` | hot | 47 | 35 | 32 | 0.681 | 0.914 | 0 |
| `medium` | hot | 129 | 92 | 91 | 0.705 | 0.989 | 0 |
| `long_delay` | hot only | 80 | 75 | 44 | — | 0.587 | — |
| `long_delay` | post-reconcile | 112 | 75 | 73 | — | 0.973 | — |

- **Precision below 1.0 on `tiny`/`medium` is last-touch *organic over-credit*, not a
  bug.** 15 organic `tiny` conversions (no truth link) are credited to a
  coincidentally-recent in-window exposure. Wrong-household count is **0** on both
  clean profiles — and since Phase 16 it is 0 on the hot path **by construction**:
  a shared-IP (ambiguous) conversion is never credited hot. Exact-`exposure_id` match
  on `tiny` is a labeled diagnostic only (3/47 = 0.064), never the headline.
- **Hot recall below 1.0 on `tiny`/`medium` is the Phase-16 deferral, not a miss.**
  tiny's 3 and medium's 1 caused shared-IP conversions are emitted unattributed
  (reason ambiguous_ip) and credited by the reconciliation pass, which re-enumerates
  the candidate households from the device graph and applies the most-recent-exposure
  rule there. Post-reconcile, tiny is 52/35/35 and medium 130/92/92 — exactly the
  pre-Phase-16 hot numbers. Same answer after reconciliation; fewer moving parts.
- **`long_delay` is the reconciliation story.** Its caused conversions arrive days
  late, so their exposures have aged out of the 7-day hot window — the hot pass misses
  them and recall sits at **0.587** (44/75). The periodic reconciliation pass re-runs
  the same pure attribution leaf at 90 days against `exposures_landed`, recovers **29**
  caused state-misses to their correct household and settles the 3 deferred shared-IP
  conversions, and lifts recall to **0.973** (73/75) — credited 80 → 112. The 2
  residuals are wrong-household shared-IP picks: reconciliation recovered every
  recoverable caused miss (`caused_missed=0`), but 2 conversions' most-recent exposure
  sits in the wrong shared-IP household — the measured fault, which caps recall at
  0.973 (unchanged by Phase 16: the same rule, applied later). This is the
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

## Query cost levers

Three ClickHouse-native cost levers, each a before/after on a scoped report query
over the `bench_large` serving tables (a multi-granule profile — `attributed_conversions`
~25k rows, `exposures_landed` 55k — so pruning has something to skip; below one 8192-row
granule every lever is a no-op). The pre-aggregation rollup (`make bench`, above) is the
*least* specific lever; these are the specific, explainable ones a data platform rewards.
Measured by `make cost-levers`, reusing `bench.py`'s canonicalization and summary reader.
The block below is regenerated verbatim by that command:

<!-- COST_LEVERS_START -->

_Measured by `make cost-levers` on `bench_large` (attributed_conversions 25,168 rows ≈ 3 granules; exposures_landed 55,000 ≈ 7 granules). Both tables canonicalized to merged steady state first; rows/bytes are ClickHouse's cache-independent `X-ClickHouse-Summary`. Re-run byte-stable._

**Lever 1 — projection ordered by `event_time` (WINS).** A date-scoped reporting slice over `attributed_conversions`. The base table is sorted by `conversion_id`, so `event_time` is scattered across every granule and the range predicate prunes nothing; the projection keeps an alternate copy ordered by `event_time` that ClickHouse auto-picks for the range.

| measure | no projection | projection | ratio |
|---|---|---|---|
| rows read | 25,168 | 16,384 | 1.54x |
| bytes read | 427,856 | 278,528 | 1.54x |

- _Why the bytes drop:_ the projection reads only the window's granules instead of the whole table. _Cost:_ a projection is a second physical copy of the table (more disk) maintained on every insert (slower writes). _Caveat:_ a projection can't serve a `FINAL` query — measured non-FINAL, valid because the canonicalized table is single-version, so FINAL and non-FINAL return identical rows here.

**Lever 2 — FINAL-avoidance / skip index (DOCUMENTED NEGATIVE RESULT).** The schema does not reward a secondary lever here, and knowing when *not* to add one is the point. Two candidates measured, both lose:

_2a — `SELECT ... FINAL` vs explicit `argMax(...) GROUP BY conversion_id`:_

| measure | FINAL | argMax GROUP BY | ratio |
|---|---|---|---|
| rows read | 25,168 | 25,168 | 1.00x |
| bytes read | 226,512 | 855,712 | 0.26x |

`argMax` reads MORE, not less: on merged single-version data `FINAL` reads only the columns it needs, while the manual collapse must scan `conversion_id`, `revenue`, `attributed`, and `processed_at` for every row and build a hash table. `FINAL` is already optimal — the version-part cost RUNBOOK incident #1 describes exists only *before* the merge, which `_canonicalize` (correctly) removes.

_2b — bloom skip index on a non-leading column (`program_genre`, and the far-more-selective `ip` — 157 of 55,000 rows):_

| query | rows read, no index | rows read, bloom index | granules skipped |
|---|---|---|---|
| `program_genre = 'sports'` | 55,000 | 55,000 | 0 |
| `ip = '100.64.0.273'` | 55,000 | 55,000 | 0 |

The index skips **zero** granules for either predicate — even the 0.3%-selective `ip`. The blocker is physical clustering, not selectivity: `exposures_landed` is sorted `(campaign_id, event_time, exposure_id)`, so the leading key already prunes a campaign filter (a bloom on `campaign_id` would be redundant), and every non-key column is uniformly scattered across all granules — an `ip`'s rows sit in every granule, so no granule can be excluded. _The condition that would change it:_ physical clustering of the filtered column (a sort key that groups it, or naturally clustered data). _Cost of adding one anyway:_ write-time index maintenance and disk for a summary that prunes nothing.

**Lever 3 — PREWHERE the window predicate (WINS).** A wide-column read behind the selective window filter.

| measure | WHERE (no auto-move) | PREWHERE | ratio |
|---|---|---|---|
| rows read | 25,168 | 25,168 | 1.00x |
| bytes read | 8,061,895 | 6,660,392 | 1.21x |

- _Why the bytes drop:_ `WHERE` (with `optimize_move_to_prewhere = 0`) reads every selected column for all scanned rows, then filters; `PREWHERE` reads the filter columns first and fetches the wide columns (the `assists` array, ids) only for surviving rows. Same rows read (the window doesn't prune granules without the projection), fewer bytes. _Cost:_ none structural — but measured against ClickHouse's default (which auto-moves the predicate already) the delta is zero, so this only 'wins' relative to an explicitly disabled move.

_Honesty boundary: these are `bench_large` numbers; the mechanisms are the claim, not the magnitudes. All three win on **scoped** access (a date range, one dimension) — the all-time per-campaign report is already near-optimal for this schema (campaign is the leading sort key), which is exactly the setting where a platform reaches for these levers. The profile was not tuned to inflate any win; lever 2 is reported as the negative result it measured._

<!-- COST_LEVERS_END -->

## Observability — alert rules

Four Alertmanager rules cover the deterministic conditions, each proven by
`make test-alerts` — `promtool check rules` + `test rules` from the digest-pinned
Prometheus image against **real captured registries** (`make metrics-capture` dumps
each stage's terminal Prometheus registry from a knobbed run;
`observability/gen_alert_fixtures.py` bakes those numbers into the test fixture, so
the threshold-crossing values come from a real stage run, never hand-authored):

| alert | expr | fires when |
|---|---|---|
| `ConsumerLag` | `resolve_input_backlog > 100` | conversions backlog at drain start (resolve runs in-process in the engine since Phase 16) |
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
  **fires on the anomalous profile (`long_delay`)**, and three of the four **stay
  silent on the clean one (`tiny`)** — a discrimination between a knobbed profile
  and a baseline. The exception since Phase 16 is `RestatementMagnitude`: tiny's 5
  shared-IP conversions are deferred hot and credited by the reconcile pass, which
  restates one campaign's ROAS by 12.9 (threshold 1.0) — a real restatement, so the
  rule fires on tiny too and the fixture says so (`observability/gen_alert_fixtures.py`).
  They do **not** claim single-knob isolation (one knob → one alert): `long_delay`
  trips more than one rule, and that is expected. The proven claim is "the anomalous
  profile trips the alerts; the clean profile trips only the restatement its own
  deferral landing causes," nothing stronger.

## Agent eval — fault → diagnosis

Every fault profile plus the no-fault baseline, run 5× (30 live invocations), scored against the pure rubric in `agent/eval/scoring.py` (unit-tested offline — the live sweep only supplies the LLM outputs). The agent is non-reproducible by construction (temperature is unset on the Claude-5 family, DECISIONS Phase 9), so each cell reports a spread over reps, not a single-run claim; the reps measure residual stability.

> **Provenance — measured in Phase 10, pre-Phase-16; not re-run.** The three
> tables below were captured by `make agent-eval` (30 live invocations) before
> Phase 16 deferred shared-IP conversions to reconciliation. Since then the
> context a fault profile presents has moved in one place: three profiles carry
> ambiguous conversions (`shared_ip_spike` 25, `late_burst` 1, `co_view_bug` 1)
> that are now credited by the reconcile pass (the deferral landing).
> `late_burst`'s one is a revenue-0 `site_visit`, so it cannot move any
> campaign's ROAS and its 26.604 cell stands; the other two restate, so their
> `max|Δroas|` is no longer 0 — those two cells are blanked below rather than
> shown stale. Verdicts,
> `ip_resolved_fraction` and `ambig` are not affected by construction, but the
> catalog has not been re-validated live (API tokens). Re-run: BACKLOG 49.

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
| `shared_ip_spike` | 0.992 | 0.420 | 25 | 3 | — (restates since Phase 16) | 0.000 |
| `real_lift` | 1.000 | 0.061 | 0 | 1 | 0.000 | 0.000 |
| `late_burst` | 1.000 | 0.065 | 1 | 2 | 26.604 | 0.000 |
| `co_view_bug` | 0.988 | 0.067 | 1 | 3 | — (restates since Phase 16) | 0.000 |
| `duplicate_flood` | 0.984 | 0.074 | 0 | 1 | 0.000 | 0.000 |
| `no_fault_baseline` | 0.977 | 0.039 | 0 | 1 | 0.000 | 0.000 |

### Honesty boundary

These are small-profile results reported as measured. `co_view_bug`'s abstention is a **labeled capability boundary**, not a gap papered over: the co-view *adjusted* factor is a DECISIONS won't-do (BACKLOG 26 — the honest per-genre expected baseline does not exist in serving data, and sourcing it from the producer's multiplier would couple reporting to generation parameters). The agent correctly declines to diagnose from noise. Verdict/hypothesis stability across reps is a measurement, never a gated assertion (the AI edge is carved out of the byte-identical guarantee, CLAUDE.md).

## Lakehouse landing + orchestrated reconciliation (Phase 12)

The reconciliation long-window matcher can source its candidate exposures from a
local **Iceberg** lake (via **DuckDB** `iceberg_scan`) instead of ClickHouse, and
runs as a **Dagster** day-partitioned asset — without changing the pipeline's
output. Measured on a clean `long_delay` stack (`make test-int-lakehouse` +
`make lake-land && make reconcile-dagster PROFILE=long_delay && make eval`).

### Byte-identical source swap (Done-when #2)

The recovered rows are identical whether the matcher's exposures come from
ClickHouse `exposures_landed FINAL` or from Iceberg-via-DuckDB:

| check | result |
|---|---|
| lake `raw.exposures` rows after `make lake-land` | 360 (== exposures produced), day-partitioned on `event_time` |
| ClickHouse-sourced vs Iceberg-sourced recovered rows | **byte-identical** (same set, same order, same `processed_at`, same last-touch exposure_id + assists) |
| `make eval` recall (long_delay) | **0.9733** — unchanged from the ClickHouse-sourced reconcile |
| `path='reconciled'` rows written | 32 (== the live pin since Phase 16: 35 candidates → 32 recovered — 29 state-misses + the 3 deferred shared-IP conversions; 3 organics without an in-90d exposure stay unmatched) |

Parity holds because the lake read (a) dedups on `exposure_id` (reproducing the
ReplacingMergeTree FINAL collapse — `exposure_id` is unique) and (b) returns naive
UTC to the millisecond, matching what clickhouse-connect returns (`SET
TimeZone='UTC'` then drop tzinfo).

### Orchestration — day-partitioned backfill (Done-when #3)

`reconciled_conversions` is a Dagster day-partitioned asset (static day keys —
wall-clock-independent for determinism); `reconciled_report` is the global
finalize. Headless via `make reconcile-dagster` (ephemeral instance, no webserver).

Single partition (`make reconcile-dagster PROFILE=long_delay PARTITION=2026-08-01`):

```
reconciled_conversions[2026-08-01] materialized
reconcile-dagster: 1 day-partition(s) recovered + finalize
```

Backfill over the candidate date range (`make reconcile-dagster PROFILE=long_delay`):

```
reconciled_conversions[2026-08-01] materialized
reconciled_conversions[2026-08-02] materialized
reconciled_conversions[2026-08-09] materialized
reconciled_conversions[2026-08-19] materialized
... (2026-08-20 … 2026-08-27, 2026-08-29 … 2026-08-31)
reconcile-dagster: 15 day-partition(s) recovered + finalize
```

15 day-partitions cover all candidate days (13 before Phase 16; the 3 deferred
shared-IP conversions add 2026-08-02 and 2026-08-09); the union reproduces the full
recovery (recall 0.9733). Provenance: the transcript above was captured live in
Phase 12 (13 days) and its day list re-derived OFFLINE for Phase 16 from the
long_delay candidate set (`orchestration.run._candidate_days` logic over the hot
output), not re-captured from a live `make reconcile-dagster`; the 32-row recovery
itself is live-pinned by `make test-int-lakehouse` / `test-int-long-delay`. Iceberg snapshot ids / commit times and Dagster run ids
are non-deterministic and are never asserted on — only row content is (DECISIONS
Phase 12).
