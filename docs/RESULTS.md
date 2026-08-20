# RESULTS.md

Measured outcomes. Finalized in Phase 11 (accuracy tables, agent eval); this file
starts at the Phase-7 benchmark so the numbers are captured where they were run.

## Benchmark — naive full scan vs optimized rollup

The same four-metric advertiser report (ROAS, CPA, CVR, site-visit rate per
campaign) run two ways over the `long_delay` profile after a full pipeline pass
(`make down && make up && make seed PROFILE=long_delay && make run && make bench`):

- **naive** — full `FINAL` scan-and-join of the raw serving tables
  (`queries/report.sql`: `attributed_conversions` ⋈ `exposures_landed`).
- **optimized** — the pre-aggregated `campaign_hourly` rollup (`queries/bench.sql`).

Both return the identical metric rows (the benchmark asserts equality to 6 dp).

| metric | naive (full scan) | optimized (rollup) | naive/opt |
|---|---|---|---|
| rows read | 864 | 340 | 2.5× |
| bytes read | 42,246 | 25,840 | 1.6× |
| latency (ms, median) | 8.49 | 3.21 | 2.6× |

### Why each change works

- **Rows read (2.5×).** The naive query reads every attributed conversion **and**
  every landed exposure, then merges the ReplacingMergeTree parts (`FINAL`) to drop
  superseded/duplicate rows before the join. The optimized query reads
  `campaign_hourly`, which holds one pre-aggregated row per `(campaign, hour)` — the
  join and the aggregation were already done once, at rollup-refresh time, so the
  read touches far fewer rows.
- **Bytes read (1.6×).** Fewer rows, and narrower ones: the rollup stores only the
  numeric components it sums (spend, counts, revenue), whereas the naive scan must
  read the wide raw rows including the string ids it joins on. The byte ratio is
  smaller than the row ratio because `campaign_hourly` is keyed
  `(campaign_id, hour)` — a low-cardinality profile has few hour buckets, so the
  rollup is not as many-times smaller in bytes as in rows here.
- **Latency (2.6×).** Fewer rows/bytes and no join at read time. At this profile
  scale latency is noisy (single-digit ms, cache and scheduling jitter dominate),
  so the **rows/bytes read are the reliable signal** — they are deterministic and
  cache-independent (from ClickHouse's `X-ClickHouse-Summary`), and both queries run
  with the query cache off.

### Honesty boundary

These are small-profile numbers; the rollup already wins, but the win is modest
because the raw tables are small. The structural point is what scales: the naive
scan grows with **every** conversion and exposure, while the rollup grows only with
the number of distinct `(campaign, hour)` buckets. At 50k–500k msgs/sec that gap is
the difference between a full-history scan and a bounded rollup read — see
`SCALING.md`. The numbers here are reported as measured; the profile was not tuned
to inflate the optimized win.

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
