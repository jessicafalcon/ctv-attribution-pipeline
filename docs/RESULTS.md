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
