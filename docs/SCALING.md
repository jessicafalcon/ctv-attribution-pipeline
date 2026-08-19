# SCALING.md — where the design breaks and what changes

Full deliverable is **Phase 11** (50k/sec and 500k/sec tiers, partition math,
state backend, Flink mapping, ClickHouse tier changes — see `PHASES.md`). This
file exists early only to catch scaling notes at the phase that discovered them,
so they aren't reconstructed later. Notes accumulate here; the tiered write-up is
authored in Phase 11.

## Notes accumulating toward the Phase 11 write-up

### Rollup benchmark: the win is scan-size, and it compounds at volume (Phase 7)

`make bench` (Phase 7) shows the `campaign_hourly` rollup reading 2.5× fewer rows
and 1.6× fewer bytes than the naive full `FINAL` scan-and-join, at `long_delay`
scale (see `RESULTS.md`). The edge is modest here only because the raw tables are
small. What matters at scale: the **naive scan grows with every conversion and
exposure** (and pays the `FINAL` merge over an ever-larger set), while the
**rollup read grows only with the number of distinct `(campaign, hour)` buckets** —
bounded by campaigns × hours in the reporting window, independent of event volume.
At the 50k/500k tiers a per-read full-history scan is the thing that breaks; the
scheduled-refresh rollup is what keeps read cost flat. The refresh itself is the
cost that grows, but it is paid once on a schedule, off the read path
(ARCHITECTURE §3.3: scheduled refresh, never insert-triggered summing MVs).

### Engine dedup: full seen-set now, TTL'd under continuous follow (Phase 5)

The Phase-5 engine is a **bounded batch drain** — it reads each topic start→end
once and holds it in memory — so dedup is a full `conversion_id`/`exposure_id`
seen-set: O(n) in the drained batch, deterministic on the single partition, and
correct because the whole stream is present at once (ARCHITECTURE §8, DECISIONS
Phase 5).

**What changes at scale / under continuous follow.** Once the engine follows the
topics continuously (unbounded, no EOF), an unbounded seen-set is a memory leak.
Then dedup must become **TTL'd state**, evicting an id once no further duplicate
of it can plausibly arrive — keyed on `event_time + max_resend_delay` against the
watermark, sized to the real duplicate-injector / upstream re-send delay. This is
also where the seeded fixture stops being a faithful proxy: the fixture's
duplicate is **timestamp-identical** to its original (same `event_time` and
`ingest_time`), so a real deployment with genuinely-later re-send timestamps is
needed to exercise TTL sizing. Continuous follow is deferred (no phase owns it
yet); the two resolve BACKLOG rows re-defer on the same trigger.
