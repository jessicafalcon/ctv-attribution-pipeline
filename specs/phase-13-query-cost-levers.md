# Phase 13 — Query cost levers (PROPOSED)

Contract for the `phase-13-query-cost-levers` branch. Source: post-plan extension
— **not** in the original `docs/PHASES.md` plan (Phases 0–11). Covers sketch 3 (a
real "made this query measurably cheaper, and why" story).

**Status: PROPOSED — do not open a branch until approved.** No new dependencies.

## Why

The existing `make bench` (Phase 7) demonstrates the *least* interesting cost lever —
pre-aggregation (rollup vs full scan). What a data platform actually rewards is a
**specific, explainable** query-cost win — a query made measurably cheaper, and why. The compelling
ClickHouse-native levers are **projections, data-skipping indexes, and PREWHERE** —
the ones that bite at OLAP scale. This phase measures each as a before/after on the
report query, reusing the honest `X-ClickHouse-Summary` harness from `queries/bench.py`.

## The central constraint

**Skip indexes and projections are no-ops below one granule (8192 rows).** ClickHouse
prunes at granule granularity; a table smaller than one granule is a single granule,
so a skip index can skip nothing and a projection reorders one block. This is precisely
why the existing bench win is modest — `tiny`/`long_delay` tables are sub-granule. So
this phase **requires a multi-granule profile**: enough rows (tens of thousands, ≥ a
few granules per key band) that pruning has something to skip. The measured absolute
win is profile-dependent; the **mechanism and direction** are the claim, not a
magnitude.

## DONE command

```
make down && make up && make seed PROFILE=bench_large && make run && \
make cost-levers && make test && make lint
```

- `make cost-levers` measures three before/after lever pairs on the report query,
  reads `read_rows`/`read_bytes` from `X-ClickHouse-Summary`, canonicalizes both sides
  to merged steady state first, asserts each lever reduces `read_bytes` (direction,
  magnitude-free) and returns identical result rows (6 dp), and writes the explanation
  to `docs/RESULTS.md`.
- `make test` + `make lint` green; gate-0 tiny golden byte-identical.

## Done-when

1. **Three levers, each measured.** On the campaign report query (filtered to one
   campaign + a date range):
   - **Projection** `(campaign_id, event_time)` on `attributed_conversions` — the
     table's RMT key is `conversion_id`, so a campaign+time filter can't prune today.
   - **Data-skipping index** (bloom filter on `campaign_id`) on `exposures_landed`.
   - **PREWHERE** the high-selectivity predicate ahead of the wide-column read.
   Each reports `read_rows`/`read_bytes` before vs after.
2. **Deterministic measurement.** Reuse `bench.py`'s `OPTIMIZE ... FINAL`
   canonicalization (the Phase-7 `read_rows`-counts-unmerged-parts gotcha) before
   every measurement; query cache off.
3. **Direction-asserted.** Each lever: after `read_bytes` < before (magnitude-free
   assert, like the bench direction assert). Result rows identical to 6 dp.
4. **Written why + tradeoff** per lever in `docs/RESULTS.md`: what it prunes, why the
   bytes drop, and the cost accepted (a projection is a second physical copy + slower
   inserts; a skip index costs write time and space). No lever's cost is hidden.
5. Gate-0 tiny golden byte-identical; `make test` + `make lint` green.

## Pinned decisions (do not re-litigate)

- **Multi-granule profile is mandatory** (see central constraint). If phase-14's
  `scale_curve` tiers land first, reuse its largest tier; otherwise add a minimal
  `bench_large` producer profile sized to cross several granules. A sub-granule
  profile makes the levers no-ops and the measurement meaningless.
- **Lever DDL runs only inside the `make cost-levers` flow against the `bench_large`
  run — never on the tiny golden DDL path**, so gate-0 stays byte-identical. (A
  projection changes physical layout, not query results, but keeping it off the golden
  path keeps the guarantee clean.)
- **Direction assert, magnitude-free.** Numbers are profile-dependent; the claim is
  the mechanism ("projection prunes to one campaign's granules → N× fewer bytes"),
  reported as measured, with the small-profile honesty boundary that a 10× needs the
  volume SCALING.md describes.
- **Reuse `bench.py`'s summary reader and canonicalization** — do not re-implement;
  the Phase-7 `FINAL read_rows` non-determinism fix is load-bearing here too.

## Scope (files)

- `queries/cost_levers.sql` (the three lever DDLs + before/after query pairs),
  `queries/measure_levers.py` (drives the pairs through `bench.py`'s summary reader),
  `Makefile` `cost-levers`.
- `producer/profiles/bench_large.py` (if not reusing phase-14's `scale_curve`).
- `docs/RESULTS.md` (a "Query cost levers" section with the three tables + why/tradeoff).
- Tests: each lever's direction assert and row-equality as offline-shaped unit tests
  where possible; the measured run is the live gate.
- Records: this spec, DECISIONS (granule-floor + off-golden-path rationale), BACKLOG,
  CLAUDE.md status + commands.

## Review & stack risk

- **code-reviewer** at the finish line (mandatory): the lever DDL adds projections /
  skip indexes to ClickHouse tables — schema change, determinism check (levers don't
  alter result rows), off-golden-path check.
- **functionality-tester** after code-reviewer.
- **security-reviewer NOT triggered** — no CI / `.env` / compose / ClickHouse-user /
  agent-context change (a projection/index DDL is not a user or exposure change).
- **coherence-auditor** at phase exit.

## Out of scope (deferred, recorded)

- Tuning the profile to inflate the win (honesty boundary: report as measured).
- Materialized-view vs projection comparison beyond the one projection shown.
- Compression-codec levers (a further SCALING note).
