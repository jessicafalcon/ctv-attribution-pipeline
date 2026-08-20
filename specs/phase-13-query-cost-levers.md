# Phase 13 — Query cost levers

Contract for the `phase-13-query-cost-levers` branch. Source: post-plan extension
— **not** in the original `docs/PHASES.md` plan (Phases 0–11). Covers sketch 3 (a
real "made this query measurably cheaper, and why" story).

**Status: APPROVED, with schema-reality corrections.** No new dependencies. The
originally-proposed levers named columns/keys the serving schema does not have —
a projection on a `campaign_id` column `attributed_conversions` never had, and a
skip index on `exposures_landed.campaign_id` which is already the LEADING sort key
(so the primary index already prunes it). Corrected to buildable levers below and
recorded in DECISIONS Phase 13 before any build. The mechanisms the phase teaches
(projection, data-skipping index, PREWHERE, FINAL-avoidance) are unchanged; only
the columns/queries moved to what the schema actually rewards.

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

1. **Three levers, each measured.** On the campaign report query, with a
   **date-/dimension-scoped variant** where a lever needs one (the all-time
   per-campaign report is already near-optimal for this schema — a lever needs a
   query that exercises it; that is expected, not a fudge):
   - **Projection ordered by `event_time`** on `attributed_conversions`. The table's
     RMT key is `conversion_id`, so `event_time` is scattered across every granule and
     a date-range predicate can't prune today; the projection is an alternate physical
     ordering ClickHouse auto-picks for the range. Requires a **date-scoped report
     variant**. (Corrected: the table has no `campaign_id` column — campaign comes from
     the join to `exposures_landed` — so the projection orders by `event_time`, not the
     originally-written `(campaign_id, event_time)`. DECISIONS Phase 13.)
   - **Lever 2 — measured, land on whichever direction-asserts** (ranked preference):
     (i) **`FINAL` vs explicit `argMax(...) GROUP BY conversion_id`** on
     `attributed_conversions` — the schema-native lever (the whole serving layer is
     ReplacingMergeTree + FINAL; this is the exact cost RUNBOOK incident #1 is about);
     (ii) a **data-skipping index** (bloom) on a **non-leading, physically-clustered**
     column with a dimension-scoped query — clustering measured, NOT assumed (the
     originally-written bloom on `exposures_landed.campaign_id` is redundant: campaign_id
     is the LEADING sort key, already primary-key-pruned); (iii) a **documented negative
     result** — a first-class landing — if neither direction-asserts, stating precisely
     when a secondary skip index does not help on this schema and the condition that
     would change it. Measure, don't assume (FINAL is often optimized; clustering depends
     on the generator).
   - **PREWHERE** the high-selectivity predicate ahead of the wide-column read, measured
     `optimize_move_to_prewhere=0` (before) vs explicit `PREWHERE` (after) — measuring
     against ClickHouse's auto-moved default would show no delta.
   Each reports `read_rows`/`read_bytes` before vs after.
2. **Deterministic measurement.** Reuse `bench.py`'s `OPTIMIZE ... FINAL`
   canonicalization (the Phase-7 `read_rows`-counts-unmerged-parts gotcha) before
   every measurement; query cache off.
3. **Direction-asserted.** Each lever: after `read_bytes` < before (magnitude-free
   assert, like the bench direction assert). Result rows identical to 6 dp.
4. **Written why + tradeoff** per lever in `docs/RESULTS.md`: what it prunes, why the
   bytes drop, and the cost accepted (a projection is a second physical copy + slower
   inserts; a skip index costs write time and space; `argMax` GROUP BY trades the
   FINAL-merge read for a full-scan aggregate that must read the version columns and
   hold a hash table). No lever's cost is hidden. A documented negative result (lever 2
   landing on option iii) states the same way *why* it does not win here.
5. Gate-0 tiny golden byte-identical; `make test` + `make lint` green.

## Pinned decisions (do not re-litigate)

- **Multi-granule profile is mandatory** (see central constraint). Phase-14's
  `scale_curve` was sized for an in-process engine drain, not a full pipeline load into
  ClickHouse, so this phase adds a `bench_large` producer profile sized to push
  `attributed_conversions` and `exposures_landed` **several granules** past the 8192-row
  floor through the live stack. Verify the counts cross the floor before measuring any
  lever — a sub-granule table is one granule and every lever is a no-op on it.
- **Lever DDL runs only inside the `make cost-levers` flow against the `bench_large`
  run — never on the tiny golden DDL path**, so gate-0 stays byte-identical. (A
  projection changes physical layout, not query results, but keeping it off the golden
  path keeps the guarantee clean.)
- **Direction assert, magnitude-free.** Numbers are profile-dependent; the claim is
  the mechanism ("projection prunes to the date range's granules → N× fewer bytes"),
  reported as measured, with the small-profile honesty boundary that a 10× needs the
  volume SCALING.md describes.
- **Reuse `bench.py`'s summary reader and canonicalization** — do not re-implement;
  the Phase-7 `FINAL read_rows` non-determinism fix is load-bearing here too. Import
  `_canonicalize` unchanged (RUNBOOK incident #1 cites it by name — a rename ripples;
  BACKLOG 37).
- **Schema-reality correction (DECISIONS Phase 13).** `attributed_conversions` has no
  `campaign_id` column (campaign is the join to `exposures_landed`); `exposures_landed`
  is ordered `(campaign_id, event_time, exposure_id)` so its campaign filter is already
  primary-key-pruned. Consequence: the all-time per-campaign report is already
  near-optimal here; the levers win on **date-/dimension-scoped** access patterns, which
  is exactly when a platform reaches for them. Each lever carries the query variant that
  exercises it.
- **Lever 2 is measured, not assumed.** Ranked preference FINAL-vs-`argMax` >
  clustered skip index > documented negative result; land on whichever direction-asserts.
  A documented negative result is a first-class outcome (knowing when *not* to add an
  index), not a phase failure.

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
