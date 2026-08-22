# Phase 18 — Cost and ops levers (PROPOSED)

Contract for the `phase-18-cost-and-ops` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), findings 6 (full-refresh rollup) and 7 (operational blind spots: schema
compat `NONE`, un-merged parts as the real `FINAL` cost, no dollar/CPU cost, no live
alert firing). Depends on Phase 16 merged; Phase 17 is NOT a dependency — every item
here works against ClickHouse serving tables regardless of where they were loaded from.

**Status: PROPOSED — do not start until approved.** No new dependencies. Each item is a
measured before/after in the `bench.py` / `measure_levers.py` style (Phase 7/13): a
direction assert, never a magnitude pin.

## Why

The architecture review's sharpest operational criticism: the repo measures *rows and
bytes read*, never *cost over time*; the rollup recomputes everything on every refresh;
`FINAL`'s real price (un-merged parts) was filed as a benchmark quirk rather than the
operating cost it is; and the schema registry runs at compatibility `NONE`, which is the
opposite of a data contract. Each is a small, ClickHouse/Kafka-specific change with a
number attached — the "made it measurably cheaper, and why" story, told four ways.

## The central constraint

**Every lever ships with its before/after and its guard.** A lever without a measured
delta is a claim; a lever without an alert or test that catches regression is a one-
off. Serving-row content is byte-identical before and after every lever (6dp row-
equality, the Phase-13 harness) — cost changes, answers don't.

## DONE command

```
make test && make lint && make test-alerts \
  && make down && make up && make seed PROFILE=long_delay && make run \
  && make rollup-bench && make cost-report && make test-int-long-delay
```

- `make rollup-bench` (new): full refresh vs dirty-partition refresh after a reconcile
  pass — rows read and rows written, direction assert (incremental < full), 6dp
  equality of `campaign_hourly` FINAL rows.
- `make cost-report` (new): per-query cost table from `system.query_log` for the
  report / restate / bench queries of this run, printed and written to
  `query_cost_daily`.
- `make test-alerts`: promtool proves the two new rules (below) fire on captured
  fixtures and stay silent on tiny.

## Done-when

1. **Incremental rollup refresh.** Reconciliation and the hot-path sink record the
   `(campaign_id, hour)` keys they touched in a small `rollup_dirty` table
   (ReplacingMergeTree, keyed on the pair). `refresh_campaign_hourly` recomputes only
   dirty keys plus a trailing lookback of N hours (N = allowed lateness, same knob as
   the engine), then clears the processed keys by version. A `--full` flag keeps the
   current full rebuild as the oracle. `rollup-bench` asserts incremental == full (6dp)
   and incremental reads fewer rows. `report_snapshots` stamps a new `reported_at` only
   for restated keys — the restatement view is unchanged in content.
2. **Part-count and merge-lag are first-class.** New Prometheus metrics from a
   lightweight ClickHouse scraper (existing `clickhouse-connect`, SELECT-only, reads
   `system.parts` / `system.merges`): `clickhouse_active_parts{table}` and
   `clickhouse_merge_backlog_seconds`. Two new alert rules, `PartCountHigh` and
   `MergeBacklog`, with promtool fixtures captured from a real run (the
   `metrics-capture` path). RUNBOOK incident #1 ("the benchmark that lied") is
   re-framed: un-merged parts are the operating cost of `FINAL`; the guard is now an
   alert, and the runbook's "would-catch-it-next-time" cell changes from *not covered*
   to the new rule. `make check-runbook` still passes.
3. **Async inserts, measured.** The engine sink (`streaming/sink.py`) gains an
   `async_insert=1, wait_for_async_insert=1` mode behind a flag (default on in `make
   run`, off in the gate-0 golden path until the parity check passes). Measured on
   `bench_large`: parts created per minute and insert wall-clock, before/after;
   direction assert (fewer parts). Serving rows byte-identical. The stale "async inserts
   are a SCALING lever, not built" lines in `sink.py`, SCALING.md and ARCHITECTURE §3.3
   are updated to "built, measured, default on".
4. **Query cost in a unit the business reads.** `query_cost_daily` (ReplacingMergeTree
   keyed `(day, query_tag)`) is filled from `system.query_log`: `query_duration_ms`,
   `read_rows`, `read_bytes`, `memory_usage`, and a derived `cpu_seconds` (`ProfileEvents
   ['OSCPUVirtualTimeMicroseconds']`). Every report/restate/bench query is tagged via
   the `log_comment` setting so the table is per-query, not per-session. A Grafana panel
   shows cost per query per day; the report in `docs/RESULTS.md` gains a "Cost per
   report query" table with the measured values from this run. A documented
   `$/cpu-second` constant (clearly labelled as an illustrative ClickHouse-Cloud-style
   rate, not a measurement) turns it into dollars; the constant is a config value, never
   hard-coded in SQL.
5. **Schema compatibility BACKWARD, proven.** `producer/schemas.py` sets every subject
   to `BACKWARD`. Proof: a test adds an optional field to `Exposure`
   (`creative_id: str | None = None`), registers it, and asserts (a) the registry
   accepts it, (b) the Phase-1 golden fixtures (old schema) still validate against the
   consumer, (c) removing a required field is REJECTED (409). The field is test-only
   (removed after the test) unless the developer chooses to keep it; if kept, fixtures
   do NOT change because the field is optional with a null default. ARCHITECTURE §3.3
   "Redpanda" and the `schemas.py` docstring drop the "no evolution story yet" caveat.
6. **Live alert firing path.** Each stage pushes its terminal registry to a Prometheus
   Pushgateway (compose service, digest-pinned, loopback-bound like the others) so the
   four existing rules plus the two new ones evaluate against a live scrape and
   Alertmanager actually delivers the webhook to `agent/webhook.py`. `make test-int`
   gains a check that a `long_delay` run produces a FIRING `RestatementMagnitude` in
   Alertmanager's API. Webhook amplification (BACKLOG): the handler dedupes by
   `groupKey` per sweep — one sweep per alert group, not per alert. Closes two BACKLOG
   rows ("Live Alertmanager firing path", "Webhook sweep amplification").
7. **Shard key chosen and defended.** SCALING.md's 500k tier names `household_id` as
   the shard key (corrections and dedup shard-local; reports aggregate via `Distributed`)
   and records why not `campaign_id`. Docs-only; no cluster is run.

## Pinned decisions (do not re-litigate)

- **Direction asserts, never magnitude pins** (Phase 7/13 precedent). Cost numbers are
  reported as measured and regenerated by the targets; tests assert `<`, not a value.
- **Dirty-set over bounded-lookback MV.** A refreshable MV with lookback can miss a
  60-day correction; the dirty set is exact and cheap. The full refresh stays as the
  equality oracle.
- **Pushgateway for batch stages.** The stages are finite drains (ARCHITECTURE §8), so
  pull-scrape has nothing to scrape after exit; Pushgateway is the standard answer for
  batch jobs. Per-run gateway reset is part of `make run`.
- **Dollars are illustrative, CPU-seconds are measured.** The `$` column is a labelled
  conversion of a measured quantity, never presented as a billed figure.
- **`agent_ro` grants unchanged.** The scraper and cost writer use a separate user; the
  agent's SELECT-only user gains no new grants.

## Scope (files)

- `reconcile/rollup.py` (dirty-set refresh, `--full` oracle), `streaming/sink.py`
  (dirty-key recording; async-insert flag), `clickhouse/` DDL + migration
  (`rollup_dirty`, `query_cost_daily`), `clickhouse/users` (metrics/cost writer user),
  `queries/rollup_bench.py`, `queries/cost_report.py` (reuse `bench.py`'s
  canonicalization — graduate `_canonicalize`/`_measure` to a public module, closing the
  BACKLOG "graduate bench helpers" row), `observability/` (scraper, two rules, promtool
  fixtures, Pushgateway in compose + prometheus.yml, Grafana cost panel),
  `producer/schemas.py` (BACKWARD) + test, `agent/webhook.py` (groupKey dedupe),
  `Makefile`, `.github/workflows` (Pushgateway in the integration job), RUNBOOK,
  SCALING.md, ARCHITECTURE §3.3/§8, RESULTS.md cost table, BACKLOG (close 4 rows),
  DECISIONS Phase 18, PHASES.md, CLAUDE.md.

## Review & stack risk

- **code-reviewer** (mandatory): serving-row byte-identity under every lever;
  idempotent `rollup_dirty` clearing (a crash between refresh and clear must re-refresh,
  never skip); no magnitude pins; helper graduation keeps `bench.py` output identical.
- **security-reviewer** (mandatory — compose service, ClickHouse users, CI workflow,
  webhook handler change): Pushgateway loopback-only; new writer user scoped to its two
  tables; `log_comment` tags never carry user-influenced text; webhook payload text
  still never reaches the LLM.
- **functionality-tester**: DONE command; incremental == full on `long_delay` after a
  reconcile pass that restates ≥ 1 campaign; alert fires live; BACKWARD rejects a
  breaking change.
- **coherence-auditor** at exit: every "async inserts not built" / "no evolution story"
  / "not alert-covered" sentence is found and updated.
- Stack risk: `log_comment` and `ProfileEvents` column shapes differ across ClickHouse
  versions — verify on the pinned 24.8 image in the first hour; Pushgateway image needs
  a digest pin like the other five.

## Out of scope (deferred, recorded)

- A ClickHouse cluster / `Distributed` tables — SCALING.md tier note only (item 7).
- Continuous follow / stream framework — still the open question after Phase 17.
- Cost attribution per *advertiser* (multi-tenant chargeback) — needs a tenant
  dimension the event model does not have; recorded as a README "Next steps" item.

## Pre-branch reconciliation required (2026-08-22)

This spec was written before Phase 17 merged (PR #31) and its body has NOT been
rewritten — that rewrite is the Phase-18 branch's commit 1 (CLAUDE.md Workflow
rules: spec-reconciliation amendment first, stop for approval, no implementation
before it is approved). The amendment must resolve every item of BACKLOG row 56
(Phase-17 coherence audit D2 + Q3/Q4):

- **`streaming/sink.py` → the lake landing step.** Done-when 1 and 3 name the
  hot-path ClickHouse sink, deleted in Phase 17. The engine lands to the lake
  (`lake/land_*`); the ONE serving-table writer is the Dagster load
  (`lake/load_serving.py`). The "Phase 17 is NOT a dependency" sentence is false.
- **Dirty set owned by the loader.** The loader already knows the days/keys it
  touched (Phase-17 D6), so `rollup_dirty` recording moves to the loader, and
  async inserts belong on the loader's `client.insert`, not on a sink that no
  longer exists.
- **DONE command gains `lake-reset` + `PROFILE=long_delay`.** As written (`make
  down && make up && make seed PROFILE=long_delay && make run`) it no longer runs:
  it needs `make lake-reset PROFILE=long_delay CONFIRM=yes` after `make down` and
  `PROFILE=long_delay` on `make run` (the engine binds its lake from `--profile`).
- **Loader-owned dirty-set gate (developer ruling, Phase-17 review).** After a
  reconcile pass that restates ≥ 1 campaign, the set of `(campaign_id, hour)` in
  `rollup_dirty` must equal the set of keys whose `campaign_hourly` rows differ
  between pre- and post-refresh FULL rebuilds — the dirty set is the contract
  between the loader and the rollup; a wrong set is silently wrong while the
  full-refresh oracle still passes.
- **`reconciled_at` anchoring question.** Whether `reconciled_at` should anchor
  in the lake rather than ClickHouse `_max_ingest` (Phase-17 coherence Q3; kept in
  ClickHouse for now) — it interacts with the dirty-set design and must be decided
  in the amendment.
- **Recapture procedure.** Done-when 2 recaptures the promtool fixtures via `make
  metrics-capture`, which since Phase 17 reproduces its numbers ONLY from a clean
  stack AND a clean lake: `make down && make lake-reset PROFILE=<p> CONFIRM=yes &&
  make up && make seed PROFILE=<p> && make metrics-capture PROFILE=<p>`.

The amendment also adds the three `specs/TEMPLATE.md` sections this spec predates:
Evidence, Record updates, Threat model (every new target here takes a variable).
