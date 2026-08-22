# Phase 18b — Cost and ops levers: async inserts, query cost, BACKWARD compat, live alert firing (PROPOSED)

Contract for the `phase-18b-cost-and-ops` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), findings 6 (full-refresh rollup) and 7 (operational blind spots: schema
compat `NONE`, un-merged parts as the real `FINAL` cost, no dollar/CPU cost, no live
alert firing). Split 2026-08-22 from `specs/phase-18-cost-and-ops.md` under the
CLAUDE.md phase-size rule (≤ ~6 pinned decisions / Done-when items per spec): this is
the second half — Done-when 3–7 and the pinned decisions they need, moved verbatim.
The first half is `specs/phase-18a-cost-and-ops.md`. Depends on Phase 18a merged (its
two alert rules are part of what Done-when 6 fires live; its `rollup_dirty` loader
write is where Done-when 3's async-insert flag lands — see the banner).

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
  && make cost-report && make test-int-long-delay
```

- `make cost-report` (new): per-query cost table from `system.query_log` for the
  report / restate / bench queries of this run, printed and written to
  `query_cost_daily`.
- `make test-alerts`: promtool proves the four existing rules plus 18a's two fire on
  captured fixtures and stay silent on tiny (unchanged from 18a; Done-when 6 is the
  LIVE firing of the same rules).
- *Carried from the original: the chain needs `make lake-reset PROFILE=long_delay
  CONFIRM=yes` after `make down` and `PROFILE=long_delay` on `make run` — banner
  item 3; fixed in the branch's commit 1, not here.*

## Done-when

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
- **Pushgateway for batch stages.** The stages are finite drains (ARCHITECTURE §8), so
  pull-scrape has nothing to scrape after exit; Pushgateway is the standard answer for
  batch jobs. Per-run gateway reset is part of `make run`.
- **Dollars are illustrative, CPU-seconds are measured.** The `$` column is a labelled
  conversion of a measured quantity, never presented as a billed figure.
- **`agent_ro` grants unchanged.** The scraper and cost writer use a separate user; the
  agent's SELECT-only user gains no new grants.

## Scope (files)

- `streaming/sink.py` (async-insert flag — since Phase 17: the loader's
  `client.insert`, `lake/load_serving.py`; banner), `clickhouse/` DDL + migration
  (`query_cost_daily`), `clickhouse/users` (cost writer user), `queries/cost_report.py`
  (reuses the public canonicalization module 18a graduated from `bench.py`),
  `observability/` (Pushgateway in compose + prometheus.yml, Grafana cost panel),
  `producer/schemas.py` (BACKWARD) + test, `agent/webhook.py` (groupKey dedupe),
  `Makefile`, `.github/workflows` (Pushgateway in the integration job), SCALING.md,
  ARCHITECTURE §3.3/§8, RESULTS.md cost table, BACKLOG (close the rows this half
  owns), DECISIONS Phase 18b, PHASES.md, CLAUDE.md.

## Review & stack risk

- **code-reviewer** (mandatory): serving-row byte-identity under every lever; no
  magnitude pins.
- **security-reviewer** (mandatory — compose service, ClickHouse users, CI workflow,
  webhook handler change): Pushgateway loopback-only; new writer user scoped to its
  table; `log_comment` tags never carry user-influenced text; webhook payload text
  still never reaches the LLM.
- **functionality-tester**: DONE command; alert fires live; BACKWARD rejects a
  breaking change.
- **coherence-auditor** at exit: every "async inserts not built" / "no evolution story"
  sentence is found and updated; diffs the Record-updates list against the actual diff.
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
rewritten — that rewrite is this branch's commit 1 (CLAUDE.md Workflow rules:
spec-reconciliation amendment first, stop for approval, no implementation before
it is approved). The amendment must resolve the items of the BACKLOG row "Phase 18
spec needs a Phase-17 follow-up edit BEFORE its branch opens (Phase-17 coherence
audit D2)" (cited by TITLE — BACKLOG line numbers shift, Phase-19 audit D-b) that
fall in this half:

- **`streaming/sink.py` → the lake landing step.** Done-when 1 and 3 name the
  hot-path ClickHouse sink, deleted in Phase 17. The engine lands to the lake
  (`lake/land_*`); the ONE serving-table writer is the Dagster load
  (`lake/load_serving.py`). The "Phase 17 is NOT a dependency" sentence is false.
- **DONE command gains `lake-reset` + `PROFILE=long_delay`.** As written (`make
  down && make up && make seed PROFILE=long_delay && make run`) it no longer runs:
  it needs `make lake-reset PROFILE=long_delay CONFIRM=yes` after `make down` and
  `PROFILE=long_delay` on `make run` (the engine binds its lake from `--profile`).
- **"cost writer user" → reuses `metrics_ro`.** Phase 18a creates ONE SELECT-only
  ClickHouse user, `metrics_ro` (`clickhouse/users.d/metrics-ro.xml`); this spec's
  Scope line "`clickhouse/users` (cost writer user)" and the pinned decision
  "The scraper and cost writer use a separate user" must be rewritten to reuse it —
  no second user (Phase-18a amendment, 2026-08-22).


The amendment also adds the three `specs/TEMPLATE.md` sections this spec predates:
Evidence, Record updates, Threat model (every new target here takes a variable).
