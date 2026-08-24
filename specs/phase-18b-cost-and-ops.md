# Phase 18b — Cost and ops levers: async inserts, query cost, BACKWARD compat, live alert firing (RECONCILED)

Contract for the `phase-18b-cost-and-ops` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), findings 6 (full-refresh rollup) and 7 (operational blind spots: schema
compat `NONE`, un-merged parts as the real `FINAL` cost, no dollar/CPU cost, no live
alert firing). Split 2026-08-22 from `specs/phase-18-cost-and-ops.md` under the
CLAUDE.md phase-size rule (≤ ~6 pinned decisions / Done-when items per spec): this is
the second half — the pre-split Done-when 3–7 and the pinned decisions they need, moved
verbatim and here **renumbered 1–5**. The first half is `specs/phase-18a-cost-and-ops.md`.
Depends on **Phase 18a merged** (PR #38, 2026-08-23): the ONE new alert rule it ships
(`PartCountHigh`) is one of the five rules Done-when 4 fires live, its terminal storage
registry (`observability/ch_scrape.py`) is a Pushgateway push source, and its
loader-owned dirty-set write in `lake/load_serving.py` is where Done-when 1's
async-insert flag lands (see the reconciliation record). Also depends on Phase 17 (the
engine's ClickHouse sink is gone; the ONE serving-table writer is `lake/load_serving.py`).

**Status: RECONCILED 2026-08-23 against main @ `52ff2a2`.** Do not start implementation
until the developer approves this amendment (commit 1 of the branch; CLAUDE.md Workflow
rules). No new dependencies. Each item is a measured before/after in the `bench.py` /
`measure_levers.py` style (Phase 7/13): a direction assert, never a magnitude pin.

## Why

The architecture review's sharpest operational criticism: the repo measures *rows and
bytes read*, never *cost over time*; the rollup recomputes everything on every refresh;
`FINAL`'s real price (un-merged parts) was filed as a benchmark quirk rather than the
operating cost it is; and the schema registry runs at compatibility `NONE`, which is the
opposite of a data contract. Each is a small, ClickHouse/Kafka-specific change with a
number attached — the "made it measurably cheaper, and why" story, told four ways.
(18a shipped the incremental rollup and the storage metric; 18b ships the remaining four.)

## The central constraint

**Every lever ships with its before/after and its guard.** A lever without a measured
delta is a claim; a lever without an alert or test that catches regression is a one-
off. Serving-row content is byte-identical before and after every lever (6dp row-
equality, the Phase-13 harness) — cost changes, answers don't.

## DONE command

```
make test && make lint && make test-alerts \
  && make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up \
  && make seed PROFILE=long_delay && make run PROFILE=long_delay \
  && make cost-report PROFILE=long_delay && make test-int-long-delay
```

- `make cost-report PROFILE=<p>` (new): per-query cost table from `system.query_log` for
  the report / restate / bench queries of this run, printed and written to
  `query_cost_daily` by the new `cost_rw` writer; reuses `queries/bench_common.py`'s
  public `canonicalize` / `measure` (the `settings=` seam carries the `log_comment` tag).
  Rewrites the "Cost per report query" block in `docs/RESULTS.md` under its own marker.
- `make test-alerts`: promtool proves the four Phase-≤17 workload rules plus 18a's ONE
  new rule (`PartCountHigh`) — **five rules total** — across BOTH fixture files
  (`alerts_test.yml`, real captures; `alerts_synthetic_test.yml`, the one synthetic
  input). Unchanged from 18a; Done-when 4 is the LIVE firing of the same five rules.
- `make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up && make seed
  PROFILE=long_delay && make run PROFILE=long_delay` — a clean stack AND a clean lake
  (the lake outlives `make down`); `make run PROFILE=long_delay` is the reconcile-bearing
  chain, so at least one campaign is restated — which is what Done-when 4's live
  `RestatementMagnitude` firing needs.
- `make test-int-long-delay` — the Phase-6 live reconciliation proof still passes on its
  own clean stack, plus 18b's live pins (the async-insert byte-identity, the live
  Alertmanager firing, the webhook `groupKey` dedupe). The engine binds its lake from
  `--profile`; the async flag is on in `make run` and off in the gate-0 golden path until
  the parity check passes.

`make check-docs` runs in CI's lint job and must stay green (the async-insert re-wording,
the cost-table block, the "five rules" count, and the BACKWARD caveat drop all pass
through it).

## Done-when

1. **Async inserts, measured.** The ONE serving-table writer's insert calls
   (`lake/load_serving.py` — `insert_attributed` / `insert_exposures`; the hot-path
   `streaming/sink.py` was deleted in Phase 17) gain an `async_insert=1,
   wait_for_async_insert=1` mode behind a flag (default on in `make run`, off in the
   gate-0 golden path until the parity check passes). Measured on `bench_large`: parts
   created per minute and insert wall-clock, before/after; direction assert (fewer parts).
   Serving rows byte-identical (Invariants 1–2). The stale "async inserts are a SCALING
   lever, not built" lines in `lake/load_serving.py`, `docs/SCALING.md` and
   `docs/ARCHITECTURE.md` §3.3 are updated to "built on the loader, measured, default on".
   *Evidence: Evidence rows 1a–1c.*
2. **Query cost in a unit the business reads.** `query_cost_daily` (ReplacingMergeTree
   keyed `(day, query_tag)`, version data-derived) is filled from `system.query_log`:
   `query_duration_ms`, `read_rows`, `read_bytes`, `memory_usage`, and a derived
   `cpu_seconds` (`ProfileEvents['OSCPUVirtualTimeMicroseconds']`). Every report / restate
   / bench query is tagged via the `log_comment` setting (through `bench_common.measure`'s
   `settings=` seam) so the table is per-query, not per-session (Invariant 4). A Grafana
   panel shows cost per query per day; `docs/RESULTS.md` gains a "Cost per report query"
   table with this run's measured values under its own `make`-generated marker. A
   documented `$/cpu-second` constant (clearly labelled illustrative, ClickHouse-Cloud
   style, not a measurement) turns cpu_seconds into dollars; the constant is a config
   value, never hard-coded in SQL (Invariant 5). The write goes through a NEW `cost_rw`
   user (Done-when's superseded "reuse `metrics_ro`" — see the reconciliation record).
   *Evidence: Evidence rows 2a–2c.*
3. **Schema compatibility BACKWARD, proven.** `producer/schemas.py` sets every subject to
   `BACKWARD` (dropping the per-subject `NONE`). Proof: a test adds an optional field to
   `Exposure` (`creative_id: str | None = None`), registers it, and asserts (a) the
   registry accepts it, (b) the Phase-1 golden fixtures (old schema) still validate
   against the consumer, (c) removing a required field is REJECTED (409) (Invariant 6).
   The field is test-only (removed after the test) unless the developer chooses to keep
   it; if kept, fixtures do NOT change because the field is optional with a null default.
   `docs/ARCHITECTURE.md` §3.3 "Redpanda" and the `schemas.py` docstring drop the "no
   evolution story yet" caveat. *Evidence: Evidence row 3.*
4. **Live alert firing path.** Each stage pushes its terminal registry (including 18a's
   `observability/ch_scrape.py` storage scrape) to a Prometheus Pushgateway (compose
   service, digest-pinned, loopback-bound like the other five), which persists the push
   after the batch stage exits, so Prometheus scrapes it and the **five** rules evaluate
   on live data — closing the "batch stages exit before a scrape" gap every prior phase
   deferred. Per-run gateway reset is part of `make run`. The AM webhook receiver is wired
   to `agent/webhook.py`. **What each layer proves (amended, review-round 0 — the shipped
   `for: 5m` makes a real-data AM-firing assertion a ~5.5-min wall-clock wait, rejected as
   slow + a clock-timed flake vector; retuning `for:` on the shipped rules is out of scope
   here):**
   - **LIVE (`make test-int-long-delay`):** a `long_delay` run's pushed reconcile registry
     makes `RestatementMagnitude` **ACTIVE (pending/firing) in Prometheus** within a
     bounded poll — proving push → scrape → evaluate on real data. Separately, a
     **synthetic** firing alert POSTed to Alertmanager's API is delivered **AM → agent**
     and `agent/webhook.py` records receipt (bounded poll) — proving the receiver is
     wired, the "alerts fire but never reach the agent" bug the integration exists to
     catch. Labelled synthetic (18a's `alerts_synthetic_test.yml` precedent for the case
     real data cannot drive the leg fast).
   - **PROMTOOL (`make test-alerts`):** the rule FIRES with `for: 5m` timing, deterministic
     via `eval_time` — not re-proven with a wall clock.
   - **CONFIG:** the AM webhook receiver → agent URL is present (asserted against
     `observability/alertmanager.yml`).

   No "live end-to-end 5-minute firing" claim anywhere. Webhook amplification (BACKLOG):
   the handler dedupes by `groupKey` per sweep — one sweep per alert group, not per alert
   (Invariant 7); the trigger-only LLM boundary is unchanged (Invariant 8). Closes two
   BACKLOG rows ("Live Alertmanager firing path", "Webhook sweep amplification").
   *Evidence: Evidence rows
   4a–4c.*
5. **Shard key chosen and defended.** `docs/SCALING.md`'s 500k tier names `household_id`
   as the shard key (corrections and dedup shard-local; reports aggregate via
   `Distributed`) and records why not `campaign_id`. Docs-only; no cluster is run.
   *Evidence: Evidence row 5.*

(Five items, within the ≤ ~6 rule.)

## Evidence (REQUIRED)

Every Done-when item names the test or command output that proves it.

| Done-when | Proof (test file / `make` target / command output) |
|---|---|
| 1a (async flag on the loader, off by default) | `tests/test_load_serving.py::test_async_flag_defaults_off_and_make_run_enables_it`, `::test_the_insert_settings_carry_async_insert_and_wait` |
| 1b (serving rows byte-identical with async on vs off) | LIVE `tests/integration/test_async_insert.py::test_serving_rows_are_byte_identical_with_async_on_and_off` (and equal to `tests/oracle.py`) |
| 1c (fewer parts, measured) | `make bench` / the `bench_large` measurement line "parts/min async < sync" (direction assert; magnitude printed, never pinned) |
| 2a (per-query cost written to the table) | `make cost-report PROFILE=long_delay` output line + LIVE `tests/integration/test_cost_report.py::test_each_measured_query_lands_one_row_keyed_by_its_tag` |
| 2b (cpu_seconds and dollars derived, not literal) | `tests/test_cost_report.py::test_cpu_seconds_is_the_profileevents_value`, `::test_dollars_come_from_the_config_rate_not_a_hardcoded_sql_literal` |
| 2c (the cost table is quarantined non-determinism) | `tests/test_cost_report.py::test_no_pipeline_path_reads_query_cost_daily`; LIVE `tests/integration/test_cost_rw.py` (the `cost_rw` principal has exactly `SELECT ON system.query_log` + `INSERT INTO query_cost_daily`, every other write ACCESS_DENIED — the `metrics_ro` mirror) |
| 3 (BACKWARD accepts optional, rejects breaking) | LIVE `tests/integration/test_schema_compat.py::test_backward_accepts_an_optional_field_and_old_fixtures_still_validate`, `::test_backward_rejects_removing_a_required_field` (409) |
| 4a (live firing path) | LIVE under `make test-int-long-delay`: `tests/integration/test_live_firing.py::test_pushed_reconcile_metric_makes_restatementmagnitude_active_in_prometheus` (real data: push→scrape→evaluate) and `::test_a_synthetic_firing_alert_is_delivered_from_alertmanager_to_the_agent` (AM→agent receipt). PROMTOOL firing+timing: `make test-alerts` (`RestatementMagnitude` fires with `for: 5m` via `eval_time`). CONFIG: `tests/test_alertmanager_config.py::test_the_webhook_receiver_points_at_the_agent` |
| 4b (webhook dedupe by group) | `tests/test_webhook.py::test_one_sweep_per_group_key_not_per_alert`, `::test_duplicate_alerts_in_one_group_trigger_one_sweep` |
| 4c (LLM boundary intact) | `tests/test_webhook.py::test_alert_text_never_enters_the_sweep_context`; pipeline output byte-identical with the agent disabled (existing determinism pin) |
| 5 (shard key defended) | `docs/SCALING.md` 500k tier names `household_id` + the why-not-`campaign_id` paragraph; `make check-docs` green (link/anchor + first-screen copy) |
| threat model | `tests/test_makefile.py` — the `cost-report` rows of the table below |

The same table, filled with the actual run's output, is item 2 of the "Before
reporting DONE" checklist (CLAUDE.md Workflow rules).

## Invariants (REQUIRED)

Properties, not mechanisms (specs/TEMPLATE.md): stated before any pinned decision names a
mechanism; a pinned decision names a mechanism only by reference to the invariant it
satisfies.

| Invariant ("for all …, … holds") | Falsified by (scenario test) |
|---|---|
| 1. For every lever this phase adds (async insert on/off, per-query cost tagging), the serving-table row content is identical to a run without it — the lever changes cost, never an answer (6dp row-equality, Phase-13 harness; equal to `tests/oracle.py`). | LIVE `tests/integration/test_async_insert.py::test_serving_rows_are_byte_identical_with_async_on_and_off` |
| 2. For every async insert, `wait_for_async_insert=1` holds: the call returns only after its rows are flushed and queryable, so a read issued immediately after a day's load sees every row — no async-buffer race. | LIVE `tests/integration/test_async_insert.py::test_a_read_right_after_a_load_sees_every_row` |
| 3. For every measured cost, `query_cost_daily` is OUTSIDE the byte-identical guarantee (durations vary run to run, like Iceberg metadata / Dagster ids) AND no serving / report / reconcile path reads it — so no pipeline answer depends on a measured cost. | `tests/test_cost_report.py::test_no_pipeline_path_reads_query_cost_daily` (import/read guard over `queries/`, `reconcile/`, `orchestration/`, `lake/`) |
| 4. For every measured query, it carries a distinct `log_comment` tag, so each `query_cost_daily` row aggregates exactly one query's `system.query_log` rows, never a whole session's. | LIVE `tests/integration/test_cost_report.py::test_each_measured_query_lands_one_row_keyed_by_its_tag`; `tests/test_cost_report.py::test_every_measured_query_is_tagged` |
| 5. For every dollar figure, `$ = cpu_seconds × $/cpu-second` with the rate read from a config constant, never a SQL literal, and cpu_seconds is the measured `ProfileEvents` value — the `$` column is a labelled conversion of a measurement, never a billed figure. | `tests/test_cost_report.py::test_dollars_come_from_the_config_rate_not_a_hardcoded_sql_literal`, `::test_cpu_seconds_is_the_profileevents_value` |
| 6. For every subject, compatibility is BACKWARD: adding an optional field is accepted and an old-schema consumer still validates the Phase-1 golden fixtures; removing a required field is rejected (409). | LIVE `tests/integration/test_schema_compat.py::test_backward_accepts_an_optional_field_and_old_fixtures_still_validate`, `::test_backward_rejects_removing_a_required_field` |
| 7. For any webhook payload, the agent triggers exactly one sweep per distinct `groupKey`, regardless of how many (duplicate / flapping) alerts it carries — amplification is bounded by group, never by alert count. | `tests/test_webhook.py::test_one_sweep_per_group_key_not_per_alert`, `::test_duplicate_alerts_in_one_group_trigger_one_sweep` |
| 8. For every alert delivered (mocked or live), the sweep re-observes ClickHouse and NO alert-body text reaches the LLM prompt — the alert is a trigger only, and pipeline output with the agent disabled is byte-identical. | `tests/test_webhook.py::test_alert_text_never_enters_the_sweep_context` |

```mutations
agent/webhook.py::_dedupe_by_group_key        constant-return:[]
agent/webhook.py::alerts                       delete-call
queries/cost_report.py::cpu_seconds            constant-return:0.0
queries/cost_report.py::to_dollars             constant-return:0.0
producer/schemas.py::_compat_level             constant-return:"NONE"
```

Coverage notes (why these lines and not others):
- `swap-sort-key` is not used: no pipeline function this phase touches sorts with a key
  lambda; `query_cost_daily`'s ordering is a DDL sort key, not a Python `sorted(…)`.
- Invariants 1, 2 (async byte-identity / no-race), 4's LIVE half and 6 (BACKWARD
  registration) are SQL / ClickHouse-server / schema-registry properties with no offline
  kill — the offline sweep would SURVIVE them by construction. They are carried by their
  LIVE Evidence rows under `make test-int-long-delay`, not by this block (the 18a
  precedent for its invariant 3).
- `producer/schemas.py::_compat_level constant-return:"NONE"` is the offline sentinel for
  Done-when 3's mechanism (the default flips `NONE` → `BACKWARD`): a unit pin asserts the
  posted level, so reverting the constant is KILLED without a live registry. If the
  implementation inlines the level rather than naming a function, the line is retargeted
  to that symbol at review-round 1, not dropped.
- `queries/cost_report.py::to_dollars` and `::cpu_seconds` are pure derivations with
  offline unit pins (Evidence 2b); the tagging + write path is LIVE (Evidence 2a/2c).

## Pinned decisions (do not re-litigate)

- **Direction asserts, never magnitude pins** (Phase 7/13 precedent). Cost numbers are
  reported as measured and regenerated by the targets; tests assert `<`, not a value.
  (Satisfies the central constraint.)
- **Async inserts on the LOADER, behind a flag, byte-identity proven.** The flag lives on
  `lake/load_serving.py`'s `client.insert` calls (Phase-17: the ONE serving-table writer,
  not the deleted `streaming/sink.py`), default on in `make run`, off in the gate-0
  golden path until the parity check passes. `wait_for_async_insert=1` keeps the read
  path synchronous-from-the-caller so no row is eventually-visible. (Satisfies invariants
  1, 2.)
- **Pushgateway for batch stages.** The stages are finite drains (ARCHITECTURE §8), so
  pull-scrape has nothing to scrape after exit; Pushgateway is the standard answer for
  batch jobs. Per-run gateway reset is part of `make run`. 18a's `ch_scrape.py` terminal
  registry is one of the push sources. Rejected: node_exporter textfile collector (a
  second file-drop convention for the same job). (Satisfies Done-when 4.)
- **Dollars are illustrative, CPU-seconds are measured.** The `$` column is a labelled
  conversion of a measured quantity (`ProfileEvents['OSCPUVirtualTimeMicroseconds']`),
  the rate a config constant, never presented as a billed figure or a SQL literal.
  (Satisfies invariant 5.)
- **A NEW `cost_rw` writer, not `metrics_ro`, and `agent_ro` grants unchanged.** Done-when
  2 needs `SELECT ON system.query_log` AND `INSERT INTO query_cost_daily`; 18a's
  `metrics_ro` is a read-only metadata principal (SELECT on `system.parts` /
  `system.merges` + SHOW TABLES on five named tables). Reusing it would widen a principal
  18a pinned as "a second principal, not a wider first one". 18b declares `cost_rw` in
  `clickhouse/users.d/cost-rw.xml` with exactly those two grants, reconstructed at every
  container start exactly like `metrics_ro` / `agent_ro`; any credential lives in `.env`
  only, never in CI. Supersedes the pre-split "reuse it" instruction (right while both
  consumers were readers; a writer gets its own). (Satisfies invariant 3's quarantine and
  the security surface.)
- **The dirty set / rollup / snapshot version story is 18a's and unchanged here.** 18b
  adds `query_cost_daily` and touches no serving-row write path except to flag the
  loader's insert; Done-when 1's parity check is the guard that it stayed a no-op on row
  content.

## Scope (files)

- `lake/load_serving.py` — the `async_insert=1, wait_for_async_insert=1` flag on
  `insert_attributed` / `insert_exposures` (NOT the deleted `streaming/sink.py`; banner).
- `clickhouse/ddl.sql` + migration — `query_cost_daily` (RMT, key `(day, query_tag)`,
  data-derived version); `clickhouse/users.d/cost-rw.xml` (the new writer principal).
- `queries/cost_report.py` (new) — reuses `queries/bench_common.py`'s public
  `canonicalize` / `measure` (the `settings=` seam carries `log_comment`); derives
  `cpu_seconds` and the illustrative `$`; writes `query_cost_daily` as `cost_rw`.
- `observability/` — Pushgateway service in `docker-compose.yml` (digest-pinned,
  loopback-bound) + `prometheus.yml` scrape config; per-run gateway reset; the push of
  each terminal registry incl. `ch_scrape.py`; a Grafana cost panel (JSON).
- `producer/schemas.py` (default `NONE` → `BACKWARD`) + the compat test.
- `agent/webhook.py` — `groupKey` dedupe (one sweep per group), trigger-only boundary
  unchanged.
- `Makefile` — `cost-report` target; the Pushgateway reset/push wired into `make run`.
- `.github/workflows/ci.yml` — Pushgateway in the integration job.
- Tests — `tests/test_load_serving.py`, `tests/test_cost_report.py`, `tests/test_webhook.py`,
  `tests/test_makefile.py`, and LIVE `tests/integration/test_async_insert.py`,
  `test_cost_report.py`, `test_cost_rw.py`, `test_schema_compat.py`, `test_live_firing.py`.
- Records — `docs/SCALING.md` (async lines + the 500k shard-key note), `docs/ARCHITECTURE.md`
  §3.3 (async + BACKWARD) / §8 (any `query_log` / `ProfileEvents` / Pushgateway surprise
  found live), `docs/RESULTS.md` (the cost-per-query block), `README.md` (cost-lever row +
  History), `BACKLOG.md` (close the two rows this half owns; re-defer Money-as-Float64),
  `DECISIONS.md` (Phase 18b), `docs/PHASES.md` (Phase 18b row), `CLAUDE.md` (Current
  status, Commands, metric-prefix list if a new prefix ships, users list),
  `scripts/check_docs.py` (any new `make` target / gauge / alert token) + `tests/test_check_docs.py`,
  `.env.example` (a commented `cost_rw` entry).

## Record updates (REQUIRED)

- [ ] `specs/phase-18b-cost-and-ops.md` — THIS amendment (commit 1: PROPOSED → RECONCILED,
      the six banner fixes, the four TEMPLATE sections). Listed here because commit 1 is
      the only commit that edits the spec itself.
- [ ] `DECISIONS.md` — Phase 18b entry: async on the loader behind a flag; Pushgateway for
      batch stages; the illustrative-`$` / measured-cpu-seconds split; the new `cost_rw`
      writer (and why not `metrics_ro`); BACKWARD as the data contract; webhook `groupKey`
      dedupe.
- [ ] `docs/PHASES.md` — Phase 18b row: Done-when as landed + a "Delivered" paragraph;
      status `PROPOSED` → built/merged.
- [ ] `CLAUDE.md` — Current status; Commands (`make cost-report`); the users list gains
      `cost_rw`; any new metric prefix ("Before reporting DONE" item 5); the alert-rule
      count where CLAUDE.md states it (five, live now).
- [ ] `docs/ARCHITECTURE.md` §3.3 (async inserts built on the loader; schema compatibility
      BACKWARD — drop "no evolution story yet") and §8 Gotchas (every `system.query_log`
      / `ProfileEvents` / Pushgateway surprise found live on the pinned 24.8 image).
- [ ] `docs/SCALING.md` — the async-insert lines change from "a scaling lever, not built"
      to "built on the loader, measured"; the 500k tier gains the `household_id` shard-key
      note + why-not-`campaign_id`.
- [ ] `docs/RESULTS.md` — a "Cost per report query" block regenerated by `make cost-report`
      under its own marker (the `cost-levers` / `scale-curve` pattern, so `make check-docs`
      guards it).
- [ ] `BACKLOG.md` — closed (strike-through + "DONE Phase 18b"): "Live Alertmanager firing
      path" and "Webhook sweep amplification". Re-deferred: "Money is stored as Float64
      end-to-end …" with the trigger **"Phase 20 candidate"** (18a set the trigger to
      "after 18b merges"). Note against the merge-lag row that it is still un-fireable
      (unchanged by 18b). Touch the "prometheus_client private `_value` accessor" row: its
      trigger fires here (Pushgateway re-reads every stage registry) — do the migration or
      re-defer with the reason.
- [ ] `README.md` — the cost-lever table gains one row (async inserts) and, if the
      cost-per-query number appears first-screen, its copy must match its block (checked by
      `make check-docs`); History row for 18b at exit.
- [ ] `.env.example` — a commented `cost_rw` credential entry (none in local dev, same
      posture as `metrics_ro` / `agent_ro`).
- [ ] `scripts/check_docs.py` + `tests/test_check_docs.py` — the `cost-report` target token
      and any new gauge/alert token in `TRACES`.
- [ ] Touched-beyond-Scope, recorded at exit (rather than left for the auditor): to be
      filled in the phase report — the "Before reporting DONE" item 6 list.

## Threat model (REQUIRED)

One new target, `make cost-report PROFILE=<p>`. It is NOT read-only: it INSERTs into
`query_cost_daily` (as `cost_rw`) and rewrites a `docs/RESULTS.md` block. It writes no
serving row and reads no truth link. It derives no path from `PROFILE` — the value is
validated and checked against the `eval_meta` marker (the `make eval` / `rollup-bench`
pattern) so the cost report refuses a database populated from a different profile. Same
shape as the destructive targets: ONE Python process (`uv run python -m queries.cost_report
--profile "$(PROFILE)"`), one recipe line, no Make-level guard interpolating a user value.
The `log_comment` tags are constants chosen by the query author (`report` / `restate` /
`bench:<name>`), never user- or payload-derived, so no untrusted text reaches
`system.query_log` (security review item). The Pushgateway service is a new surface:
digest-pinned image, published on `127.0.0.1` only (loopback, like the other five compose
services), reset per run.

| Target | empty | `../x` | `"; ` | env-exported | `$(origin)` on CONFIRM | Pinned by |
|---|---|---|---|---|---|---|
| `make cost-report PROFILE=` | `LakeRootUnset: profile '' is not [a-z0-9_]+`, exit 1, before ClickHouse is touched | refused by the same rule (`lake.iceberg_catalog.validate_profile`); no path is derived from `PROFILE` | reaches argv as ONE element (`--profile "$(PROFILE)"`, no shell re-split) and is then refused by the rule | same refusal — validation is in the process, not in Make; an env-origin `PROFILE='$(shell …)'` is the stated repo-wide residual (DECISIONS Phase 17) | n/a — no `CONFIRM`; the table is append-only and idempotent by RMT, nothing is deleted | `tests/test_makefile.py::test_cost_report_refuses_a_malformed_profile` (5 values), `::test_cost_report_is_one_python_process_with_a_quoted_profile`, `::test_cost_report_profile_from_the_environment_is_still_validated` |
| migration re-run | `clickhouse/apply.py` re-applies the `query_cost_daily` DDL with no error and no row change (`create … if not exists`) | — | — | — | — | `tests/test_cost_report.py::test_query_cost_daily_ddl_is_a_no_op_on_re_run` |

## Review & stack risk

- **code-reviewer** (mandatory): serving-row byte-identity under the async flag (the
  parity check is the guard); no magnitude pins; the cost table never read by a pipeline
  path; `log_comment` tags are constants.
- **security-reviewer** (mandatory — compose service, ClickHouse users, CI workflow,
  webhook handler change): Pushgateway loopback-only + digest-pinned; `cost_rw` scoped to
  exactly `SELECT ON system.query_log` + `INSERT INTO query_cost_daily`, `metrics_ro` /
  `agent_ro` unchanged; `log_comment` tags never carry user-influenced text; webhook
  payload text still never reaches the LLM (Invariant 8).
- **functionality-tester**: the DONE command; the async parity check; BACKWARD accepts an
  optional field and rejects a required-field removal (409); the alert fires live; the
  webhook dedupes by group.
- **coherence-auditor** at exit: every "async inserts not built" / "no evolution story" /
  "four alerts" sentence is found and updated; diffs the Record-updates list against the
  actual diff.
- Stack risk: `log_comment` and `ProfileEvents` column shapes differ across ClickHouse
  versions — verify on the pinned 24.8 image in the first hour; the Pushgateway image
  needs a digest pin like the other five; Alertmanager's evaluation/group intervals make
  a *fast, deterministic* live-firing assertion the hard part of Done-when 4 (poll the
  API with a bounded timeout, never a fixed sleep). STOP and report before any workaround;
  findings go under ARCHITECTURE §8.

## Out of scope (deferred, recorded)

- A ClickHouse cluster / `Distributed` tables — `docs/SCALING.md` tier note only (item 5).
- Continuous follow / stream framework — still the open question after Phase 17.
- Cost attribution per *advertiser* (multi-tenant chargeback) — needs a tenant dimension
  the event model does not have; recorded as a README "Next steps" item.
- Decimal64(4) money end-to-end — BACKLOG "Money is stored as Float64 end-to-end",
  re-deferred here (trigger: Phase 20 candidate, after this half merges).
- A merge-lag alert rule — still un-fireable (every settled capture reads
  `clickhouse_merge_backlog_seconds = 0`); BACKLOG row unchanged by this phase.

## Reconciliation record (commit 1, 2026-08-23)

This spec was written before Phase 17 and Phase 18a merged; its body had NOT been
rewritten. The amendment above resolves every item the BACKLOG row "Phase 18 spec needs a
Phase-17 follow-up edit BEFORE its branch opens (Phase-17 coherence audit D2)" placed in
this half, plus the Phase-18a corrections. What changed, and how:

- **`streaming/sink.py` → the Dagster loader.** Done-when 1 (was 3) named the deleted
  hot-path ClickHouse sink; the async flag now lands on `lake/load_serving.py`'s
  `insert_attributed` / `insert_exposures` (the ONE serving-table writer since Phase 17).
  The stale "Phase 17 is NOT a dependency" premise is deleted; the header states the
  opposite.
- **DONE command** gains `make lake-reset PROFILE=long_delay CONFIRM=yes` after `make
  down` and `PROFILE=long_delay` on `make run` and `make cost-report` (the engine binds
  its lake from `--profile`).
- **"four existing rules plus the two new ones" → FIVE rules.** Phase 18a shipped
  `PartCountHigh` only; the merge-lag rule was not shippable from a real capture (every
  settled capture reads `clickhouse_merge_backlog_seconds = 0`) and stays a BACKLOG row.
  Done-when 4 and the `make test-alerts` bullet say five rules, and 18a's synthetic
  fixture (`alerts_synthetic_test.yml`) is a second file the live firing path accounts
  for. The header's stale "two alert rules" dependency sentence is corrected to one.
- **A NEW `cost_rw` writer, not `metrics_ro`.** 18a's `metrics_ro` is a read-only metadata
  principal; Done-when 2 needs `SELECT ON system.query_log` + `INSERT INTO
  query_cost_daily`. Reusing it would widen a principal 18a pinned as "a second principal,
  not a wider first one". 18b creates `cost_rw` (`clickhouse/users.d/cost-rw.xml`) with
  exactly those two grants; `metrics_ro` / `agent_ro` stay unchanged. The pre-split
  "reuse it" instruction is superseded (right while both consumers were readers).
- **`observability/ch_scrape.py`** (18a's terminal storage registry) is named as one of
  the Pushgateway push sources for Done-when 4.
- **Done-when renumbered** 3–7 → 1–5.
- **TEMPLATE sections added**: Evidence, Record updates, Threat model, and — from commit 1,
  not backfilled (18a had to backfill its Invariants at review-round 1) — **Invariants**,
  written before the pinned decisions name mechanisms.
- **Citations**: BACKLOG rows are cited by TITLE, never by line number (line numbers shift
  — Phase-19 audit D-b).

## Amendment 2 — Done-when 4a live assertion (2026-08-23, architect ruling)

Done-when 4a originally read "a `long_delay` run produces a FIRING `RestatementMagnitude`
in Alertmanager's API". The shipped rule carries `for: 5m`, so a real-data AM-firing
assertion is a ~5.5-min wall-clock wait — rejected as slow AND a clock-timed flake vector
(the stack-risk line: never a fixed sleep / real-clock timing). Retuning `for:` on the
four shipped production rules to make a test fast was also rejected: whether a batch-pushed
*settled terminal* metric warrants the 5-minute transient-debounce is a real alert-semantics
question, but it is its own pinned decision for every consumer, not something to smuggle in
through a test-timing fix — out of scope here, shipped rule semantics unchanged.

The assertion is split by what each layer can prove deterministically and fast (Done-when 4
above): **LIVE** — `RestatementMagnitude` ACTIVE in Prometheus on the pushed real reconcile
metric (push→scrape→evaluate, the genuinely new capability every prior phase deferred) plus a
labelled **synthetic** firing alert delivered AM→`agent/webhook.py` (the mis-wired-receiver
bug); **PROMTOOL** — the rule fires with `for: 5m` timing via `eval_time`; **CONFIG** — the AM
webhook receiver points at the agent. Nothing is left to a 5.5-min timer. Recorded in
DECISIONS Phase 18b.
