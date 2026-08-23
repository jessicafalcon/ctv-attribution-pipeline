# Phase 18a — Cost and ops levers: incremental rollup, dirty-set gate, part-count and merge-lag (PROPOSED)

Contract for the `phase-18a-cost-and-ops` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), findings 6 (full-refresh rollup) and 7 (operational blind spots: schema
compat `NONE`, un-merged parts as the real `FINAL` cost, no dollar/CPU cost, no live
alert firing). Split 2026-08-22 from `specs/phase-18-cost-and-ops.md` under the
CLAUDE.md phase-size rule (≤ ~6 pinned decisions / Done-when items per spec): this is
the first half — the pre-split Done-when 1–2 and the pinned decisions they need, moved
verbatim (the dirty-set gate promoted to its own item), plus the `report_snapshots`
version column pulled in by the amendment.
The second half is `specs/phase-18b-cost-and-ops.md`. Depends on Phase 19 (docs
reshape) merged — reordered 2026-08-22, DECISIONS "Process" — **and on Phase 17
merged**: the engine's ClickHouse sink is gone, the ONE serving-table writer is the
Dagster load `lake/load_serving.py`, and this phase's dirty set is written there.

**Status: RECONCILED 2026-08-22 against main @ `3362a53`.** Do not start
implementation until the developer approves this amendment (commit 1 of the branch;
CLAUDE.md Workflow rules). No new dependencies. Each item is a measured before/after
in the `bench.py` / `measure_levers.py` style (Phase 7/13): a direction assert, never
a magnitude pin.

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
  && make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up \
  && make seed PROFILE=long_delay && make run PROFILE=long_delay \
  && make rollup-bench PROFILE=long_delay && make test-int-long-delay
```

- `make test` — the offline suite: the marker/watermark idempotence pin, the
  `rollup_dirty` unit pins, the graduated-helper pins (`bench.py` output byte-identical
  after the move), the `make rollup-bench` threat-model pins in `tests/test_makefile.py`.
- `make lint` / `make test-alerts` — ruff; promtool proves the two new rules
  (`PartCountHigh`, `MergeBacklog`) fire on the recaptured fixtures and stay silent on
  tiny, alongside the four existing rules.
- `make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up && make seed
  PROFILE=long_delay && make run PROFILE=long_delay` — a clean stack AND a clean lake
  (the lake outlives `make down`; over a populated lake the reconcile candidates are the
  lake's current rows). `make run` here is the reconcile-bearing chain: at least one
  campaign is restated, which is what the dirty-set gate needs.
- `make rollup-bench PROFILE=long_delay` (new) — full refresh vs dirty-set refresh after
  that reconcile pass: rows read and rows written, direction assert (incremental < full),
  6dp equality of `campaign_hourly` FINAL rows, and the gate — `set(rollup_dirty keys) ==
  set(keys whose campaign_hourly rows differ between the pre- and post-refresh FULL
  rebuilds)`.
- `make test-int-long-delay` — the Phase-6 live reconciliation proof still passes on its
  own clean stack, plus this phase's live pins (the twin comparison and the forced-
  `OPTIMIZE` version pin on `report_snapshots`).

`make check-docs` runs in CI's lint job and must stay green (the RUNBOOK incident-1
re-frame, the graduated `_canonicalize` trace, and the two new alert names all pass
through it).

## Done-when

1. **Incremental rollup refresh, from a loader-owned dirty set.** The Dagster loader
   (`lake/load_serving.py` — the ONE serving-table writer since Phase 17, which already
   knows the rows it loaded) records the `(campaign_id, hour)` keys it touched in
   `rollup_dirty` (ReplacingMergeTree, key `(campaign_id, hour)`, version = `max`
   `processed_at` over the loaded rows for that key — data-derived, no wall clock).
   `refresh_campaign_hourly` recomputes only the keys whose `rollup_dirty` version is
   greater than the watermark in the one-row `rollup_refresh_marker`, then writes the new
   watermark; there are no deletes and no mutations, so a crash between the refresh and
   the marker write re-refreshes the same keys on the next pass (idempotent, never
   skipped). A `--full` flag keeps the current full rebuild as the oracle.
   `rollup-bench` asserts incremental == full (6dp) and incremental reads fewer rows.
   The `report_snapshots` write path is unchanged by the dirty set (both passes still
   snapshot every campaign; Done-when 4 is this phase's only change to that table), so
   the restatement view is unchanged in content. *Evidence: Evidence rows 1a–1d.*
2. **The dirty set is the loader↔rollup contract, and it is gated exactly.** After a
   reconcile pass that restates ≥ 1 campaign, `set((campaign_id, hour) in rollup_dirty)`
   **equals** `set(keys whose campaign_hourly rows differ between a pre-refresh and a
   post-refresh FULL rebuild)` — exact set equality, not counts, in both directions (a
   missing key is a silently stale rollup; an extra key is wasted work the full-refresh
   oracle can never see). *Evidence: Evidence row 2.*
3. **Part-count and merge-lag are first-class.** A one-shot scraper function
   (`observability/ch_scrape.py`, no daemon and no new compose service) reads
   `system.parts` / `system.merges` through a new SELECT-only ClickHouse user
   (`metrics_ro`) at the END of `make run`, `make run-hot` and `make metrics-capture`,
   and exports `clickhouse_active_parts{table}` and `clickhouse_merge_backlog_seconds`
   into that stage's terminal registry. Two new alert rules, `PartCountHigh` and
   `MergeBacklog`, with promtool fixtures recaptured from a real clean-stack run. RUNBOOK
   incident #1 ("the benchmark that lied") is re-framed: un-merged parts are the operating
   cost of `FINAL`; its "Would catch it next time" cell changes from **No alert covers
   this** to the new rules (the un-merged-part *condition* is now alerted; the benchmark
   harness's own `read_rows` still is not — the cell says which is which).
   `make check-docs` still passes. *Evidence: Evidence rows 3a–3c.*
4. **`report_snapshots` has a defined version column.** A migration adds
   `snapshot_version` = `max(processed_at)` over the rows the snapshot summarized —
   data-derived (no wall clock, no counter) and monotone across the two passes because
   reconciliation stamps `processed_at = reconciled_at`, strictly greater than the hot
   max — and makes it the ReplacingMergeTree version. **The sort key is unchanged**
   (`(reported_at, campaign_id, period)`): `make restate` must keep BOTH the pre- and
   post-reconciliation rows through `FINAL`, so the version disambiguates twins WITHIN a
   key, never collapses the pair. Twins on a fully-equal sort key can only come from a
   re-run, which the determinism pin makes byte-identical — equal versions over equal
   content is a defined choice (either row is the same row); the column declares the rule
   for the case the pin exists to exclude: if a re-run ever produced a differing twin, the
   later `processed_at` wins loudly instead of by merge timing. Closes the BACKLOG row
   "`report_snapshots` is a ReplacingMergeTree with NO version column". The migration is
   idempotent on re-run. *Evidence: Evidence rows 4a–4b.*

(Four items, within the ≤ ~6 rule. Items 1 and 2 were one clause in the pre-split
spec; the amendment splits them because the gate is a separate falsifiable contract —
the equality oracle passes while the dirty set is wrong.)

## Evidence (REQUIRED)

Every Done-when item names the test or command output that proves it.

| Done-when | Proof (test file / `make` target / command output) |
|---|---|
| 1a (dirty set written by the loader) | `tests/test_load_serving.py::test_loading_a_day_records_its_campaign_hour_keys` — the keys and the data-derived version of a loaded day, no wall clock |
| 1b (marker-driven, no deletes) | `tests/test_rollup_dirty.py::test_refresh_selects_only_keys_above_the_marker` and `::test_crash_before_the_marker_write_re_refreshes_the_same_keys` (the marker is written after the refresh; a second call with the marker unwritten recomputes the same key set) |
| 1c (incremental == full) | `make rollup-bench PROFILE=long_delay` output line "campaign_hourly FINAL rows identical (6dp): N keys" |
| 1d (incremental is cheaper) | same command, "rows read incremental < full" direction assert (magnitude printed, never pinned) |
| 2 (the gate) | `make rollup-bench PROFILE=long_delay` output line "dirty set == changed set (N keys)"; live pin `tests/integration/test_rollup_dirty.py::test_dirty_set_equals_the_changed_key_set` under `make test-int-long-delay` |
| 3a (metrics exist, from a real run) | `data/out/long_delay/metrics/*.prom` after `make metrics-capture PROFILE=long_delay` contains `clickhouse_active_parts` and `clickhouse_merge_backlog_seconds`; `tests/test_metrics.py::test_clickhouse_scrape_metrics_are_registered` |
| 3b (the rules fire and stay silent) | `make test-alerts` — promtool `test rules` over `observability/rules/tests/alerts_test.yml`: `PartCountHigh` + `MergeBacklog` fire on the long_delay fixture, silent on tiny |
| 3c (RUNBOOK re-frame traced) | `make check-docs` green with the two new alert names in `scripts/check_docs.py` `TRACES`; `tests/test_check_docs.py` |
| 4a (a real re-run: equal versions, equal rows) | existing `tests/integration/test_reconcile.py::test_second_pass_twins_are_byte_identical` still passes on the migrated table; `make restate` still shows both the pre- and post-reconciliation rows through `FINAL` |
| 4b (the later version wins a merge) | `tests/integration/test_snapshot_version.py::test_forced_optimize_keeps_the_later_snapshot_version` — insert two rows with an IDENTICAL sort key, different `snapshot_version` and different `revenue`, into a probe created `as report_snapshots` (structure AND engine copied, asserted via `engine_full`, so the DDL property is proven without touching a row the restatement pins read); `OPTIMIZE TABLE … FINAL`; assert the higher-version row survived. The shape is not reachable through the pipeline (a re-run is byte-identical) — the docstring says so, so nobody later reads it as a live failure mode |
| threat model | `tests/test_makefile.py` — the `rollup-bench` rows of the table below |
| helper graduation | `tests/test_cost_levers.py` + `queries/bench.py` output unchanged (`make bench` prints the same table); `make check-docs` (the RUNBOOK `_canonicalize` citation and the `TRACES` entry both point at `queries/bench_common.py`) |

The same table, filled with the actual run's output, is item 2 of the "Before
reporting DONE" checklist (CLAUDE.md Workflow rules).

## Pinned decisions (do not re-litigate)

- **Direction asserts, never magnitude pins** (Phase 7/13 precedent). Cost numbers are
  reported as measured and regenerated by the targets; tests assert `<`, not a value.
- **Dirty-set over bounded-lookback MV.** A refreshable MV with lookback can miss a
  60-day correction; the dirty set is exact and cheap. The full refresh stays as the
  equality oracle. Because the set is exact, there is no trailing lookback window —
  a lookback would only mask a wrong dirty set, which is what Done-when 2 exists to
  catch.
- **The dirty set is owned by the loader, not by the engine or the reconcile job.**
  The Phase-17 loader is the ONE writer of the serving tables and already knows the
  rows it loaded (Phase-17 D6), so it is the only place that cannot disagree with what
  ClickHouse holds. Rejected: recording keys in the engine and the reconcile job
  separately (two writers, two chances to drift, and neither sees a re-load).
- **Cleared by watermark, never by delete.** `rollup_refresh_marker` holds the
  `processed_at` watermark the last refresh covered; the refresh reads keys above it
  and writes the new marker afterwards. Rejected: deleting or mutating processed rows
  out of `rollup_dirty` (a ClickHouse mutation is asynchronous and unversioned — a
  crash between refresh and delete would silently skip keys, the one failure mode that
  is invisible to the full-refresh oracle).
- **`reconciled_at` stays anchored in ClickHouse `_max_ingest`** (Phase-17 coherence
  Q3, decided here). The anchor is `max(ingest_time)` over the fixed serving state, it
  is already data-derived and re-run-identical, and the dirty set does not depend on it.
  Moving the anchor into the lake would be a determinism-relevant change to the
  reconcile stamp for no gain in this phase; it is recorded as a BACKLOG row with the
  trigger that would force it.
- **One scraper user, `metrics_ro`, SELECT-only on `system.parts` / `system.merges`;
  `agent_ro` grants unchanged.** Declared in `clickhouse/users.d/metrics-ro.xml` and
  reconstructed at every container start, exactly like `agent_ro`; any credential lives
  in `.env` only, never in CI. Phase 18b reuses this user rather than adding a "cost
  writer user".
- **The snapshot version is a data-derived timestamp, not a pass counter.**
  `snapshot_version = max(processed_at)` over the summarized rows has a deterministic
  source and survives a replay; the BACKLOG row's phrasing ("a pass sequence number")
  would invent state with no source — a re-run restarts it at 1 and the twin choice is
  wrong in the one case the column exists for.
- **The scraper is a one-shot function, not a service.** Every stage here is a finite
  drain (ARCHITECTURE §8), so a scrape at the end of the run is the same shape as the
  existing terminal-registry dump; a daemon or a compose exporter would be a new
  always-on surface for a number that only matters at the end of a pass. (The live
  scrape path is Phase 18b's Pushgateway item.)

## Scope (files)

- `lake/load_serving.py` — dirty-key recording on the ONE serving-table writer.
- `reconcile/rollup.py` — marker-driven incremental refresh, `--full` oracle.
- `clickhouse/ddl.sql` + migration — `rollup_dirty`, `rollup_refresh_marker`,
  `report_snapshots` version column; `clickhouse/users.d/metrics-ro.xml`.
- `queries/bench_common.py` (new) — the graduated public `canonicalize` / `measure` /
  `round_row`, imported by `queries/bench.py`, `queries/measure_levers.py` and the new
  `queries/rollup_bench.py`; closes the BACKLOG row "Graduate `bench.py`'s
  `_canonicalize`/`_measure`/`_round_row` to a public shared harness module".
  `bench.py`'s printed output is byte-identical after the move.
- `queries/rollup_bench.py` (new) — full vs incremental, the equality oracle, the gate.
- `observability/ch_scrape.py` (new), `observability/rules/alerts.yml` (+2 rules),
  `observability/rules/tests/alerts_test.yml`, the recaptured
  `data/out/<profile>/metrics/*.prom` provenance (gitignored) via `make metrics-capture`.
- `scripts/check_docs.py` — `TRACES`: the two new alert names, and the `_canonicalize`
  trace moved from `queries/bench.py` to `queries/bench_common.py`; `tests/test_check_docs.py`.
- `Makefile` — `rollup-bench` target; the scraper line appended to `run`, `run-hot`,
  `metrics-capture`.
- Tests — `tests/test_rollup_dirty.py`, `tests/integration/test_rollup_dirty.py`,
  `tests/integration/test_snapshot_version.py`, `tests/test_makefile.py`,
  `tests/test_load_serving.py`, `tests/test_metrics.py`.
- Records — `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` §3.3/§8, `docs/RESULTS.md`,
  `README.md`, `BACKLOG.md`, `DECISIONS.md`, `docs/PHASES.md`, `CLAUDE.md`,
  `.env.example`.

## Record updates (REQUIRED)

- [ ] `DECISIONS.md` — Phase 18a entry: the loader-owned dirty set, the marker instead
      of deletes, `reconciled_at` kept in ClickHouse, the one-shot scraper and
      `metrics_ro`, the `report_snapshots` version column, the helper graduation.
- [ ] `docs/PHASES.md` — Phase 18a row: Done-when as landed + a "Delivered" paragraph;
      the 18b row's dependency sentence if anything moved between the halves.
- [ ] `CLAUDE.md` — Current status; Commands (`make rollup-bench`, and the scraper line
      now inside `run` / `run-hot` / `metrics-capture`); the Prometheus metric-prefix
      list gains `clickhouse_` (the scraper's prefix; "Before reporting DONE" item 5);
      Repo map if `observability/` gains a described file.
- [ ] `docs/ARCHITECTURE.md` §3.3 (the loader writes the dirty set; the rollup refresh
      is incremental) and §8 Gotchas (every `system.parts` / `system.merges` surprise
      found live on the pinned 24.8 image).
- [ ] `docs/RUNBOOK.md` — incident #1: un-merged parts re-framed as the operating cost
      of `FINAL`; the "Would catch it next time" cell names `PartCountHigh` /
      `MergeBacklog` and states what is still un-alerted; the `_canonicalize` citation
      moves to `queries/bench_common.py`; the alert-list preamble goes from four rules
      to six.
- [ ] `BACKLOG.md` — closed (strike-through + "DONE Phase 18a"): the loader-owned
      dirty-set row ("Phase 18 spec needs a Phase-17 follow-up edit …"), "Graduate
      `bench.py`'s `_canonicalize`/`_measure`/`_round_row` …", "`report_snapshots` is a
      ReplacingMergeTree with NO version column …" (closing note: "version = `max
      processed_at`, not a sequence; twin choice defined, content identical by pin").
      Re-deferred: "Money is stored as
      Float64 end-to-end …" with the trigger **"after 18b merges — Phase 20
      candidate"**. Opened: **`reconciled_at` anchors in ClickHouse `_max_ingest`, not
      in the lake** — trigger "the reconcile pass must run with ClickHouse unreachable".
- [ ] `docs/RESULTS.md` — a "Rollup refresh: full vs incremental" block regenerated by
      `make rollup-bench` under its own marker (the `cost-levers` / `scale-curve`
      pattern, so `make check-docs` guards it).
- [ ] `README.md` — the cost-lever table gains one row (incremental rollup refresh);
      the first-screen copy of any regenerated number must match its block (checked by
      `make check-docs`); History row for 18a at exit.
- [ ] `.env.example` — a commented `metrics_ro` entry if the user ever needs a
      credential (none in local dev, same posture as `agent_ro`).
- [ ] Spec amendments — none. 18b's own commit 1 fixes its "cost writer user" scope
      line to "reuses `metrics_ro`" (noted in its banner by this commit).

## Threat model (REQUIRED)

One new target, `make rollup-bench PROFILE=<p>`. It reads ClickHouse and writes a
`docs/RESULTS.md` block; it deletes nothing and derives no path from `PROFILE` — the
value is validated and checked against the `eval_meta` marker (the `make eval` pattern,
BACKLOG 43) so the bench refuses a database populated from a different profile. Same
shape as the destructive targets: ONE Python process (`uv run python -m
queries.rollup_bench --profile "$(PROFILE)"`), one recipe line, no Make-level guard
interpolating a user value.

| Target | empty | `../x` | `"; ` | env-exported | `$(origin)` on CONFIRM | Pinned by |
|---|---|---|---|---|---|---|
| `make rollup-bench PROFILE=` | the process refuses `""` (profile regex `[a-z0-9_]+`) and exits non-zero before touching ClickHouse | refused by the same regex; no path is derived from `PROFILE`, so there is nothing to escape | passed as one argv element (quoted `"$(PROFILE)"`, no shell re-split) and then refused by the regex | same refusal — the validation is in the process, not in Make, so origin does not change behaviour; an env-origin `PROFILE='$(shell …)'` remains the stated repo-wide residual (DECISIONS Phase 17) | n/a — no `CONFIRM`; the target is non-destructive | `tests/test_makefile.py::test_rollup_bench_refuses_a_malformed_profile`, `::test_rollup_bench_is_one_python_process` |
| `make rollup-bench … --full` | the flag is a fixed literal in the recipe/CLI, never a Make variable, so no user value reaches it; `--full` only *adds* the oracle rebuild — it cannot skip the equality assert | — | — | — | — | `tests/test_rollup_dirty.py::test_full_flag_is_the_oracle_not_a_bypass` |
| migration re-run | `clickhouse/apply.py` re-applies the `report_snapshots` version-column migration with no error and no row change (`create … if not exists` / guarded `alter`) | — | — | — | — | `tests/test_rollup_dirty.py::test_migration_is_idempotent_on_re_run` |

## Review & stack risk

- **code-reviewer** (mandatory): serving-row byte-identity under every lever; the
  marker/dirty-set path re-refreshes rather than skips after a crash; no magnitude
  pins; the helper graduation keeps `bench.py` output identical.
- **security-reviewer** (mandatory — ClickHouse users): `metrics_ro` SELECT-only on
  `system.parts` / `system.merges`; `agent_ro` unchanged; no credential in CI.
- **functionality-tester**: the DONE command; incremental == full on `long_delay`
  after a reconcile pass that restates ≥ 1 campaign; the dirty-set gate (Done-when 2)
  as a set comparison, not a count.
- **coherence-auditor** at exit: every "No alert covers this" / "four alerts" sentence
  is found and updated; diffs the Record-updates list against the actual diff.
- Stack risk: `system.parts` / `system.merges` column shapes on the pinned 24.8 image,
  and whether a ReplacingMergeTree version column can be added to the EXISTING
  `report_snapshots` by `alter` or needs a table rebuild (ClickHouse has no
  `alter table … modify engine`) — verify both in the first hour; STOP and report before
  any workaround, findings go under ARCHITECTURE §8.

## Out of scope (deferred, recorded)

- Async inserts, `query_cost_daily`, schema compatibility BACKWARD, the live alert
  firing path + webhook dedupe, the shard key note — `specs/phase-18b-cost-and-ops.md`.
- Decimal64(4) money end-to-end — BACKLOG "Money is stored as Float64 end-to-end",
  re-deferred here (trigger: after 18b merges — Phase 20 candidate).
- Moving the `reconciled_at` anchor into the lake — BACKLOG row opened here.
- Continuous follow / stream framework — still the open question after Phase 17.

## Reconciliation record (commit 1, 2026-08-22)

This spec was written before Phase 17 merged (PR #31). The amendment above resolves
every item the BACKLOG row "Phase 18 spec needs a Phase-17 follow-up edit BEFORE its
branch opens (Phase-17 coherence audit D2)" placed in this half, plus the Phase-19
items. What changed, and how:

- **`streaming/sink.py` → the Dagster loader.** Every mention now names
  `lake/load_serving.py`; the "Phase 17 is NOT a dependency" sentence is deleted and
  the header states the opposite.
- **Dirty set owned by the loader**, with the mechanism pinned: `rollup_dirty` (RMT,
  key `(campaign_id, hour)`, data-derived version) plus a one-row
  `rollup_refresh_marker` watermark, no deletes and no mutations.
- **DONE command** gains `make lake-reset PROFILE=long_delay CONFIRM=yes` after `make
  down` and `PROFILE=long_delay` on `make run`; `make rollup-bench` takes the profile.
- **The dirty-set gate** is promoted from a review note to Done-when 2, as exact set
  equality in both directions.
- **`reconciled_at`** is decided: it stays anchored in ClickHouse `_max_ingest`, with
  a BACKLOG row carrying the trigger that would move it.
- **Recapture procedure**: the fixture recapture runs off a clean stack AND a clean
  lake (the DONE command's chain), per Phase 17.
- **Phase-19 items**: `make check-runbook` → `make check-docs` throughout; Scope adds
  `scripts/check_docs.py` and `tests/test_check_docs.py` (the `_canonicalize` trace
  this phase moves, and the two alert names it adds).
- **Citations**: BACKLOG rows are cited by TITLE, never by line number (line numbers
  shift — Phase-19 audit D-b); every "Phase 18" that means this half reads "18a".
- **TEMPLATE sections added**: Evidence, Record updates, Threat model.
