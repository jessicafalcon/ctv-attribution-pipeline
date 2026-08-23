# Phase 18a — Cost and ops levers: incremental rollup, dirty-set gate, storage metrics (RECONCILED)

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
- `make lint` / `make test-alerts` — ruff; promtool proves the ONE new rule
  (`PartCountHigh`) stays silent on both recaptured real captures and fires on the
  labelled synthetic input, alongside the four existing workload rules. No merge-lag
  rule ships — see Done-when 3.
- `make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up && make seed
  PROFILE=long_delay && make run PROFILE=long_delay` — a clean stack AND a clean lake
  (the lake outlives `make down`; over a populated lake the reconcile candidates are the
  lake's current rows). `make run` here is the reconcile-bearing chain: at least one
  campaign is restated, which is what the dirty-set gate needs.
- `make rollup-bench PROFILE=long_delay` (new) — full refresh vs dirty-set refresh after
  that reconcile pass: 6dp equality of `campaign_hourly` FINAL rows, a direction assert
  on rows WRITTEN (rows read printed with the single-granule caveat), and the gate —
  every key whose `campaign_hourly` row differs between the pre- and post-refresh FULL
  rebuilds is in the dirty set above the watermark, with `|dirty − changed|` reported.
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
   `processed_at` over the loaded rows for that key — data-derived, no wall clock), and
   then REFRESHES those keys (`orchestration.run.materialize_load`), at offset 0 for the
   hot load and `RECONCILE_DELTA_MS` for the reconcile pass's reload — so the pipeline's
   own second pass is the incremental one, not a scenario a bench constructs. Still a
   batch step recomputing from source, never an insert-triggered summing MV.
   `refresh_campaign_hourly` recomputes exactly the keys whose `rollup_dirty` version
   DIFFERS from that key's row in `rollup_refreshed`, then stamps those keys with the
   versions it computed against. **Per key, never against a global maximum** — the first
   cut compared every key to one scalar watermark, which left 321 of 340 keys on
   long_delay permanently unrefreshed and served a stale rollup (review gate). There are
   no deletes and no mutations, so a crash between the refresh and the stamp re-refreshes
   the same keys on the next pass (idempotent, never skipped). A `--full` flag keeps the
   whole-table rebuild as the oracle.
   `rollup-bench` asserts incremental == full (6dp) and that the incremental refresh
   WRITES fewer rows (measured 19 vs 340 on `long_delay` — 18×, reported as measured,
   never pinned). Rows read are printed for both and NOT asserted: at profile scale the
   source tables are a single granule (`exposures_landed` 360 rows in 2 marks), so a
   dirty-key predicate has nothing to prune and the dirty-key lookup itself reads —
   measured 2,001 vs 1,310. A read-side win needs a multi-granule table (BACKLOG:
   bench_large, 18b's query-cost work); asserting it here would claim scale we do not
   run.
   The `report_snapshots` write path is unchanged by the dirty set (both passes still
   snapshot every campaign; Done-when 4 is this phase's only change to that table), so
   the restatement view is unchanged in content. *Evidence: Evidence rows 1a–1d.*
2. **The dirty set is the loader↔rollup contract, and it is gated.** The dirty set is
   the keys in `rollup_dirty` ABOVE the refresh watermark — the only set the refresh
   ever sees, so the only set the contract can be about. After a reconcile pass that
   restates ≥ 1 campaign, every key whose `campaign_hourly` row differs between a
   pre-refresh and a post-refresh FULL rebuild is IN that set (`changed ⊆ dirty`, the
   hard assert: a changed key the refresh would not recompute is the silent-wrong case
   the full-refresh oracle can never see). The cost assert is `len(dirty) < total keys`,
   and `rollup-bench` prints the over-refresh count `|dirty − changed|`. Equality is
   NOT asserted — a reload of a touched day re-records that day's exposure hours at the
   new version whether or not their aggregate moved, so the dirty set is a lawful
   superset; on `long_delay` the two sets are in fact equal (19 of 340 keys, 0
   over-refresh) and the run reports which held. *Evidence: Evidence row 2.*
3. **Storage is measured; ONE rule ships, on the server's own threshold.** A one-shot
   scraper function (`observability/ch_scrape.py`, no daemon and no new compose service)
   reads `system.parts` / `system.merges` through a new SELECT-only ClickHouse user
   (`metrics_ro`) at the END of `make run`, `make run-hot` and `make metrics-capture`,
   waits for the storage state to settle (no merge running, counts stable across two
   reads; raises on the cap rather than capturing a moving number) and exports
   `clickhouse_active_parts{table}`, `clickhouse_unmerged_parts{table}` and
   `clickhouse_merge_backlog_seconds`. **One** alert rule, `PartCountHigh`
   (`clickhouse_active_parts > 150`): the threshold is ClickHouse's own
   `parts_to_delay_insert` default, cited in the rule's annotation, because no
   threshold "between the profiles" exists — the real captures read 5 active parts max
   on BOTH tiny and long_delay (part count follows insert batching and merge timing,
   not event volume). Its SILENCE is proven by the two real captures and its FIRING by
   a synthetic promtool input (`alerts_synthetic_test.yml`, `active_parts=151` — the
   file name and header say synthetic). A merge-lag rule is NOT shipped: every settled
   capture reads `clickhouse_merge_backlog_seconds = 0`, so it could only be proven by
   an invented number (BACKLOG row; the metric ships regardless — measuring without
   alerting is true, alerting without a fireable measurement is not). RUNBOOK incident
   #1 is re-framed: the un-merged-part condition is now measured, and its "Would catch
   it next time" cell says plainly that `PartCountHigh` would NOT have caught this
   incident's 4 parts — the benchmark's `canonicalize` OPTIMIZE remains the guard.
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
| 1a (dirty set written by the loader) | `tests/test_rollup_dirty.py::test_the_loader_records_both_sides_of_a_days_keys`, `::test_the_loaders_versions_are_data_derived_not_wall_clock`, `::test_a_reload_of_the_same_day_records_the_same_versions` |
| 1b (per-key watermark, no deletes) | `tests/test_rollup_dirty.py::test_a_key_is_dirty_when_its_own_version_differs_not_when_it_beats_a_global_max`, `::test_a_version_that_moves_DOWN_still_marks_the_key_dirty`, `::test_refresh_selects_only_dirty_keys_and_binds_them`, `::test_crash_before_the_stamp_re_refreshes_the_same_keys`, `::test_the_dirty_set_is_never_deleted_or_mutated`, `::test_full_is_the_oracle_not_a_bypass` |
| 1c (incremental == full) | `make rollup-bench PROFILE=long_delay` output line "campaign_hourly FINAL rows identical (6dp): N keys" |
| 1d (incremental is cheaper) | same command, "rows read incremental < full" direction assert (magnitude printed, never pinned) |
| 2 (the gate) | LIVE under `make test-int-long-delay` (moved there from the bench — a contract proven only by a target CI never runs is proven nowhere): `tests/integration/test_rollup_dirty.py::test_every_changed_key_was_refreshed`, `::test_the_served_rollup_equals_a_full_rebuild`, `::test_the_pipeline_converges_to_nothing_dirty`, `::test_every_served_row_carries_a_pass_stamp` (6 tests). Also reported by `make rollup-bench PROFILE=long_delay`: "dirty set == changed set (19 keys)" / "over-refresh: 0 keys" |
| 3a (metrics from a real run, reproducibly) | `data/out/<p>/metrics/clickhouse.prom` after `make metrics-capture`; TWO full clean-stack tiny cycles produce byte-identical files (13 active / 11 unmerged); `tests/test_ch_scrape.py` (8 offline pins incl. the settle contract and "no wall clock in the registry") |
| 3b (the rule is silent on real captures, fires on the server's threshold) | `make test-alerts` — promtool over `alerts_test.yml` (both real captures: `PartCountHigh` silent) and `alerts_synthetic_test.yml` (`active_parts=151`: fires, annotation matched) |
| 3c (the principal is scoped, the re-frame traced) | `tests/integration/test_metrics_ro.py` — 7 ACCESS_DENIED cases + the scrape running end-to-end as `metrics_ro`; `make check-docs` green with `PartCountHigh` and both gauge names in `scripts/check_docs.py` `TRACES` |
| 4a (a real re-run: equal versions, equal rows) | existing `tests/integration/test_reconcile.py::test_second_pass_twins_are_byte_identical` still passes on the migrated table; `make restate` still shows both the pre- and post-reconciliation rows through `FINAL` |
| 4b (the later version wins a merge) | `tests/integration/test_snapshot_version.py::test_forced_optimize_keeps_the_later_snapshot_version` — insert two rows with an IDENTICAL sort key, different `snapshot_version` and different `revenue`, into a probe created `as report_snapshots` (structure AND engine copied, asserted via `engine_full`, so the DDL property is proven without touching a row the restatement pins read); `OPTIMIZE TABLE … FINAL`; assert the higher-version row survived. The shape is not reachable through the pipeline (a re-run is byte-identical) — the docstring says so, so nobody later reads it as a live failure mode |
| threat model | `tests/test_makefile.py` — the `rollup-bench` rows of the table below; the key-filter substitution is pinned by `tests/test_rollup_dirty.py::test_the_key_filter_is_applied_to_both_exposure_reads` |
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
- **Cleared by a PER-KEY stamp, never by delete and never by a global watermark.**
  `rollup_refreshed` holds one row per key: the version that key was last computed
  against. The refresh recomputes the keys whose `rollup_dirty` version differs and
  stamps exactly those versions afterwards. Rejected: one scalar watermark (shipped in
  the first cut and caught at the review gate — it left 321 of 340 keys on long_delay
  permanently below it, serving a stale rollup); deleting or mutating processed rows
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
- `clickhouse/ddl.sql` + migration — `rollup_dirty`, `rollup_refreshed`,
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
      states what is still un-alerted (and that `PartCountHigh` would NOT have caught
      incident #1); the `_canonicalize` citation
      moves to `queries/bench_common.py`; the alert-list preamble goes from four rules
      from four rules to five.
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
| `make rollup-bench PROFILE=` | `LakeRootUnset: profile '' is not [a-z0-9_]+`, exit 1, before ClickHouse is touched | refused by the same rule (`lake.iceberg_catalog.validate_profile`, the one every lake path uses); no path is derived from `PROFILE` here, so there is nothing to escape | reaches argv as ONE element (`--profile "$(PROFILE)"`, no shell re-split) and is then refused by the rule | same refusal — the validation is in the process, not in Make, so origin does not change behaviour; an env-origin `PROFILE='$(shell …)'` remains the stated repo-wide residual (DECISIONS Phase 17) | n/a — no `CONFIRM`; nothing is deleted (pinned by `::test_rollup_bench_recipe_has_no_delete_and_no_confirm`) | `tests/test_makefile.py::test_rollup_bench_refuses_a_malformed_profile` (5 values), `::test_rollup_bench_is_one_python_process_with_a_quoted_profile`, `::test_rollup_bench_profile_from_the_environment_is_still_validated` |
| `make rollup-bench … --full` | the flag is a fixed literal in the recipe/CLI, never a Make variable, so no user value reaches it; `--full` only *adds* the oracle rebuild — it cannot skip the equality assert | — | — | — | — | `tests/test_rollup_dirty.py::test_full_is_the_oracle_not_a_bypass` |
| migration re-run | `clickhouse/apply.py` re-applies the `report_snapshots` version-column migration with no error and no row change (`create … if not exists` / guarded `alter`) | — | — | — | — | `tests/test_snapshot_version.py::test_migration_is_a_no_op_once_the_table_is_versioned`, `tests/integration/test_snapshot_version.py::test_migration_preserves_every_row_and_is_a_no_op_on_the_second_run` |

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
  per-key `rollup_refreshed` stamp, no deletes and no mutations.
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
