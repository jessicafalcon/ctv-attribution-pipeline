# ROADMAP.md — ordered next work

The planned phases (0–19, 18a, 18b) are complete; next work is BACKLOG-driven
(CLAUDE.md → Current status). This file records the agreed order for what comes
next, so any session can pick up the work without re-deriving it. The ordering
rule is evidence per effort: close open measurements and convert
correct-by-construction arguments into pinned evidence before starting the next
phase-sized build. Phase 20 (Decimal money) stays the top *phase-sized*
candidate; the fix-sized items below land first.

**Do items 1–5 first, in this order.** Each item is its own branch, never
mixed; the usual review discipline applies. Items 6–8 each need an approved
Invariants-first spec before any code (Workflow rules). Tick the checkbox when
the item's PR merges.

## Items 1–5 — fix-sized, do first

- [x] **1. Flake fix** (BACKLOG: "test_second_pass_is_idempotent compares … by
  exact float equality"). `tests/integration/test_reconcile.py` compares
  restatement rows exactly; summation order over un-merged parts moves the last
  digit. Round to 6 dp as `tests/integration/test_lakehouse.py::_norm` does.
  Do NOT canonicalize (`OPTIMIZE … FINAL`) inside this test — an idempotency
  test should observe the table as the second pass left it. Test-only: no spec
  amendment. `fix/` branch.
- [x] **2. Agent-eval re-run** (BACKLOG: "Profiles whose deferred shared-IP
  conversions carry revenue now restate after `make run`"). Re-run
  `make agent-eval` (API tokens — ask first; since Phase 17 it resets each
  scenario's lake itself), then in `docs/RESULTS.md`: restore the two blanked
  `max|Δroas|` cells, replace the "measured Phase 10, pre-Phase-16" provenance
  banner with the new capture date, and re-verify the 30/30 and FP 0/10
  headline. README cites those numbers — if they move, surface the change,
  never smooth it. Capture + docs PR.
- [x] **3. Dirty-key READ measurement on `bench_large`** (BACKLOG: "The
  incremental rollup refresh's READ side is unmeasured on a multi-granule
  table"). Run the `make rollup-bench` measurement against `bench_large`
  (~7 granules of exposures) after `make run PROFILE=bench_large`. Record
  either outcome beside the Phase-13 cost levers: a read win, or another
  documented negative with its mechanism. Do NOT resize the profile (BACKLOG:
  producer queue cap ~100k messages). `fix/` branch. **DONE (`fix/rollup-bench-read`,
  2026-08-23):** documented negative — reads do not fall even at ~7 granules
  (135,168 rows both sides); the dirty set is 156 of 165 keys (shared-IP deferrals
  spread across every campaign/hour), so no granule can be skipped. Recorded in the
  RESULTS "Rollup refresh" block (now `bench_large`) + DECISIONS `fix/rollup-bench-read`.
- [x] **4. Poison-message disposition** (BACKLOG: "No stated disposition for a
  message that fails schema validation at consume time"; DECISIONS entry + one
  unit test — docs first, no dead-letter build, which would be speculative
  code).
  Nothing states what happens to a message that fails schema validation at
  consume time. Recommended disposition: fail loud and halt the pass — a
  bounded, replayable drain must never skip-and-continue, because a silent
  skip breaks the byte-identical guarantee; a dead-letter topic is the
  continuous-follow answer and should be noted as such. Pin with a unit test
  feeding `common.kafka` drain's decode path a malformed payload. If current
  behavior turns out to be skip-or-inscrutable-crash, that is a finding to
  report first (a write-path behavior change takes the fix-amendment ritual).
  **DONE (`fix/poison-message-disposition`, 2026-08-24):** STEP-0 finding —
  current behavior already fails loud (the three decode sites are bare
  `model_validate_json` comprehensions, no try/except; a malformed payload
  raises `ValidationError` and halts, never skips), so this was docs + one test,
  NO code change. Disposition recorded in DECISIONS (fail loud and halt; DLQ
  noted not built); `tests/test_poison_message.py` pins the loud halt. The loud
  failure is INSCRUTABLE (no topic/offset context) — left as an accepted
  limitation, deferred to the continuous-follow phase (a scrutability wrap is a
  write-path change → fix-amendment ritual).
- [x] **5. Crash-recovery proof** (BACKLOG: "The land → load seam's crash
  idempotency is argued, never demonstrated"; additive integration tests, no
  spec).
  The land → load seam is idempotent by construction (append-only lake,
  touched-day reloads, ReplacingMergeTree); nothing demonstrates it. Two
  cases: (a) land to the lake, deliberately skip the load (a crash between
  land and load), then load fresh — serving rows equal the uninterrupted
  oracle's; (b) drive `orchestration/run.py` load day-by-day, stop after a
  subset of touched days, re-run the full load — convergence. `fix/` branch.
  **DONE (`fix/crash-recovery-proof`, 2026-08-24):** STEP-0 finding — no
  production code change needed (`materialize_load` already takes an explicit
  touched-day set; the RMT collapses re-loads on FINAL). Two additive live tests
  in `tests/integration/test_lakehouse.py` under `make test-int-lakehouse`
  (`test_load_after_a_skipped_load_recovers_the_oracle_rows`,
  `test_load_resumed_after_a_partial_multi_day_load_converges`); each establishes
  the crash state by truncating the sanctioned `SERVING_TABLES` (as
  `make replay-serving` does — no new destructive path), then loads as the recovery
  and asserts 6dp equality to the uninterrupted oracle. DECISIONS
  `fix/crash-recovery-proof`.

## Items 6–8 — phase-sized, after 1–5

- [ ] **6. Two-partition parallelism demo** (small phase). SCALING.md claims
  parallelism = partition count with a household-keyed re-partition; nothing
  executes it. Minimal honest version: 2-partition topics, two engine drains
  (one per partition's household subset), merged landing byte-identical to the
  single-drain oracle. Invariant: for all partitionings of the input, merged
  output equals single-partition output. The spec must settle the conversions
  re-partition — conversions are keyed `device_id`, so a device's conversion
  can land apart from its household's exposures; making that shuffle visible
  is the point of the demo.
- [ ] **7. Phase 20 — Decimal money migration** (full phase; BACKLOG: "Money
  is stored as Float64" + the report/bench 6-dp-gate row). BACKWARD
  compatibility 409s an in-place retype of the required `spend`/`revenue`, so
  the shape is: add optional Decimal fields → dual-write → readers prefer
  Decimal when present → one signed-off fixture re-freeze → retire the floats
  later. Fold in the report.sql/bench.sql Decimal-sum row; afterwards check
  whether the 1-ulp restatement-metric mystery (BACKLOG) reproduces or
  vanishes — either result closes that row's "no established cause".
- [ ] **8. Continuous follow** (largest, last). The framework decision
  (Bytewax proper vs Flink) that the graph-refresh, offset-management, and
  TTL-dedup BACKLOG rows all park on. A cheaper middle step, if wanted before
  the framework port: an incremental-drain mode — real consumer group,
  committed offsets, repeated bounded passes — with the invariant that N
  incremental passes converge to the single-drain output.
