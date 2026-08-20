# Phase 10 — Agent eval and the near-miss demo · CHECKPOINT

Contract for the `phase-10-agent-eval` branch. Source: `docs/PHASES.md` → Phase 10,
`docs/ARCHITECTURE.md` §4.3 (how it's proven — validate against ground truth: the
fault → top-hypothesis → correct? table, the false-positive controls, the near-miss
pair), and the Phase-9 forward-notes: `EVAL_REPS = 5` is defined in `agent/config.py`
and CONSUMED here; scoring must key an escalation on `verdict ==
AMBIGUOUS_NEEDS_HUMAN` as an ABSTENTION and never read the escalation-default
`top_hypothesis = upstream_data_change` as a diagnosis (DECISIONS Phase 9); FG2
(BACKLOG 31) pins each profile's live headline here; BACKLOG 26 (co-view adjusted
factor) hits its recorded HARD STOP here.

**Deliverables (all gated):** the **no-fault baseline** producer profile (entry
condition — it does not exist yet); an **eval harness** (`agent/eval/`) that runs
every scenario live, `EVAL_REPS` times, scores each finding against a pure,
offline-tested rubric, and renders the two tables; the **fault → top hypothesis →
correct? table with false-positive rate** and the **near-miss pair table** in
`docs/RESULTS.md`. This is a checkpoint: stopping here yields a coherent project
(the agent story, proven against ground truth).

## Design-review rulings settled with the developer (do not re-litigate)

**Ruling A — BACKLOG 26 (co-view adjusted factor) closes as a DECISIONS won't-do.**
Row 26 anchors the trigger to "the Phase-10 near-miss demo (real-lift vs shared-IP)."
That near-miss is a **device-graph / shared-IP discrimination** — it turns on
`ip_clusters.ip_resolved_fraction` and candidate counts, not on any genre number.
Walking Phase 10 end to end (baseline → 5-fault sweep → real_lift/shared_ip
near-miss), **nothing consumes a genre-adjusted advertiser number** — the exact HARD
STOP condition. Building the factor (Option 2) was rejected on the merits, not just
scope: the honest per-genre expected baseline does not exist in serving data
(DECISIONS Phase 8: the truth-side 2.5× skew collapses to a ~7% margin, sports 0.561
vs comedy 0.522 — noise-indistinguishable), so a positive `co_view_bug` diagnosis
would require feeding the expected baseline from the producer's co-view multiplier —
the reporting-reads-generation-params coupling row 26 forbids — or shipping a
7%-margin detector flaky by construction. Either "works" only because it was told the
answer, undercutting the determinism/isolation spine. **Consequence:** `co_view_bug`'s
correct outcome is **abstention** (`AMBIGUOUS_NEEDS_HUMAN`), exactly what the Phase-9
prompt already routes an unexplained genre skew to. Two record-fixes carried in the
won't-do (below).

**Ruling B — four scoring buckets, and the two abstentions are NOT the same thing.**
- **fault-recall** (agent must CONFIDENTLY name the right cause): `shared_ip_spike →
  DEVICE_GRAPH_MISMATCH`, `late_burst → LATE_ARRIVAL_DISTORTION`.
- **negative-confirmation** (the near-miss NEGATIVE half, `real_lift`): a genuine lift
  is NOT a wrong number — the number is right, just higher. From the frozen context the
  honest read is "healthy pipeline" (flat `ip_resolved_fraction`, no distortion signal),
  which Ruling E routes to abstention; there is **no baseline/vs-prior field**, so a
  CONFIDENT `REAL_PERFORMANCE_CHANGE` is structurally unreachable and would collide with
  Ruling E. Resolved on the rubric side (matching PHASES.md "DECLINES to fire
  device_graph_mismatch"): **correct = abstain OR confident `REAL_PERFORMANCE_CHANGE`**
  (the bonus, by elimination); **FAILURE = confidently firing `DEVICE_GRAPH_MISMATCH`**
  (the exact near-miss failure §4.3 warns of) **or any other fault**. Not a control,
  not in the FP-rate denominator.
- **capability-boundary** (abstention-expected, but for a *seeing* reason):
  `co_view_bug` — a real fault the agent CANNOT diagnose from serving data by design
  (Ruling A). Correct = abstain. Labeled a known capability boundary with the
  isolation reason stated; **not** in the false-positive-rate denominator.
- **control** (abstention-expected because nothing is wrong): `duplicate_flood`
  (dedup absorbs the flood; ClickHouse carries no fingerprint) and `no_fault_baseline`
  (healthy pipeline). Correct = abstain. **These two are the FP-rate denominator**
  (§4.3 "two controls the agent must correctly leave alone").

The developer's record-fix: **duplicate_flood abstains because nothing is wrong;
co_view_bug abstains because the fault is undiagnosable from serving data by design.**
Score both as correct-abstention, but the table + RESULTS must label them distinctly —
never conflate "found nothing" with "couldn't see it."

**Ruling C — per-rep outcome taxonomy (the pure rubric).** `verdict ==
AMBIGUOUS_NEEDS_HUMAN` is always read as ABSTENTION (never `top_hypothesis` — which on
an escalation is the neutral `upstream_data_change` default, DECISIONS Phase 9).
- fault-recall rep → `CORRECT_DIAGNOSIS` (CONFIDENT ∧ top == expected) ·
  `ABSTAINED` (AMBIGUOUS — over-cautious miss on a diagnosable fault) ·
  `WRONG_DIAGNOSIS` (CONFIDENT ∧ top ≠ expected — the dangerous case).
- negative-confirmation rep (`real_lift`) → `CORRECT_ABSTENTION` (AMBIGUOUS) ·
  `CORRECT_DIAGNOSIS` (CONFIDENT `REAL_PERFORMANCE_CHANGE`, the bonus) ·
  `WRONG_DIAGNOSIS` (any other CONFIDENT verdict — a CONFIDENT `DEVICE_GRAPH_MISMATCH`
  is the near-miss NEGATIVE-half failure).
- capability-boundary / control rep → `CORRECT_ABSTENTION` (AMBIGUOUS) ·
  `FALSE_POSITIVE` (CONFIDENT with any real-fault hypothesis).

**FP rate = FALSE_POSITIVE reps over the two controls ÷ control reps (2 × 5 = 10).**
`co_view_bug` fires are reported in its own row (capability boundary), not folded into
the headline FP rate.

**Ruling D — the agent is non-reproducible (Phase-9 Ruling C); the reps MEASURE
residual stability.** Temperature is unset on the Claude-5 family, so per-rep output
varies. The `EVAL_REPS = 5` reps per scenario are exactly why the tables are honest:
they report a rate (k/5 correct), not a single-run claim. Verdict/`top_hypothesis`
stability across reps is a measurement, never a gated assertion.

**Ruling E — no-fault abstain path is explicit in the prompt (minimal Phase-10
refinement).** The Phase-9 prompt asks the agent to decide whether a number is
"probably WRONG" but never blesses a clean-baseline outcome. The controls are a
first-class §4.3 requirement, so one sentence is added to `SYSTEM_PROMPT`: *if no
signal indicates a probably-wrong number, do not invent a fault — submit
`AMBIGUOUS_NEEDS_HUMAN`.* Recorded in DECISIONS; the cached prefix changes byte-value
but stays stable within Phase 10 (Ruling B caching discipline intact). This is the
only agent-behavior change this phase.

## The no-fault baseline profile (entry condition)

`producer/profiles/no_fault_baseline.json`. A healthy pipeline, one fresh seed, every
fault knob at its non-fault setting: baseline `caused_conversion_rate` (0.2),
`shared_ip_fraction 0.1`, delays inside the 7d hot window, baseline
`duplicate_fraction`, low `unknown_device_fraction`, and **realistic** (non-flat)
`co_view_multiplier` (DECISIONS Phase 8: the baseline keeps realistic co-view — the
other faults flatten it; the baseline is the one profile that does not, so the sweep
has a realistic control, not a sterile one). Offline-pinned in `test_fault_profiles.py`
like the others: reproducible, zero wrong-household, `recall == 1.0`, no state-misses —
nothing for the agent to confidently flag.

## The eval harness (`agent/eval/`)

Mirrors the deterministic/pure split the rest of the repo uses. The scoring is PURE
and unit-tested offline with synthetic findings — **the only thing the token-gated
live sweep adds is the real LLM outputs**, so the rubric is proven before a token is
spent.

- `agent/eval/scenarios.py` — the frozen scenario catalog: 6 `Scenario(name, profile,
  kind, expected, note)` rows (Ruling B). `kind ∈ {fault_recall,
  negative_confirmation, capability_boundary, control}`; `expected: Hypothesis | None`.
  `len(SCENARIOS) == 6` and
  `EVAL_REPS × len(SCENARIOS) == 30` asserted in a test (the config↔sweep contract).
- `agent/eval/scoring.py` — PURE. `score_rep(scenario, finding) -> Outcome` (Ruling
  C); `RepResult` / `ScenarioResult` / `SweepResult` aggregates; `false_positive_rate`
  over the two controls; `near_miss_rows` (real_lift vs shared_ip_spike). No I/O, no
  clock, no LLM. Unit-tested exhaustively.
- `agent/eval/tables.py` — PURE renderers: the fault→diagnosis table (per scenario:
  kind, expected, k/5 correct, verdict spread, FP-rate row) and the near-miss table
  (per profile: `ip_resolved_fraction`, top_hypothesis spread, verdict), as Markdown.
- `agent/eval/run_eval.py` — the ONLY token-spending path (`make agent-eval`). For each
  scenario: drive a clean isolated stack (`make down && up && seed PROFILE=<s> && run`
  — full `run`, not `run-hot`: `late_burst` needs the restatement and every scenario
  needs `report_snapshots` for the context's restatement field), capture the live
  context headline (FG2 pin), then `EVAL_REPS` live `run_agent(connect_agent(),
  collect(...), Anthropic())` invocations against the populated tables (data identical
  across reps; only the LLM varies). Accumulate, score, render, write the tables into
  `docs/RESULTS.md`, and log total input/output/cache tokens (§2 cost posture — expect
  well under $10). **API-token command — ask the developer before running (CLAUDE.md).**

Isolation reason (each scenario its own clean stack): tiny/medium/faults share
`conversion_id` space (DECISIONS Phase 5) — two profiles cannot co-reside in the
serving tables. Same reason as every `make test-int-*` target; here it is looped.

## `docs/RESULTS.md` — both tables (Done-when)

Append an "Agent eval" section after the Phase-7 benchmark:
1. **fault → top hypothesis → correct? (with false-positive rate).** One row per
   scenario: kind, expected outcome, correct k/5, the verdict/hypothesis spread across
   reps, and the note (co_view_bug labeled a capability boundary, duplicate_flood a
   benign control — distinctly, Ruling B). A trailing FP-rate line over the two
   controls.
2. **near-miss pair.** real_lift vs shared_ip_spike side by side: the deterministic
   `ip_resolved_fraction` discriminator, the agent's top_hypothesis + verdict spread,
   showing it tells a genuine lift from shared-IP inflation on the evidence.
3. **per-profile live context headline** (FG2 — BACKLOG 31): one row per scenario with
   its deterministic, LLM-free discriminator (`match_rate`, `ip_resolved_fraction`,
   ambiguous/candidate counts, `max|Δroas|`, window-edge share). This is the durable
   cross-profile pin — all six scenarios, not just the near-miss pair.

Honesty boundary (checkpoint obligation, BACKLOG 22): state that the agent is
non-reproducible by construction (reps measure residual stability, not determinism),
that `co_view_bug`'s abstention is a labeled capability boundary (co-view adjusted
factor is a DECISIONS won't-do, not a gap papered over), and that these are
small-profile results reported as measured.

## Records

- `DECISIONS.md` Phase 10 block: Rulings A–E; the **BACKLOG 26 won't-do** with the two
  record-fixes (correct row 26's stale "near-miss needs it" anchor — the near-miss is
  shared-IP/device-graph, not co-view, which is *why* the stop fires; and the
  duplicate_flood-vs-co_view_bug abstention distinction); the FG2 live-headline pins.
- `BACKLOG.md`: close row 26 (→ DECISIONS won't-do); resolve row 31 (FG2 — live
  headlines pinned in the sweep + RESULTS); row 32 (sweep amplification) and row 33
  (promote agent_ro write-denied to CI) reviewed — re-defer, triggers unchanged (no
  live Alertmanager push / CI-rebalance this phase).
- `CLAUDE.md`: Phase 10 status line; `make agent-eval` in Commands.

## DONE command

```
make test && make lint          # offline: scoring rubric, scenarios contract, baseline pins
# then, with developer approval (API tokens — the whole sweep):
make agent-eval                 # 6 scenarios × 5 reps live → both tables into docs/RESULTS.md
```

- `make test` (offline, no tokens): the pure scoring rubric over synthetic findings
  (every Ruling-C outcome incl. the near-miss WRONG_DIAGNOSIS and the
  escalation-default-is-abstention rule), the scenarios↔config contract
  (`EVAL_REPS × 6 == 30`), the table renderers, and the `no_fault_baseline` offline
  pins in `test_fault_profiles.py`.
- `make lint`; gate-0 tiny golden byte-identical (the agent writes nothing to the
  pipeline tables; the eval is a read-only observer).
- `make agent-eval` (developer-approved, API tokens): runs the full sweep and writes
  both tables. Watch, per §4.3 and PHASES.md, in this order: (1) the **no-fault
  baseline** is left alone (abstention); (2) the near-miss **NEGATIVE half** —
  `real_lift` is ruled a clean lift (`REAL_PERFORMANCE_CHANGE`, NOT
  `DEVICE_GRAPH_MISMATCH`); (3) the rest of the sweep. Observed-expected outcomes are
  reported, not single-run-gated (Ruling D).

## Done-when

1. **`docs/RESULTS.md` has both tables** — the fault → top-hypothesis → correct? table
   with false-positive rate, and the near-miss pair — produced by `make agent-eval`
   over every fault profile plus the no-fault baseline, `EVAL_REPS` times.
2. Offline `make test` (pure scoring rubric, scenarios/config contract, table
   renderers, baseline profile pins) + `make lint` + gate-0 tiny golden byte-identical,
   all green — the rubric proven before the token run.
3. The no-fault baseline profile exists, is reproducible, and is offline-pinned as
   non-alarming (the sweep's entry condition).

## Scope (files)

- `producer/profiles/no_fault_baseline.json` (new).
- `agent/eval/__init__.py`, `agent/eval/scenarios.py`, `agent/eval/scoring.py`,
  `agent/eval/tables.py`, `agent/eval/run_eval.py` (new package).
- `agent/loop.py` — one prompt sentence (Ruling E), no structural change.
- `Makefile` — `agent-eval` target.
- `docs/RESULTS.md` — the two agent-eval tables + honesty boundary.
- Tests: `tests/test_eval_scoring.py`, `tests/test_eval_scenarios.py`,
  `tests/test_fault_profiles.py` (add baseline pins).
- Records: this spec, `DECISIONS.md`, `BACKLOG.md`, `CLAUDE.md`.

## Review & stack risk

- **code-reviewer + functionality-tester** at the finish line (mandatory).
- **security-reviewer** — the eval reads through `connect_agent()` (SELECT-only,
  unchanged) and spends API tokens; no new ClickHouse user, no CI/.env/compose change,
  no untrusted text into the LLM (the sweep re-derives context from ClickHouse, same
  SN1 boundary). Not triggered unless a review turns up a boundary change.
- **coherence-auditor** at phase exit (mandatory) + BACKLOG review.
- Stack risk: 6 clean-stack cycles in one `make agent-eval` run — slow but inherent to
  the shared-`conversion_id` isolation; the harness logs each stage so a mid-sweep
  failure is diagnosable and the partial tables are still written.

## Out of scope (deferred, recorded)

- Co-view adjusted factor → **DECISIONS won't-do** (Ruling A); `CO_VIEW_INFLATION`
  stays a caveated enum member, `co_view_bug` an abstention.
- Live Alertmanager scrape → webhook push path → BACKLOG (unchanged; no live push).
- Webhook sweep-amplification bound (BACKLOG 32) / agent_ro-write-denied CI job
  (BACKLOG 33) → re-deferred, triggers unchanged.
