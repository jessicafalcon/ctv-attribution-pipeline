# Phase 16 — Simplify the core (PROPOSED)

Contract for the `phase-16-simplify-core` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan (Phases 0–11). Origin: the Phase-15 architecture
review (2026-08-20). Three findings, one phase: (1) the Bytewax dataflow is a
`TestingSource` + `fold_final` wrapper over a batch drain — the windowing, watermark,
eviction and dedup logic all live in pure-Python `streaming/attribute.py`, so the
framework does no work; (2) ambiguous shared-IP conversions are fanned out to N
household-keyed candidate rows and collapsed by a second `conversion_id`-keyed reduce —
two operators to make a fast guess that reconciliation could make correctly; (3) the
resolve stage is a separate consumer + topic for an in-memory dict lookup.

**Status: PROPOSED — do not start implementation until approved.** No new dependencies;
one dependency REMOVED (`bytewax`). This phase is deletion-first: it removes concepts,
it adds none.

## Why

Every box in the architecture should be either a seam for another team or a scale
boundary. Today three boxes are neither:

- **Bytewax** contributes a `build_flow` that regroups lists. A reviewer who knows
  streaming spots this in the first five minutes; an honest "deterministic batch
  attributor" is a stronger story than a streaming label that isn't earned. Continuous
  follow on a real framework is a separate decision (Phase 17+ question: Bytewax done
  properly vs Flink) and should be chosen with fresh eyes, not inherited from the
  `TestingSource` shape.
- **Fan-out + reduce** exists only because the household-keyed join cannot compare
  candidates partition-locally (DECISIONS Phase 2/3). Reconciliation already holds every
  exposure (`exposures_landed`) and already applies the most-recent-exposure rule; it is
  the correct owner of the ambiguous case. Advertisers prefer a late correct credit over
  a fast wrong one, and the hot path's wrong-household rate drops to 0 by construction.
- **Resolve as a topic** mirrors real systems where the identity graph is owned by a
  vendor or another team. That seam is worth keeping as a *function signature*, not as a
  consumer group, a topic, and a schema-registry subject.

## The central constraint

**Same answer after reconciliation; fewer moving parts to get there.** The post-
reconciliation attribution for every profile must be at least as good as today
(recall ≥ current pin, wrong-household ≤ current pin). Hot-path numbers WILL move —
ambiguous conversions are no longer credited hot — and that is the point, not a
regression. Every pin change is recorded in `tests/pins.py` with the reason in its
comment, and every docs table moves with it via the existing docs-vs-pins guard
(BACKLOG 36).

Determinism policy is unchanged: same seed + profile → byte-identical topics and
identical attribution output. The pipeline still never reads truth links.

## DONE command

```
make test && make lint \
  && make down && make up && make seed PROFILE=tiny && make run-hot && make eval \
  && make test-int \
  && make test-int-shared-ip
```

- `make test`: unit suite green against the regenerated `fixtures/tiny/expected/`
  and re-pinned `tests/pins.py`; `tests/test_docs_accuracy_pins.py` proves the docs
  tables moved with the pins.
- `make test-int`: the tiny golden comparison passes against the NEW expected rows.
- `make test-int-shared-ip`: hot-path `caused_wrong_household == 0` (was 11), and
  after a reconcile pass the shared-IP conversions are credited to the correct
  household at least as often as today.

## Done-when

1. **Ambiguity is deferred, not guessed.** In `streaming/attribute.py`, a resolved
   conversion with `candidate_count > 1` is emitted `attributed=false` with a new
   `reason` value `ambiguous_ip` (alongside the existing state-miss reason). Device-hit
   and single-candidate IP conversions attribute exactly as today. The
   `conversion_id`-keyed reduction (`_collect` / `_reduce_and_observe`) is deleted.
2. **Reconciliation owns ambiguous rows.** `reconcile/reconcile.py` selects
   `ambiguous_ip` rows inside the long window, enumerates the candidate households for
   the conversion's IP from the device graph, and applies the SAME most-recent-exposure
   rule (ties: `exposure_id`, then `household_id`) that the deleted reduce applied —
   one implementation, moved, not rewritten. Output rows carry `path=reconciled`.
3. **Resolve is a map step.** `streaming/` calls `resolve.resolve(conversion) ->
   list[ResolvedConversion]` in-process (graph loaded via the existing
   `load_graph_index` from the compacted `device_graph` topic). The
   `conversions_resolved` topic, its schema-registry subject, `resolve.stage`'s Kafka
   producer path, and the `resolve` lines in `make run` / `run-hot` / `lake-land` /
   `metrics-capture` are removed. `resolve/` stays as a library module with the same
   function signature and the same `resolve_` metrics (now emitted from the engine
   process). `make resolve` (offline replay) stays — it is the service-free unit proof.
4. **Bytewax is gone.** `streaming/dataflow.py` drives `attribute.py` directly from
   the drained topics (the drain idiom itself is unchanged — ARCHITECTURE §8). `bytewax`
   is removed from `pyproject.toml`, the CLAUDE.md allowlist, and the lockfile. The
   evicting-vs-non-evicting oracle parity (Phase 5) still holds byte-for-byte — it was
   always a property of `attribute.py`.
5. **Fixtures re-frozen with sign-off.** `fixtures/tiny/expected/` is regenerated
   once, in its own commit, after the developer has approved the diff (CLAUDE.md:
   fixtures are read-only after Phase 1 — this is the ONE sanctioned exception and it
   is recorded in DECISIONS). `fixtures/tiny/` producer output (topics) is byte-
   identical to before: the producer is untouched.
6. **Pins and docs move together.** `tests/pins.py` updated with a one-line reason per
   changed pin; README / RESULTS accuracy tables updated; `make test` proves the tables
   match the pins. Expected direction: tiny/medium precision ≥ today (fewer ambiguous
   guesses credited), hot recall may drop where a profile's caused conversions were
   ambiguously resolved, post-reconcile recall ≥ today.
7. **Topology docs match the code.** ARCHITECTURE §3.2 diagram, §3.3 (Resolve stage,
   Engine "Ambiguous reduction", Redpanda "three topics"), §3.4 decision table row 1,
   CLAUDE.md architecture block + repo map + commands, SCALING.md baseline, RUNBOOK
   batch-drain entry, and the Grafana dashboard (no `resolve_` scrape target) are
   updated. Two topics remain: `exposures`, `conversions`; plus `device_graph`.
8. **Agent contract unchanged.** `AttributionContext` shape (frozen Phase 8) and the
   probe registry are untouched; `ip_resolved_fraction` and the shared-IP cluster
   stats still compute from the serving tables. `make agent-eval` is NOT re-run in
   this phase (API tokens) — the eval catalog's expected diagnoses are re-validated
   offline against the new fault-profile contexts via the existing PURE scoring path.

## Pinned decisions (do not re-litigate)

- **Remove Bytewax rather than make it real.** Deletion first keeps this phase low-
  risk; continuous follow is a framework choice (Bytewax vs Flink) for a later phase
  and must not be pre-empted by the wrapper's shape. Recorded in DECISIONS Phase 16 with
  the SCALING.md Flink-mapping table retained as the port target.
- **Ambiguous → reconciliation, never a hot guess.** The hot path attributes only
  when the household is certain (device hit or single IP candidate).
- **One rule, one place.** The most-recent-exposure tiebreak lives in ONE function
  shared by reconciliation and the offline oracle; the deleted reduce is not
  re-implemented anywhere else.
- **Resolve stays a module with the Phase-2 signature.** Its DECISIONS entry gains
  the sentence: "becomes a separate service again when the device graph is owned by
  another team or a vendor; the interface is the function, not the topic."
- **Producer untouched.** Event models, seed, truth links, profiles: zero diff. If a
  Done-when item seems to need a producer change, STOP and report.
- **Fixture re-freeze is one commit, approved before it lands.**

## Scope (files)

- `streaming/attribute.py` (ambiguous → unattributed; delete candidate reduce),
  `streaming/dataflow.py` (drop Bytewax; in-process resolve), `streaming/replay.py`,
  `streaming/scale_probe.py` (same engine entry), `streaming/metrics.py`.
- `resolve/stage.py` (Kafka producer path removed or reduced to the offline replay),
  `resolve/` metrics moved to the engine registry.
- `reconcile/reconcile.py`, `reconcile/sources.py` (ambiguous-candidate enumeration
  needs the device graph: read from the compacted topic or a landed `device_graph`
  table — choose the one that keeps `make reconcile-dagster` output byte-identical to
  `make run`'s pass; record in DECISIONS).
- `clickhouse/` DDL: `attributed_conversions.reason` enum gains `ambiguous_ip`
  (migration, additive).
- `producer/schemas.py` registration list (drop the `conversions_resolved` subject),
  compose topic creation, Makefile, `.github/workflows`.
- `fixtures/tiny/expected/` (re-frozen), `tests/pins.py`, affected tests
  (`test_accuracy`, `test_medium_parity`, `test_fixture_coverage` fan-out shape pins,
  integration engine/eval/reconcile/shared-ip).
- `pyproject.toml` / `uv.lock` (remove `bytewax`).
- Records: this spec, DECISIONS Phase 16, PHASES.md row, CLAUDE.md (architecture,
  repo map, commands, allowlist, status table), ARCHITECTURE §3.2–3.4 + §8, SCALING.md
  baseline, RESULTS.md tables, RUNBOOK batch-drain entry, BACKLOG (close: both resolve
  "continuous follow" rows re-defer to the Phase-17 framework decision; the
  `attribute.py`-one-file row is re-read — it gets smaller).

## Review & stack risk

- **code-reviewer** at the finish line (mandatory): determinism (same seed → same
  output), truth-link isolation, ONE tiebreak implementation, producer zero-diff,
  fixtures changed in exactly one approved commit, allowlist updated.
- **security-reviewer** (mandatory — CI workflow, compose, and ClickHouse DDL change):
  no new exposure; `agent_ro` grants unchanged; the additive enum migration is not
  reachable by the SELECT-only user.
- **functionality-tester** after code-reviewer: runs the DONE command; confirms
  `caused_wrong_household` 11 → 0 hot and ≥ today's correct credits post-reconcile;
  confirms Phase-5 oracle parity and Phase-12 lakehouse parity still hold.
- **coherence-auditor** at phase exit: the topology change touches every doc — the
  audit's job is to find the stale "three topics" / "resolve stage" / "Bytewax"
  sentence that survived.
- Stack risk: removing a topic means `make up` topic creation and CI's `make seed`
  change together; test on a clean `make down && make up` before the review gate.

## Out of scope (deferred, recorded)

- Continuous Kafka follow / framework choice (Bytewax proper vs Flink) — Phase 17+
  decision, informed by SCALING.md's mapping table.
- Iceberg as system of record, bucketed reconciliation — `specs/phase-17-lake-of-
  record.md`.
- Incremental rollups, part-count alerts, async inserts, query-cost table, schema
  compat BACKWARD — Phase 18 (cost & ops), spec to follow.
- Re-running `make agent-eval` live (API tokens; ask first) — offline re-validation
  only in this phase.
