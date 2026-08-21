# CLAUDE.md — CTV Attribution Pipeline

## What this is

A self-contained streaming data pipeline: a seeded producer emits TV
ad-exposure and conversion events into Redpanda, a deterministic batch
attributor (plain Python, no stream framework) resolves conversions to
households through a device graph in-process and does a windowed,
late-tolerant, cross-device join over a drain of the topics, a periodic
reconciliation job closes the long-window tail and settles the shared-IP
ambiguous conversions the hot path refuses to guess, ClickHouse serves
ROAS/CPA/CVR/site-visit rate with restatements, and a read-only AI agent
triages attribution integrity from Prometheus/Alertmanager alerts. Ground-truth causal links let
attribution accuracy and agent accuracy be scored against reality.

Built by a developer who is NEW to Redpanda, stream processing (windowing,
watermarks — Bytewax until Phase 16), and ClickHouse — see Teaching rule below.

`docs/ARCHITECTURE.md` is the spec. `docs/PHASES.md` is the plan. Read both
before design decisions.

## Architecture

```
PRODUCER (seeded)  ── device graph (compacted topic) ── truth links (side file, never read)
   │ exposures (key household_id)      │ conversions (key device_id)
   ▼                                   ▼
REDPANDA  exposures | conversions  + schema registry
                                       │
   exposures ──────────────────────────┤
                                       ▼
ATTRIBUTION ENGINE (deterministic batch attributor, no stream framework)
   resolve step in-process: device → household (IP fallback; shared IP = ambiguous)
   hot window (7d) · last-touch + assists · dedup (seen-set) · watermarks + allowed lateness
   ambiguous shared-IP → unattributed (ambiguous_ip), never a hot guess
                                       ▼
CLICKHOUSE  attributed_conversions (ReplacingMergeTree, key conversion_id, ver processed_at)
            exposures_landed · campaign_hourly (scheduled refresh) · report_snapshots
                                       ▲
RECONCILIATION JOB (periodic)  unattributed in long window (≤90d): state-misses → match vs
                               exposures_landed; ambiguous_ip → candidate households from
                               device_graph, most-recent exposure wins (the ONE tiebreak)
                               → corrected rows → refresh → snapshot
                                       │
REPORTING  4 metrics · restatement view · naive-vs-optimized benchmark
──── off the critical path ──────────────────────────────────────────────
PROMETHEUS → GRAFANA · ALERTMANAGER ──webhook──► AGENT (SELECT-only user, probe registry,
                                                  typed AttributionFinding)
```
Control plane: Docker Compose · Makefile · GitHub Actions CI (tiny profile).

## Repo map

- `specs/` — one spec per phase task. Each has ONE done command. Read fully
  first. `docs/PHASES.md` is the phase list; specs are the executable
  contracts derived from it.
- `producer/` — pydantic event models (source of truth for schemas), device
  graph generator, seeded stream generator, `profiles/` (tiny, medium, fault
  scenarios). Truth links written to `data/truth/`, never read by the pipeline.
- `resolve/` — conversion → household resolution, called IN-PROCESS by the
  engine (`resolve_one`, the Phase-2 signature; `stage.py` is now only the
  compacted-topic graph loader). Offline replay = `make resolve`. No topic.
- `streaming/` — the engine: `attribute.py` pure core (hot rule, watermark,
  eviction, dedup) + `dataflow.py` batch-drain driver. No Bytewax (Phase 16).
- `reconcile/` — periodic long-window matcher (state-misses AND the deferred
  ambiguous_ip rows: candidates re-enumerated from the device graph,
  `pick_household` = the one most-recent-exposure tiebreak), rollup refresh,
  snapshot writer; `sources.py` = the ClickHouse / Iceberg exposure-source
  interface (Phase 12).
- `lake/` — local Iceberg exposure lake (Phase 12): catalog + schema, dedup-safe
  landing, DuckDB read. Data under gitignored `data/lake/`.
- `orchestration/` — Dagster day-partitioned reconciliation assets + headless
  runner (Phase 12).
- `clickhouse/` — DDL, users (agent user is SELECT-only), migrations.
- `queries/` — reporting SQL, restatement view, benchmark harness.
- `observability/` — prometheus.yml, alert rules, grafana dashboards (JSON).
- `agent/` — collectors (deterministic, no LLM), hypothesis catalog (enum),
  `probes.py` registry, loop, webhook endpoint, `eval/` fault → diagnosis.
- `tests/` — pytest unit (no services); `tests/integration/` needs `make up`.
- `fixtures/tiny/` — golden producer output + expected resolved/attributed
  rows. READ-ONLY ground truth after Phase 1.
- `docs/` — ARCHITECTURE.md (spec), PHASES.md (plan), SCALING.md,
  RESULTS.md, demo_checklist.md.
- `data/` — gitignored. `data/truth/` side files.
- `DECISIONS.md` — why-not-X log. Add an entry for every non-obvious choice.
- `BACKLOG.md` — deferred findings with revisit triggers. Review at every
  phase exit (alongside the coherence audit): do due items or re-defer with
  a new trigger, never silently drop.

## Commands (macOS, uv)

- `make setup` — uv sync, pre-commit install
- `make up` / `make down` — compose up with health checks / down with volumes
  (`down` is the ONLY sanctioned destructive path)
- `make seed PROFILE=tiny|medium|<fault>` — run producer (deterministic per
  PRODUCER_SEED; writes truth to data/truth/<profile>/)
- `make resolve PROFILE=tiny SOURCE=fixtures|out` — offline resolve replay
  (service-free): device→household, IP fallback, fan-out → data/out/<profile>/;
  the unit proof of the resolve step the engine runs in-process
- `make run` — engine (resolve in-process → hot join) + reconciliation pass (a
  single pass, not a daemon); the full pipeline over the seeded stream
- `make run-hot` — engine only, no reconciliation; backs the hot-path oracle
  suites (tiny golden/accuracy, medium hardening) and CI, where a reconciliation
  pass would over-credit long-tail organics and shift the pins. Hot numbers
  exclude the deferred shared-IP conversions by design (Phase 16)
- `make lake-land` — engine, dual-writing the SAME deduped exposures
  into the Iceberg lake (`raw.exposures`, day-partitioned) alongside ClickHouse. The
  SOLE landing site (`--lake-land` flag; `make run`/CI never land, so the engine path
  stays byte-identical). Run after `make up && make seed` (Phase 12)
- `make reconcile-dagster PROFILE=<p>` — orchestrated reconciliation: materialize the
  day-partitioned `reconciled_conversions` Dagster asset (exposures sourced from
  Iceberg via DuckDB) over the candidate days + finalize, headless (ephemeral
  instance, no webserver). `PARTITION=<YYYY-MM-DD>` materializes one day. Output is
  byte-identical to `make run`'s reconcile pass. Run after `make lake-land` (Phase 12)
- `make dagster-ui` — optional dev-only Dagster asset-graph UI + backfill controls,
  bound to loopback 127.0.0.1:3000 (DAGSTER_HOME under gitignored `data/`). Not needed
  for `make reconcile-dagster`. A containerized/published webserver is a deployment
  lever, not built (Phase 12)
- `make eval` — attribution precision/recall vs truth for the given `PROFILE`
  (default `tiny`)
- `make report` — 4 advertiser metrics per campaign, from the raw serving tables
- `make restate` — restatement: each campaign's metric as reported
  pre-reconciliation vs now (`report_snapshots` FINAL); run after `make run`
- `make context PROFILE=<p>` — build + print the typed `AttributionContext`
  (ARCHITECTURE §4.2) from ClickHouse: the deterministic, LLM-free observe step
  (Phase 8). Serving layer only (N1). Run after `make run`. The agent loop that
  reasons over it is Phase 9
- `make bench` — naive (full FINAL scan-and-join) vs optimized (`campaign_hourly`
  rollup): latency, rows read, bytes read; asserts identical metric rows. Run after
  `make run` populated the rollup
- `make cost-levers` — three ClickHouse-native query-cost levers, each a before/after
  on a scoped report query over the `bench_large` serving tables (live-stack): a
  projection ordered by `event_time` on `attributed_conversions` (WINS — date-range
  prune), a FINAL-avoidance / bloom-skip-index candidate (documented NEGATIVE result —
  the schema doesn't reward one: leading key already prunes campaign, non-key columns
  scattered), and PREWHERE (WINS). Reuses `bench.py`'s canonicalization + summary
  reader; magnitude-free direction asserts + 6dp row-equality; rewrites the "Query cost
  levers" block in `docs/RESULTS.md`. Run after `make up && make seed PROFILE=bench_large
  && make run` (Phase 13)
- `make scale-curve` — measured hot-window scaling curve (offline, no compose):
  drain the engine over tiered event counts (1k/10k/100k exposures resident), print
  the measured STRUCTURAL per-exposure window-state cost and the occupancy curve (deep
  sys.getsizeof of retained state ÷ entries — deterministic; `tracemalloc` peak printed
  as a console-only cross-check, never asserted or committed), then rewrite the
  measured-constant block in `docs/SCALING.md`. Occupancy (state size), not throughput
  (Phase 14)
- `make metrics-capture PROFILE=<p>` — dump each stage's terminal Prometheus
  registry from a REAL run to `data/out/<p>/metrics/*.prom` (provenance of the
  promtool alert fixtures; live-stack, run after `make up && make seed`)
- `make test-alerts` — `promtool check rules` + `test rules` from the digest-pinned
  prometheus image: the four alert rules fire on long_delay's captured values;
  on tiny's only RestatementMagnitude fires (the Phase-16 deferral landing restates
  ROAS) and the other three stay silent (offline; needs the image, not the stack)
- `make check-runbook` — standalone trace check for docs/RUNBOOK.md: every
  link/anchor resolves and every named guard/alert still exists in source (offline;
  not a pytest file, to avoid the run-tests-hook full-suite re-trigger)
- `make agent-run PROFILE=<fault>` — one agent invocation (API tokens; ask first)
- `make agent-eval` — full fault → diagnosis table incl. no-fault baseline
  (API tokens; ask first)
- `make test` — pytest, no services, no network
- `make test-int` — pytest against running compose stack (tiny profile)
- `make test-int-medium` — clean medium-only stack (make down && up && seed
  medium && run medium) → the Phase-5 live hardening proof; isolated because
  tiny/medium share conversion_id space (DECISIONS Phase 5)
- `make test-int-long-delay` — clean long_delay-only stack (make down && up && seed
  long_delay && run long_delay) → the Phase-6 live reconciliation proof; isolated
  for the same shared-conversion_id reason (DECISIONS Phase 5/6)
- `make test-int-shared-ip` — clean shared_ip_spike-only stack (make down && up &&
  seed shared_ip_spike && run-hot; the test runs the reconcile pass itself) → the
  Phase-8/16 live fault-harness proof: hot `caused_wrong_household=0` (ambiguous
  deferred), post-reconcile 69/80 correct / 11 wrong-household (the fault observed,
  same pick the old hot reduce made), and the `AttributionContext` is populated;
  isolated for the same shared-conversion_id reason
- `make test-int-agent` — clean shared_ip_spike-only stack (make down && up && seed &&
  run) → the Phase-9 live read-only proof: the SELECT-only `agent_ro` user cannot write
  (INSERT/ALTER/DROP/CREATE → ACCESS_DENIED) and the whole collector+probe read path
  runs under it (SN2). No LLM call, no API tokens; isolated for the same reason
- `make test-int-lakehouse` — clean long_delay-only stack (make down && up && seed
  long_delay && lake-land long_delay) → the Phase-12 live lakehouse proof: reconcile
  output is byte-identical whether exposures are sourced from ClickHouse or
  Iceberg-via-DuckDB, and the Dagster-orchestrated pass writes the same reconciled
  rows. No API tokens; isolated for the same shared-conversion_id reason
- `make lint` — ruff via pre-commit

Canonical clean-state demos:
- Hot-path headline (fast, stable pins — tiny has no caused hot-misses, so
  `run-hot` avoids reconciliation over-crediting its organics):
  `make down && make up && make seed PROFILE=tiny && make run-hot && make eval && make report`
- Reconciliation + restatement (where the long tail earns its keep — recall
  0.587→0.973, ROAS restated up):
  `make down && make up && make seed PROFILE=long_delay && make run && make eval PROFILE=long_delay && make report && make restate`

## Event model facts (from ARCHITECTURE.md; update if empirical findings differ)

- Exposure: exposure_id, event_time, ingest_time, campaign_id, household_id,
  ip, app_id, program_genre, spend. Key: household_id.
- Conversion: conversion_id, event_time, ingest_time, device_id, ip,
  conversion_type (site_visit | purchase), revenue, order_id. Key: device_id.
- Resolved conversion adds: household_id, resolution (device | ip),
  ambiguous (bool), candidate_count.
- Attributed record adds: exposure_id (nullable), assists (list),
  attributed (bool), processed_at, path (hot | reconciled), reason
  (ambiguous_ip | state_miss, null when attributed — Phase 16).
- Lateness = ingest_time − event_time. Hot path handles minutes–hours;
  reconciliation handles days.
- Shared IPs across households are the ONLY source of wrong-household
  matches. Keep it that way so the fault is isolatable. Since Phase 16 a
  wrong-household credit can only be made by the reconciliation pass (the hot
  path emits `candidate_count > 1` unattributed — reason ambiguous_ip).

## Determinism policy (core design principle)

AI sits at the edge; the pipeline is deterministic.
- Same PRODUCER_SEED + profile → byte-identical topics and identical
  attribution output. Break this and it's a bug.
- Anything computable is computed in Python/SQL, never asked of an LLM:
  match rates, deltas, IP-cluster stats, restatement magnitudes. Collectors
  build AttributionContext with zero LLM calls.
- The agent is read-only (DB-enforced SELECT-only user), off the critical
  path, and outputs are pydantic-validated. Pipeline output with the agent
  disabled is byte-identical.
- The Iceberg lake's metadata (snapshot ids, commit times) and Dagster run
  ids/wall-clock are non-deterministic and carved out of the byte-identical
  guarantee, exactly like the agent — every asserted check reads row content
  back, never metadata; landing is off by default (`--lake-land`), so `make
  run`/CI stay byte-identical (Phase 12; DECISIONS Phase 12).
- The pipeline NEVER reads truth links.
- Every write to attributed_conversions is idempotent (ReplacingMergeTree
  keyed conversion_id, version processed_at). Rollups are refreshed on
  schedule, never insert-triggered summing MVs (corrections would double-count).
- Test question for any design choice: "could this step give a different
  answer on a re-run?" If yes, justify in DECISIONS.md or fix it.

## Engineering contracts

- Schema contract: pydantic models in producer/ are the source of truth;
  JSON Schemas are generated from them and registered — never hand-edited.
  Producer and every consumer validate.
- Probe contract: `agent/probes.py` entries are (name, parameterized SQL,
  pydantic result type). The model never writes SQL.
- Output contract: AttributionContext and AttributionFinding are pydantic
  models; validation failure → escalate AMBIGUOUS_NEEDS_HUMAN, never silent
  retry.
- Idempotency: replaying any topic from offset 0 converges to the same
  ClickHouse state.
- Minimal but scalable: simplest standard solution now; the scaling path is
  a SCALING.md / DECISIONS.md note, not speculative code. Do not claim scale
  we don't run.

## Communication style (applies to chat replies, comments, docs, commits)

- Result first: lead with what changed / passed / failed, then details.
- Plain English, short sentences. No task restatement, no "I will now..."
  preambles, no closing summaries that repeat the middle.
- If it fits in one sentence, one sentence. Explanations max 4 sentences
  (Teaching-rule explanations included).
- Ban filler adjectives: "robust", "comprehensive", "production-ready",
  "seamless", "powerful". Show the property; don't claim it.
- Code comments only where the code can't say it (a quirk, a why).
  Docstrings: one line unless the function has non-obvious behavior.
- Reports after a task: files touched, commands run, result, open risks,
  next step. Nothing else.

## Conventions

- Python 3.12. Type hints everywhere. No JVM anywhere.
- Dependencies: ask before adding ANY new package. Current allowlist:
  confluent-kafka, clickhouse-connect, pydantic, prometheus-client,
  anthropic, fastapi + uvicorn (agent webhook), pytest, ruff, pre-commit,
  pyiceberg (+ pyiceberg-core write engine), pyarrow, duckdb, dagster,
  dagster-webserver (Phase 12 lakehouse landing + orchestration).
- Prometheus metric names prefixed by stage: producer_, resolve_, engine_,
  reconcile_, agent_.
- Fault scenarios are producer profiles under producer/profiles/, not
  ad-hoc scripts.
- Engine features (dedup, lateness, eviction) are added one at a time, each
  with a test that uses a producer knob to exercise it.
- SQL keywords lowercase, one column per line in select lists.
- Secrets: never commit .env, data/, credentials. ANTHROPIC_API_KEY lives
  in .env only, never CI. Same block-secrets hook and security-reviewer
  rule as previous projects (see Project tooling).

## Teaching rule (IMPORTANT)

The developer is learning Redpanda/Kafka, stream processing (windowing,
watermarks, stateful operators — Bytewax until Phase 16), and ClickHouse. The first time any concept from these tools appears in a
session (e.g. partitions and keys, consumer groups and offsets, compacted
topics, watermarks and allowed lateness, stateful operators and eviction,
ReplacingMergeTree and FINAL, async inserts, refreshable materialized
views, sort keys), add a 2-4 sentence plain-language explanation of what
it is and why it's used here, BEFORE the implementation. Every line merged
must be explainable by the developer in a design review. Prefer the simple,
standard way over the clever way.

## Workflow rules

- Agent-loop tasks: the spec in `specs/` is the contract. Its DONE command
  is the only definition of done. Do not weaken failing tests. If a spec,
  fixture, or ARCHITECTURE.md seems wrong, STOP and report — never silently
  repair.
- Before a phase: restate its "Done when" from docs/PHASES.md.
- At each phase exit: run the coherence audit and review BACKLOG.md for due
  items.
- Build at tiny scale first (fixtures/tiny), prove correctness, then turn
  up the profile.
- Fixtures in fixtures/tiny/ are read-only after Phase 1.
- Stack surprises (ClickHouse, Redpanda, pyiceberg/DuckDB/Dagster): check official docs
  before working around; log the finding under ARCHITECTURE.md "Gotchas".
- Do not add features outside ARCHITECTURE.md without asking. Out of scope
  v1: co-viewing inside the engine, Iceberg landing, multi-touch models.
- Destructive commands (volume removal, DROP, TRUNCATE): only via `make
  down`, or with explicit confirmation.
- API-token commands (`make agent-run`, `make agent-eval`): ask first.
- Commit at every green state with a descriptive message.
- End each loop with a summary: what changed + decisions the spec didn't
  cover, listed explicitly for human review.

## Git workflow (one branch + one PR per phase)

- Treat `main` as protected: never commit to it directly; never force-push.
- Review gate BEFORE the remote: run the review agents (code-reviewer,
  security-reviewer, functionality-tester) on the finished work and report
  the verdicts to the developer. Do NOT push and do NOT open a PR until the
  developer has seen the verdicts and explicitly says to. Commits stay local
  until then.
- STOP-on-findings (IMPORTANT): when a review agent (or any agent) returns
  findings, STOP and report them verbatim. Do NOT fix, patch, or work around
  anything — not even a "trivial" fix — until the developer has reviewed both
  the issue AND the proposed solution and explicitly says to proceed. Present
  the issue and the proposed fix; then wait. This applies to every agent run,
  at every phase, not only the pre-PR review gate.
- Start each phase on a fresh branch from up-to-date main:
  `git checkout main && git pull && git checkout -b phase-N-<slug>`
  (e.g. `phase-2-resolve-stage`). One phase = one branch = one PR.
- Commit small, at green states, message prefixed `phase-N:`.
- Open the PR with `gh pr create` when the phase's Done-when passes AND the
  developer has approved the review verdicts (see review gate above).
  PR body: Done-when check + command output, files touched, decisions
  the spec didn't cover, open risks. Title `Phase N — <name>`.
- CI (GitHub Actions) runs `make lint`, `make test`, and on PRs also
  `make up && make seed PROFILE=tiny && make run && make test-int`.
  A PR is mergeable only when CI is green and code-reviewer +
  functionality-tester have run.
- The developer merges (squash), never Claude. After merge:
  `git checkout main && git pull` before the next phase.
- Hotfixes on a merged phase go on `fix/<slug>` from main, same PR rules.
- Never open a PR that mixes two phases. If a phase reveals a needed
  change in an earlier phase, STOP and report; it becomes its own fix PR.

## Project tooling

Index only — hooks fire from the developer's local, gitignored
`.claude/settings.local.json`; agents and commands self-describe in their
own files. All agents are report-only by contract: none carry Write/Edit,
and their instructions forbid fixing, committing, or working around
findings. A finding is fixed in the main session or explicitly accepted —
never auto-fixed, ignored, or committed around.

- `run-tests` hook — `.claude/hooks/run-tests.py` (committed, adopted as-is
  from trial-signal-assistant); after any .py edit inside this repo, runs
  pytest and blocks on red; treats "no tests collected" as skip. WIRING is
  local-only by design (a committed settings.json would auto-execute an
  inbound PR branch's hook + pytest + conftest.py for anyone opening it in
  Claude Code). One-time re-enable — copy into the gitignored
  `.claude/settings.local.json`:
  `{"hooks": {"PostToolUse": [{"matcher": "Write|Edit|MultiEdit|NotebookEdit",
  "hooks": [{"type": "command", "command": "python3
  \"$CLAUDE_PROJECT_DIR/.claude/hooks/run-tests.py\""}]}]}}`.
  Surviving surface no config closes: running pytest on an inbound branch
  still executes that branch's conftest.py — review conftest.py and
  test-file changes in the GitHub UI before running pytest on it.
- `block-secrets` hook — `~/.claude/hooks/block-secrets.py` (user-level,
  already wired for all projects); blocks writes containing secret-looking
  values.
- `code-reviewer` agent — `.claude/agents/`; diff review against this
  file's rules (determinism, truth-link isolation, schema contract,
  idempotency, allowlist). Run at each spec's finish line, before commit.
- `security-reviewer` agent — `.claude/agents/`; secrets/CI/service-
  exposure/LLM-boundary review. Mandatory before committing changes that
  touch CI workflows, .env or credential handling, compose service
  exposure, ClickHouse users, or agent context assembly.
- `functionality-tester` agent — `.claude/agents/`; runs the suite + the
  spec's DONE command, compares behavior to intent. Run after code-reviewer.
- `coherence-auditor` agent — `.claude/agents/`; whole-repo drift audit vs
  CLAUDE.md / ARCHITECTURE.md / PHASES.md / DECISIONS.md. MANDATORY once at
  each phase exit, before the phase PR merges.
- `/selfcheck` command — `.claude/commands/selfcheck.md`; verifies the last
  commit (suite, DONE command, determinism, fixtures), then stops.
- `strategic-compact` skill — `~/.claude/skills/strategic-compact/`
  (user-level, already wired); suggests /compact at phase breakpoints.

## Current status

All phases **0–15 merged; the plan is complete.** CHECKPOINTs: 4, 7, 10.
Phases 12–15 are post-plan extensions (not in the original PHASES.md 0–11).
Phase 16 (simplify the core) is on branch `phase-16-simplify-core`, in review.
Full per-phase rationale lives in `DECISIONS.md` and `specs/`; deferred items in
`BACKLOG.md`; headline numbers in `docs/RESULTS.md`. Dates are 2026; Spec cell is
the `specs/` file where one was cited.

| Phase | Date | PR | Deliverable (headline result) | Gate | Spec |
|---|---|---|---|---|---|
| 0 | 08-17 | #2 | Scaffolding — compose, Makefile, CI skeleton | — | — |
| 1 | 08-17 | #3 | Event models, seeded generator + knobs (incl. unknown-device), schema registration, tiny golden fixtures (frozen read-only) | — | — |
| 2 | 08-17 | #4 | Resolve stage: device→household, IP fallback, ambiguous fan-out; `ResolvedConversion` schema (compat NONE); offline replay + golden `fixtures/tiny/expected/`; live batch drain + `resolve_` metrics | — | — |
| 3 | 08-17 | #5 | Attribution engine — pure last-touch join + conversion_id-keyed ambiguous reduce (shared by replay + Bytewax); `attributed_conversions`/`exposures_landed` RMT + sync sink; CI integration job (SHA-pinned actions, digest-pinned images) | — | — |
| 4 | 08-18 | #6 | **CHECKPOINT** — household-grain accuracy (precision 0.673 / recall 1.000, N1 side-file join) + report v1 (4 per-campaign metrics: ROAS/CPA/CVR/site-visit) | PASSED | — |
| 5 | 08-18 | #7 | Engine hardening — arrival-ordered, watermark-gated, evicting operator; dedup seen-set, allowed-lateness release, 7d eviction; `medium` profile; evicting == non-evicting oracle byte-identical (92/130, recall 1.0, wrong_hh 0, dedup suppressed 70) | PASSED | — |
| 6 | 08-18 | #8 | Reconciliation + restatements — periodic ≤90d matcher (`reconcile/`) recovers hot-misses via the shared leaf; `campaign_hourly` + `report_snapshots`; `long_delay` profile; 32 candidates → 29 recovered, recall 0.587→0.973, all 3 campaigns' ROAS restated up | PASSED | `phase-6` |
| 7 | 08-19 | #9 | **CHECKPOINT** — benchmark + observability; 4 metrics (incl. `engine_join_state_current`, closes BACKLOG 25); `make bench` (rollup 2.5× fewer rows / 1.6× bytes / 2.6× faster); 4 promtool-proven alert rules (fire on long_delay, silent on tiny); Grafana dashboard | PASSED | `phase-7` |
| 8 | 08-19 | #10 | Fault harness — 5 isolated fault profiles (one anomaly each: shared_ip_spike, late_burst, co_view_bug, real_lift, duplicate_flood) + LLM-free collectors build §4.2 `AttributionContext` from ClickHouse (N1), shape FROZEN as the Phase-9 contract; shared_ip_spike caused_wrong_household=11 | PASSED | `phase-8` |
| 9 | 08-19 | #11 | Agent loop — 6-cause hypothesis enum + 5 parameterized-SQL probes over SELECT-only `agent_ro` (no free-form SQL); typed `AttributionFinding` (fail → AMBIGUOUS_NEEDS_HUMAN); manual tool-use loop (Sonnet-5); Alertmanager webhook (trigger-only, alert text never reaches LLM). Live: device_graph_mismatch CONFIDENT | PASSED | `phase-9` |
| 10 | 08-19 | #12 | **CHECKPOINT** — agent eval + near-miss demo; `no_fault_baseline` profile; frozen 6-scenario catalog + PURE scoring; `make agent-eval` → **30/30 correct, false-positive 0/10 = 0%** (real_lift vs shared_ip_spike both clean; co_view_bug 5× AMBIGUOUS; late_burst 5× CONFIDENT) | PASSED | `phase-10` |
| 11 | 08-20 | #13 | Docs (final planned phase, no pipeline code) — `README.md` design doc, `docs/SCALING.md` (hot-window-state constraint, partition math, 50k/500k tiers, Bytewax→Flink mapping), `docs/RESULTS.md` accuracy tables (tiny 0.673/1.000, medium 0.708/1.000, long_delay 0.587→0.973); no new numbers invented | PASSED (coherence BLOCKER — false "async inserts on" claim — fixed to a SCALING lever) | none (docs) |
| 12 | 08-20 | #21 | *post-plan* — lakehouse landing + orchestrated reconciliation: local Iceberg exposure lake (`lake/`) + Dagster day-partitioned assets (`orchestration/`); `--lake-land` dual-write, byte-identical parity (ClickHouse == Iceberg-sourced reconcile); +5 packages (approved) | PASSED | `phase-12-lakehouse-landing` |
| 13 | 08-20 | #20 | *post-plan* — query cost levers on `bench_large`: projection-by-`event_time` WINS, FINAL-avoidance / skip-index DOCUMENTED NEGATIVE, PREWHERE WINS; lever DDL only inside `make cost-levers`, gate-0 golden untouched | PASSED (BLOCKER + drift cleared on re-check) | `phase-13-query-cost-levers` |
| 14 | 08-20 | #19 | *post-plan* — measured scaling curve: `make scale-curve` drains the real engine over 1k/10k/100k tiers → **~571 B/exposure** (→ ~8.6 TB extrapolation at 25k/s × 7d); tracemalloc console-only, never committed | PASSED (coherence BLOCKER — tracemalloc-in-doc non-idempotency — CLOSED) | `phase-14-scaling-curve` |
| 15 | 08-20 | #18 | *post-plan* — runbook + incident log (`docs/RUNBOOK.md`): 2 incidents (CI benchmark FINAL read_rows; tz round-trip snapshots) + batch-drain limitation; neither is alert-covered (said so); `make check-runbook` trace check | PASSED | `phase-15-runbook` |
| 16 | 08-21 | — | *post-plan* — simplify the core (deletion-first): ambiguous shared-IP conversions deferred hot (reason ambiguous_ip) → hot wrong-household 0 by construction, reconciliation owns the one most-recent-exposure tiebreak (`pick_household`, candidates re-enumerated from `device_graph`); resolve is an in-process map step (`conversions_resolved` topic/subject/stage gone — two event topics); Bytewax removed (`dataflow.py` drives `attribute.py`; `-1` package). Pins: tiny hot 47/35/32, medium hot 129/92/91, long_delay hot 80/75/44 → post 112/75/73 (recall 0.587→0.973 unchanged); shared_ip_spike post-reconcile 69/80 (== old hot). `reason` column (ambiguous_ip \| state_miss, null when attributed) added to the attributed model/DDL/sink; tiny `expected/attributed.jsonl` re-frozen once with sign-off (5 decision rows change; all rows gain `reason`) | in review | `phase-16-simplify-core` |

**Follow-on / standalone fix PRs** (each its own branch off main, same review discipline):
- `fix/bench-direction-guard` (PR #14) — magnitude-free bench direction assert + `_canonicalize` OPTIMIZE for deterministic `read_rows` (BACKLOG 29).
- `fix/agent-env-load` (PR #15) — `agent-run`/`agent-eval` auto-load `.env` via `uv run --env-file`, guarded + scoped (BACKLOG 34; security-review PASS).
- `fix/eval-demo-profile` (PR #23) — `make eval` PROFILE prose + long_delay demo fixed; the durable profile/DB-mismatch guard and the `Makefile:128-129` comment twin shipped in `fix/eval-profile-guard` (BACKLOG 43).
- `fix/eval-profile-guard` (PR #25) — fail-loud eval profile/DB-mismatch guard: `eval_meta` marker stamped by every populate target (run/run-hot/lake-land/metrics-capture), asserted `== --profile` in `accuracy/run.py`; closes BACKLOG 43 incl. the Makefile:128-129 comment twin. Marker off the golden path, no timestamp → gate-0 byte-identical.
- `fix/docs-accuracy-pin` (PR #26) — single-sourced the household-grain accuracy pins into `tests/pins.py` (tiny/medium/long_delay), referenced by the 5 test suites, plus `tests/test_docs_accuracy_pins.py` asserting the README/RESULTS accuracy TABLE cells equal them; closes BACKLOG 36. Table-scoped — prose citations deferred to a new BACKLOG row.

No API keys in repo.

(Update this section at the end of every working day.)
