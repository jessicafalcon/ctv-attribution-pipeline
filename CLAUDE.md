# CLAUDE.md — CTV Attribution Pipeline

## What this is

A portfolio-grade streaming data pipeline: a seeded producer emits TV
ad-exposure and conversion events into Redpanda, a resolve stage maps
conversions to households through a device graph, a Bytewax engine does a
windowed, late-tolerant, cross-device stream join, a periodic reconciliation
job closes the long-window tail, ClickHouse serves ROAS/CPA/CVR/site-visit
rate with restatements, and a read-only AI agent triages attribution
integrity from Prometheus/Alertmanager alerts. Ground-truth causal links let
attribution accuracy and agent accuracy be scored against reality.

Built by a developer who is NEW to Redpanda, stream processing (Bytewax),
and ClickHouse — see Teaching rule below.

`docs/ARCHITECTURE.md` is the spec. `docs/PHASES.md` is the plan. Read both
before design decisions.

## Architecture

```
PRODUCER (seeded)  ── device graph (compacted topic) ── truth links (side file, never read)
   │ exposures (key household_id)      │ conversions (key device_id)
   ▼                                   ▼
REDPANDA  exposures | conversions | conversions_resolved  + schema registry
                                       │
                                  RESOLVE STAGE  device → household (IP fallback, fan-out)
                                       │  → conversions_resolved (key household_id)
   exposures ──────────────────────────┤
                                       ▼
ATTRIBUTION ENGINE (Bytewax)  hot window (7d) · last-touch + assists · dedup (seen-set)
                              watermarks + allowed lateness · emits attributed/unattributed
                                       ▼
CLICKHOUSE  attributed_conversions (ReplacingMergeTree, key conversion_id, ver processed_at)
            exposures_landed · campaign_hourly (scheduled refresh) · report_snapshots
                                       ▲
RECONCILIATION JOB (periodic)  unattributed in long window (≤90d) → match vs
                               exposures_landed → corrected rows → refresh → snapshot
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
- `resolve/` — conversion → household stage. Loads the graph from the
  compacted topic; emits to `conversions_resolved`.
- `streaming/` — Bytewax dataflow: join, hot window, dedup, lateness.
- `reconcile/` — periodic long-window matcher, rollup refresh, snapshot writer.
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
  (service-free): device→household, IP fallback, fan-out → data/out/<profile>/
- `make run` — resolve stage + engine + reconciliation pass (a single pass, not a
  daemon); the full pipeline over the seeded stream
- `make run-hot` — resolve stage + engine only, no reconciliation; backs the
  hot-path oracle suites (tiny golden/accuracy, medium hardening) and CI, where a
  reconciliation pass would over-credit long-tail organics and shift the pins
- `make eval` — attribution precision/recall vs truth for the last profile
- `make report` — 4 advertiser metrics per campaign, from the raw serving tables
- `make restate` — restatement: each campaign's metric as reported
  pre-reconciliation vs now (`report_snapshots` FINAL); run after `make run`
- `make bench` — naive (full FINAL scan-and-join) vs optimized (`campaign_hourly`
  rollup): latency, rows read, bytes read; asserts identical metric rows. Run after
  `make run` populated the rollup
- `make metrics-capture PROFILE=<p>` — dump each stage's terminal Prometheus
  registry from a REAL run to `data/out/<p>/metrics/*.prom` (provenance of the
  promtool alert fixtures; live-stack, run after `make up && make seed`)
- `make test-alerts` — `promtool check rules` + `test rules` from the digest-pinned
  prometheus image: the four alert rules fire on long_delay's captured values,
  silent on tiny's (offline; needs the image, not the compose stack)
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
- `make lint` — ruff via pre-commit

Canonical clean-state demos:
- Hot-path headline (fast, stable pins — tiny has no caused hot-misses, so
  `run-hot` avoids reconciliation over-crediting its organics):
  `make down && make up && make seed PROFILE=tiny && make run-hot && make eval && make report`
- Reconciliation + restatement (where the long tail earns its keep — recall
  0.587→0.973, ROAS restated up):
  `make down && make up && make seed PROFILE=long_delay && make run && make eval && make report && make restate`

## Event model facts (from ARCHITECTURE.md; update if empirical findings differ)

- Exposure: exposure_id, event_time, ingest_time, campaign_id, household_id,
  ip, app_id, program_genre, spend. Key: household_id.
- Conversion: conversion_id, event_time, ingest_time, device_id, ip,
  conversion_type (site_visit | purchase), revenue, order_id. Key: device_id.
- Resolved conversion adds: household_id, resolution (device | ip),
  ambiguous (bool), candidate_count.
- Attributed record adds: exposure_id (nullable), assists (list),
  attributed (bool), processed_at, path (hot | reconciled).
- Lateness = ingest_time − event_time. Hot path handles minutes–hours;
  reconciliation handles days.
- Shared IPs across households are the ONLY source of wrong-household
  matches. Keep it that way so the fault is isolatable.

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
  bytewax, confluent-kafka, clickhouse-connect, pydantic, prometheus-client,
  anthropic, fastapi + uvicorn (agent webhook), pytest, ruff, pre-commit.
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

The developer is learning Redpanda/Kafka, stream processing (Bytewax), and
ClickHouse. The first time any concept from these tools appears in a
session (e.g. partitions and keys, consumer groups and offsets, compacted
topics, watermarks and allowed lateness, stateful operators and eviction,
ReplacingMergeTree and FINAL, async inserts, refreshable materialized
views, sort keys), add a 2-4 sentence plain-language explanation of what
it is and why it's used here, BEFORE the implementation. Every line merged
must be explainable by the developer in a job interview. Prefer the simple,
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
- Stack surprises (Bytewax, ClickHouse, Redpanda): check official docs
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

- Phase 0 (2026-08-17): PR #2 merged.
- Phase 1 (2026-08-17): merged (PR #3). Models, seeded generator + knobs
  (incl. unknown-device), schema registration, curated tiny golden fixtures.
  Fixtures frozen read-only.
- Phase 2 (2026-08-17): merged (PR #4). Resolve stage (device→household, IP
  fallback, ambiguous fan-out), stateless map, `ResolvedConversion` +
  `conversions_resolved-value` schema (per-subject compatibility NONE), offline
  replay + golden `fixtures/tiny/expected/`, live batch stage (EOF-driven drain)
  + resolve_ metrics, live integration test.
- Phase 3 (2026-08-17): built on `phase-3-attribution-engine` — attribution
  engine. Pure core (`streaming/attribute.py`): household last-touch join +
  conversion_id-keyed ambiguous reduction as two leaf functions shared by the
  offline replay and the live Bytewax dataflow. `AttributedConversion` model;
  golden `fixtures/tiny/expected/attributed.jsonl` (55 rows). ClickHouse serving
  layer: `attributed_conversions` (ReplacingMergeTree) + `exposures_landed`
  (RMT), DDL + applier, engine_ metrics, sync sink. `make run` / `make test-int`
  + CI integration job (SHA-pinned actions, digest-pinned images). Both DONE
  halves green (71 tests, lint; integration green on a clean compose cycle:
  FINAL == golden, exposures_landed idempotent). Merged (PR #5).
- Phase 4 (2026-08-18): built on `phase-4-eval-reporting` — accuracy eval +
  reporting v1 (CHECKPOINT). Household-grain accuracy (`accuracy/`): precision
  0.673 (35/52) / recall 1.000, scored from `attributed_conversions` FINAL
  joined against the truth side file in-harness (never in the DB, N1); exact
  exposure-id is a labeled diagnostic only. Report v1 (`queries/`): four
  per-campaign metrics (ROAS, CPA, CVR, site-visit rate) from the raw serving
  tables, FINAL on both RMT tables, NULL on zero denominators, wrong-household
  attributions kept in. DONE green (79 tests, lint; `make eval`/`make report`
  reproduce the pinned numbers; integration `test_eval_report.py` green).
  Pre-spec doc corrections merged (household grain, N1 side-file join, tiny =
  organic over-credit not shared-IP). Review gate passed (code-reviewer +
  functionality-tester + coherence-auditor). Merged (PR #6).
- Phase 5 (2026-08-18): built on `phase-5-engine-hardening` — engine hardening.
  Engine moved from `fold_final` to an arrival-ordered, watermark-gated,
  evicting operator (still a batch drain; continuous follow deferred, no phase
  owns it). Features, each knob-driven: (1) dedup as a **full seen-set** (not
  TTL'd — the seeded duplicate is timestamp-identical; TTL is a continuous-mode
  SCALING note); (2) watermarks + allowed-lateness **release** (a conversion is
  a pure probe, buffered until `max(event_time) − allowed_lateness ≥ its
  event_time`, then attributed; EOF flush is the completeness backstop);
  (3) hot-window **eviction** (`watermark > event_time + 7d`, strict `>`, run
  after release) + `engine_exposures_evicted_total` / `engine_join_state_size`.
  `assists`/`processed_at`/`path` were Phase-3 deliverables, regression-guarded
  here. New `medium` profile (seed 11, 12.5d span, `unknown_device 0.1`).
  Done-when green: robustness-oracle equality (evicting engine == non-evicting
  oracle **byte-identical**, 132 rows; precision 92/130, recall 1.0, wrong_hh 0;
  dedup suppressed 70; eviction fired), gate-0 tiny golden held byte-identical
  through the rewrite, live proof via isolated `make test-int-medium`. 103
  offline tests + 2 live; lint clean. Review gate passed (code-reviewer +
  functionality-tester + coherence-auditor); follow-ups applied. Merged (PR #7).
- Phase 6 (2026-08-18): built on `phase-6-reconciliation` — reconciliation and
  restatements. Periodic long-window (≤90d) matcher (`reconcile/`) recovers
  hot-path misses (conversions whose causal exposure is >7d before them in
  event-time — evicted from the hot window), reusing the pure `attribute_household`
  leaf at 90d over models reconstructed from ClickHouse FINAL (serving-only, N1).
  Candidates are hot-unattributed rows (`attributed=0 AND path='hot'`); corrected
  rows carry `path=reconciled`, `processed_at = max(ingest_time over fixed state) +
  1s` (> the hot version, stable across re-runs). `campaign_hourly` (versioned-
  replace RMT, all keys recomputed per refresh), `report_snapshots` (per-campaign,
  PRE filters `path='hot'` so the restatement is re-run-safe), `queries/
  restatement.sql`. `make run` now resolve→engine→reconcile; `make run-hot`
  (resolve→engine) backs the hot-path oracle suites (tiny golden/accuracy, medium
  hardening) + CI, since reconcile would over-credit tiny/medium long-tail organics
  and shift their pins. New `long_delay` profile (seed 6, delay straddles ≤7d and
  (7d,90d]). Green: gate-0 tiny golden byte-identical; 113 offline + lint; live
  tiny `make test-int` (5), medium `make test-int-medium` (2, run-hot), long_delay
  `make test-int-long-delay` (3) — 32 candidates → 29 recovered, recall 0.587→0.973,
  restatement shows all 3 campaigns' ROAS up. Review gate passed; merged (PR #8).
  Spec: `specs/phase-6.md`.
- Phase 7 (2026-08-19): built on `phase-7-benchmark-observability` — benchmark +
  observability (CHECKPOINT). Four new metrics (each unit-tested): `resolve_input_backlog`
  (batch consumer-lag proxy), `engine_watermark_lag_seconds` (peak arrival lateness,
  computed engine-side so the pure core stays untouched), `engine_join_state_current`
  (post-eviction occupancy, rises AND falls — closes BACKLOG 25), and
  `reconcile_restatement_roas_abs_delta`. `make bench`: naive full FINAL scan-and-join
  vs `campaign_hourly` rollup, reporting latency/rows/bytes from `X-ClickHouse-Summary`,
  asserting identical metric rows (6 dp); long_delay = rollup reads 2.5× fewer rows,
  1.6× fewer bytes, 2.6× faster (RESULTS.md). Four alert rules (ConsumerLag,
  WatermarkStall, MatchRateOutOfBand, RestatementMagnitude) proven by `make test-alerts`
  (promtool from the digest-pinned image) against REAL captured values — `--metrics-out`
  dumps each stage's own registry, `make metrics-capture` orchestrates a live run,
  `observability/gen_alert_fixtures.py` bakes the numbers into the fixture (fires on
  long_delay, silent on tiny). Grafana "Attribution Integrity" dashboard (JSON, file
  provider). Green: gate-0 tiny golden byte-identical; 118 offline + lint; bench +
  test-alerts live-green; Grafana provisions. Review gate (code-reviewer +
  security-reviewer + functionality-tester + coherence-auditor) PENDING. Spec:
  `specs/phase-7.md`.
- No API keys in repo.

(Update this section at the end of every working day.)
