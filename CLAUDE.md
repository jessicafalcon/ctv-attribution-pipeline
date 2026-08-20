# CLAUDE.md — CTV Attribution Pipeline

## What this is

A self-contained streaming data pipeline: a seeded producer emits TV
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
- `make context PROFILE=<p>` — build + print the typed `AttributionContext`
  (ARCHITECTURE §4.2) from ClickHouse: the deterministic, LLM-free observe step
  (Phase 8). Serving layer only (N1). Run after `make run`. The agent loop that
  reasons over it is Phase 9
- `make bench` — naive (full FINAL scan-and-join) vs optimized (`campaign_hourly`
  rollup): latency, rows read, bytes read; asserts identical metric rows. Run after
  `make run` populated the rollup
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
  prometheus image: the four alert rules fire on long_delay's captured values,
  silent on tiny's (offline; needs the image, not the compose stack)
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
  seed shared_ip_spike && run) → the Phase-8 live fault-harness proof: the shared-IP
  wrong-household fault is observed (caused_wrong_household=11) and the
  `AttributionContext` is populated; isolated for the same shared-conversion_id reason
- `make test-int-agent` — clean shared_ip_spike-only stack (make down && up && seed &&
  run) → the Phase-9 live read-only proof: the SELECT-only `agent_ro` user cannot write
  (INSERT/ALTER/DROP/CREATE → ACCESS_DENIED) and the whole collector+probe read path
  runs under it (SN2). No LLM call, no API tokens; isolated for the same reason
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
  provider). Green: gate-0 tiny golden byte-identical; 117 offline + lint; bench +
  test-alerts live-green; Grafana provisions. Review gate passed; merged (PR #9).
  Spec: `specs/phase-7.md`.
- Phase 8 (2026-08-19): built on `phase-8-fault-harness` — fault harness + signal
  collectors. Five isolated fault profiles (one anomaly each): `shared_ip_spike`
  (seed 0 — 11 caused wrong-household misattributions, 0 misses; closes BACKLOG 20),
  `late_burst` (seed 7 — 5 hot-misses, ~13.8d peak arrival lateness), `co_view_bug`
  (seed 5 — sports 4× caused-rate, below the `min(1.0, rate)` clamp; BACKLOG 15
  dispositioned), `real_lift` (seed 3 — clean 2× lift, the near-miss counterpart to
  shared_ip_spike), `duplicate_flood` (seed 9 — benign CONTROL: dedup absorbs the
  flood, decision byte-identical dedup on/off, so ClickHouse carries no fingerprint).
  Deterministic LLM-free collectors (`agent/`, mirrors `accuracy/`: pure `collect.py`
  + `readers.py` + `run_context.py`) build the full §4.2 `AttributionContext` from
  ClickHouse only (N1): match rate (+ over time), per-campaign metrics, per-campaign
  restatement deltas, window-edge lag distribution, shared-IP/ambiguous cluster stats,
  RAW genre reach (co-view-adjusted factor stays deferred — BACKLOG 26). Context shape
  FROZEN as the Phase-9 contract (`test_context_schema.py`). `make context` /
  `make test-int-shared-ip`. Green: gate-0 tiny golden byte-identical; 139 offline +
  lint; live `make eval`/`make context` on shared_ip_spike (caused_wrong_household=11,
  ip_resolved_fraction 0.42 — the near-miss discriminator), `make test-int-shared-ip`
  (2). Review gate (code-reviewer + functionality-tester + coherence-auditor;
  security-reviewer not triggered — no CI/.env/compose/CH-user/LLM change) passed;
  merged (PR #10). Spec: `specs/phase-8.md`.
- Phase 9 (2026-08-19): built on `phase-9-agent-loop` — the agent loop. Hypothesis
  catalog enum (`agent/hypotheses.py`, the six §4.2 causes); probe registry
  (`agent/probes.py`, five named parameterized-SQL tools over the SELECT-only
  `agent_ro` user, server-side-bound params, no free-form SQL); typed
  `AttributionFinding` (`agent/finding.py`) emitted via a terminal `submit_finding`
  tool, validation failure → AMBIGUOUS_NEEDS_HUMAN; explicit manual tool-use loop
  (`agent/loop.py`) with Sonnet-5 / effort=medium / adaptive thinking / a cached
  system+enum+probe prefix / the ≥1-probe contract; Alertmanager webhook endpoint
  (`agent/webhook.py`, trigger-only — alert text never reaches the LLM). Config pins in
  `agent/config.py` (AGENT_MODEL/AGENT_EFFORT/EVAL_REPS=5/MAX_PROBE_ROUNDS). New
  SELECT-only `agent_ro` (`clickhouse/users.d/agent-ro.xml`, grant-form) backing the
  WHOLE agent read path via `connect_agent()` (collectors re-pointed, SN2). `make
  agent-run` (API tokens; ask first) / `make test-int-agent`. Done-when all met:
  166 offline + lint; gate-0 tiny golden byte-identical (`make test-int` 11); live
  `make test-int-agent` (6 — write-denied INSERT/ALTER/DROP/CREATE + agent_ro read
  path + all 5 probes execute typed); live `make agent-run` on shared_ip_spike →
  valid finding, top_hypothesis device_graph_mismatch, native ranked, CONFIDENT,
  turn-2 cache_read 2857. Review gate passed (code-reviewer 2 minor → CR-1 rename +
  CR-2 name-based mapping applied; security-reviewer PASS, 2 notes tracked; func PASS;
  FT-1 residual materialized live → Fix A strict `submit_finding`, malformed payload
  committed as a regression fixture). Merged (PR #11). Spec: `specs/phase-9.md`.
- Phase 10 (2026-08-19): built on `phase-10-agent-eval` — agent eval + near-miss demo
  (CHECKPOINT). New `no_fault_baseline` profile (seed 1, medium-scale, REALISTIC
  co-view; offline-clean: truth 90/90 correct, 0 wrong-household, recall 1.0 — nothing
  to flag). Eval harness (`agent/eval/`): frozen 6-scenario catalog (`scenarios.py`),
  PURE scoring rubric (`scoring.py`, four buckets — fault_recall / negative_confirmation
  / capability_boundary / control — with `verdict==AMBIGUOUS_NEEDS_HUMAN` always read as
  abstention, never the escalation-default hypothesis), PURE Markdown renderers
  (`tables.py`), and the
  token-gated `make agent-eval` sweep (`run_eval.py` — clean stack per scenario, EVAL_REPS
  live invocations, both tables → `docs/RESULTS.md`, FG2 headlines captured). One prompt
  sentence added for the no-fault abstain path (Ruling E). BACKLOG 26 (co-view adjusted
  factor) closed as a DECISIONS won't-do (the near-miss is shared-IP/device-graph, not a
  genre number — hard stop fired); co_view_bug scored as a labeled capability boundary,
  distinct from the duplicate_flood/no_fault_baseline FP controls. BACKLOG 31 (FG2)
  resolved via the sweep's live-headline capture. Offline green: 206 tests + lint; gate-0
  tiny golden byte-identical. Review gate passed (code-reviewer + functionality-tester +
  coherence-auditor; 2 blockers B1/B2 + drift D1/D2 + CR-2/CR-3 all dispositioned — one
  offline batch; DECISIONS is a dated trail, ARCHITECTURE the one forward statement, so
  no consolidation). Live `make agent-eval` DONE (30 invocations, Sonnet-5; ~178k cache_read
  input, well under $10): **30/30 correct, false-positive rate 0/10 = 0%**; near-miss both
  halves clean (real_lift → 5× CONFIDENT real_performance_change at ip_resolved_fraction
  0.061, NEVER device_graph_mismatch; shared_ip_spike → 5× CONFIDENT device_graph_mismatch
  at 0.420); co_view_bug → 5× AMBIGUOUS (top co_view_inflation — names the suspect, declines
  to confirm, distinct from the controls' abstention); late_burst → 5× CONFIDENT
  late_arrival_distortion. Both/three tables in `docs/RESULTS.md`. Done-when all met. Merged
  (PR #12). Spec: `specs/phase-10.md`.
- Phase 11 (2026-08-20): built on `phase-11-docs` — docs (final phase, no pipeline
  code). Root `README.md` as a design doc (problem → scope/honesty → architecture with
  teaching-level stream-concept explanations → agent → results → run-it-in-two-commands
  → determinism → repo map → Next-steps/what-was-cut). `docs/SCALING.md` finalized: the
  hot-window-state constraint (`exposure_rate × window`, the first wall), partition math
  (join pins equal partition counts on the two household-keyed topics), 50k/500k tiers,
  state-backend progression (in-memory → RocksDB-sharded → checkpointed), 1:1 Bytewax→Flink
  operator mapping, ClickHouse tier changes (single node → ReplicatedReplacingMergeTree +
  Distributed + per-shard refreshable MVs); the two accumulated build notes kept as
  evidence. `docs/RESULTS.md` finalized with the attribution-accuracy tables (tiny
  0.673/1.000, medium 92/130=0.708/1.000, long_delay recall 0.587→0.973 via reconciliation)
  alongside the existing benchmark + agent-eval sections. No new numbers invented — accuracy
  cites the deterministic integration-test pins; benchmark/eval unchanged from where they
  were captured. Done-when met: README → `make up` → `make seed && make run` is the lead
  path; all internal links resolve, every README command is a real Makefile target. Green:
  206 offline tests + lint (docs-only, no code touched). Review gate PASSED
  (code-reviewer + functionality-tester + coherence-auditor; security-reviewer not
  triggered — no CI/.env/compose/CH-user/agent-context change): coherence found a BLOCKER
  (docs claimed "async inserts on" but the sink inserts synchronously — no `async_insert`
  anywhere) + a drift (RESULTS mislabeled the 2 long_delay wrong-household attributions as
  "misses") + code-reviewer flagged an unmeasured "few thousand msgs/sec" throughput claim;
  all fixed in-branch (async reframed as a SCALING lever in ARCHITECTURE §3.3/§5 + SCALING;
  residuals reworded to caused_missed=0/recall-capped; throughput dropped to non-numeric;
  README webhook forward-points to the live-push cut), re-audit clean. Two loose ends filed,
  not fixed (branch stays docs-only): BACKLOG 35 (stale `sink.py:2` async marker → next
  streaming/ touch), BACKLOG 36 (a test guarding the docs accuracy table vs the integration
  pins → next tests/ touch). Merged: PENDING (developer merges). Spec: none (docs phase;
  Done-when from PHASES.md).
- Phase 11 follow-on (not in the docs PR): BACKLOG 34 — make the token targets auto-load
  `.env` via `uv run --env-file` (guarded `AGENT_ENV := $(if $(wildcard .env),--env-file .env,)`,
  scoped to `agent-run`/`agent-eval`). Own `fix/agent-env-load` branch off main after Phase 11
  merges; mandatory security-review; proof is a fresh-shell `make agent-run PROFILE=shared_ip_spike`
  (key only in .env) reaching `messages.create` (~$1.50, ask first); close row 34 when green.
- Phase 15 (2026-08-20): built on `phase-15-runbook` — runbook + incident log
  (post-plan extension, NOT in the original PHASES.md 0–11; spec added in PR #17).
  Docs-only, no pipeline code. `docs/RUNBOOK.md`: two recorded incidents in
  symptom→detection→root-cause→fix→generalization→would-catch-it-next-time form —
  (1) the benchmark that lied in CI (`FINAL read_rows` counts un-merged version-parts:
  CI rollup 1020 rows lost 0.8×, local 340 rows won 2.5×; guard = `queries/bench.py`
  `_canonicalize` OPTIMIZE + magnitude-free direction assert), (2) the timezone
  round-trip that quadrupled the snapshots (clickhouse-connect renders DateTime in
  client-local tz; guard = server-side `reported_at` in `reconcile/rollup.py` +
  tz-free `toUnixTimestamp64Milli` version read in `reconcile/reconcile.py`) — plus
  the batch-drain known-limitation (windowing proven on a bounded drain; continuous
  follow / spill-to-disk state / TTL'd dedup NOT operated → SCALING.md Flink mapping).
  Would-catch honesty: NEITHER incident is covered by the four `observability/rules/`
  alerts (FINAL read_rows is offline, not scraped; the tz collapse sits below
  `RestatementMagnitude`'s >1.0 threshold) — said so, not implied. Elevate-invent-nothing
  discipline: every number/fix traces to a §8 gotcha / DECISIONS / RESULTS fact.
  Trace check = standalone `docs/check_runbook.py` (`make check-runbook`), NOT a pytest
  file (avoids the run-tests-hook full-suite re-trigger, per BACKLOG 36): verifies every
  RUNBOOK link/anchor resolves and every named guard/alert still exists in source. DONE
  green: `make test` (206 offline) + `make lint` clean; `make check-runbook` OK. README
  repo-map pointer added. Review gate: PENDING (developer runs code-reviewer +
  functionality-tester + coherence-auditor; security-reviewer not triggered — no
  CI/.env/compose/CH-user/agent change). Merged: PENDING. Spec: `specs/phase-15-runbook.md`.
- Phase 14 (2026-08-20): built on `phase-14-scaling-curve` — measured scaling curve
  (post-plan extension, NOT in the original PHASES.md 0–11; spec on main). Turns
  SCALING.md's guessed ~200 B/exposure into ONE measured constant. New reusable volume
  profile `producer/profiles/scale_curve.json` (seed 20, 100k exposures / 2000 households
  / ~100h span < 7d window so nothing evicts — occupancy == count; co-view flat, no fault;
  the top tier, so Phase 13 cost levers can reuse it for granule volume). `streaming/
  scale_probe.py` (`make scale-curve`, offline, no compose): drains the REAL engine
  (`build_flow`+`run_main`, EOF) over tiers 1k/10k/100k, measures the STRUCTURAL
  per-exposure state cost (`deep_sizeof` = recursive sys.getsizeof of the retained
  hot-window exposures ÷ entry count, id()-dedup so shared category strings count once —
  deterministic on re-run), reads Phase-7 `engine_join_state_current` (no new metric), and
  rewrites a marked block in `docs/SCALING.md`. **Measured ~571 B/exposure** (571–573 across
  the curve, ~2.9× the retired guess) → extrapolation re-derived to **~8.6 TB** at 25k/sec ×
  7d (labeled extrapolation — only the per-exposure cost moved asserted→measured; the rate
  and product stay order-of-magnitude sizing). `tracemalloc` peak (~0.75× structural) is a
  labeled cross-check column, NEVER asserted (the determinism trap this phase exists to
  avoid — same discipline as the Phase-7 FINAL read_rows fix). Households scale with count
  (fixed per-household density) so the O(n²)-per-key drain stays cheap (100k in ~2.5s) and
  realistic; `measure_tier` raises if eviction ever fires (retained==input guard). DONE green:
  `make scale-curve && make test (213 offline) && make lint`; gate-0 tiny golden byte-identical;
  SCALING.md byte-stable across re-runs. Spec-vs-repo note: spec named `scale_curve.py` but
  profiles are JSON — followed the real convention, surfaced not silently repaired (DECISIONS
  Phase 14). BACKLOG 35 (stale `sink.py:2` async marker) done in-branch (trigger fired — in
  `streaming/`); BACKLOG 36 (docs accuracy-pin test) trigger fired (added a test file) but
  consciously re-deferred pending developer decision (orthogonal to scaling; would widen the
  PR). Review gate: PENDING (developer runs code-reviewer + functionality-tester +
  coherence-auditor; security-reviewer NOT triggered — no CI/.env/compose/CH-user/agent
  change). Merged: PENDING. Spec: `specs/phase-14-scaling-curve.md`.
- No API keys in repo.

(Update this section at the end of every working day.)
