# CLAUDE.md — CTV Attribution Pipeline

## What this is

A self-contained streaming data pipeline: a seeded producer emits TV
ad-exposure and conversion events into Redpanda, a deterministic batch
attributor (plain Python, no stream framework) resolves conversions to
households through a device graph in-process and does a windowed,
late-tolerant, cross-device join over a drain of the topics and lands its output
in a local Iceberg lake (the system of record), Dagster loads ClickHouse from the
lake, a periodic reconciliation job reads the lake bucket-locally to close the
long-window tail and settle the shared-IP ambiguous conversions the hot path
refuses to guess, ClickHouse serves
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
   ambiguous shared-IP → unattributed (ambiguous_ip) + candidate_households, never a hot guess
                                       ▼ land (append)
ICEBERG LAKE (system of record, Phase 17)  raw.exposures · raw.attributed_conversions
            day(event_time) × bucket(8, household_id) · append-only · argMax(processed_at) read
                                       ▼ Dagster load (touched days)
CLICKHOUSE  (derived)  attributed_conversions (ReplacingMergeTree, key conversion_id, ver processed_at)
            exposures_landed · campaign_hourly (scheduled refresh) · report_snapshots
                                       ▲
RECONCILIATION JOB (periodic, reads the lake, no broker)  per day, current hot-unattributed rows:
                               state-miss → bucket-local join vs raw.exposures [day−90d, day];
                               ambiguous_ip → explode over candidate_households → bucket-local
                               join → reduce across buckets, most-recent exposure wins (the ONE
                               tiebreak) → append to lake → reload → refresh → snapshot
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
  engine (`resolve_one`, the Phase-2 signature; `graph_loader.py` = the
  compacted-topic graph loader, was `stage.py`). Offline replay = `make resolve`.
  No topic.
- `streaming/` — the engine: `attribute.py` pure core (hot rule, watermark,
  eviction, dedup, the candidate set captured before the placeholder collapse) +
  `dataflow.py` batch-drain driver that LANDS to the lake (never writes ClickHouse
  rows). No Bytewax (Phase 16). The old direct sink is `tests/oracle.py`.
- `reconcile/` — periodic long-window matcher over the lake (Phase 17): per day,
  state-miss rows join bucket-locally; ambiguous_ip rows are exploded over their
  persisted `candidate_households`, joined bucket-locally, reduced across buckets
  by `pick_household` = the one most-recent-exposure tiebreak; rollup refresh,
  snapshot writer. One candidate reader (the lake), one picker, one fix — the
  Phase-6 ClickHouse reader and the Phase-12 source classes were deleted at the
  Phase-17 review gate; the integration parity proof reads `exposures_landed`
  itself.
- `lake/` — the lake of record (Phase 12 → 17): catalog + schemas
  (`raw.exposures`, `raw.attributed_conversions`, `day × bucket(8, household_id)`),
  landing (returns touched days), DuckDB reads (dedup / argMax current),
  `load_serving.py` (the ONE ClickHouse writer of the landed tables),
  `maintenance.py` (compact + expire), `destructive.py` (the THREE destructive paths
  — `reset | replay | maintain` — one process each: validate the profile, derive
  the root, tty prompt, act; `replay` stamps `eval_meta` in-process). Data under
  gitignored `data/lake/<profile>/`.
- `orchestration/` — Dagster assets: per-day lake → ClickHouse load, day-partitioned
  bucket-aligned reconciliation, finalize; the `lake_maintenance` job (not
  registered in the UI code location — destructive, one entry point); ONE headless
  CLI, `run.py` (`load | reconcile`, `--profile` required — it binds the lake);
  `replay.py` / `maintenance.py` are library code for `lake/destructive.py`.
- `clickhouse/` — DDL, users (agent user is SELECT-only), migrations.
- `queries/` — reporting SQL, restatement view, benchmark harness.
- `observability/` — prometheus.yml, alert rules, grafana dashboards (JSON).
- `agent/` — collectors (deterministic, no LLM), hypothesis catalog (enum),
  `probes.py` registry, loop, webhook endpoint, `eval/` fault → diagnosis.
- `common/` — `kafka.py`, the shared start→end topic drain (engine + graph loader).
- `tests/` — pytest unit (no services); `tests/integration/` needs `make up`.
- `fixtures/tiny/` — golden producer output + expected resolved/attributed
  rows. READ-ONLY ground truth after Phase 1.
- `docs/` — ARCHITECTURE.md (spec), PHASES.md (plan), SCALING.md,
  RESULTS.md, RUNBOOK.md.
- `scripts/` — `check_docs.py`, the one docs guard (`make check-docs`; was
  `docs/check_runbook.py`, Phase 19).
- `data/` — gitignored. `data/truth/` side files.
- `DECISIONS.md` — why-not-X log. Add an entry for every non-obvious choice.
- `BACKLOG.md` — deferred findings with revisit triggers. Review at every
  phase exit (alongside the coherence audit): do due items or re-defer with
  a new trigger, never silently drop.

## Commands (macOS, uv)

- `make setup` — uv sync, pre-commit install
- `make up` / `make down` — compose up with health checks / down with volumes
  (`down` plus the three paths in `lake/destructive.py` — `lake-reset`,
  `replay-serving`, `lake-maintain` — are the sanctioned destructive paths; one
  process each: validate the profile, prompt on a tty, act)
- `make seed PROFILE=tiny|medium|<fault>` — run producer (deterministic per
  PRODUCER_SEED; writes truth to data/truth/<profile>/)
- `make resolve PROFILE=tiny SOURCE=fixtures|out` — offline resolve replay
  (service-free): device→household, IP fallback, fan-out → data/out/<profile>/;
  the unit proof of the resolve step the engine runs in-process
- `make run` — engine (resolve in-process → hot join) → lake → Dagster load →
  ClickHouse, then the reconciliation pass (reads the lake → appends corrections →
  reloads touched days → rollup + snapshots; a single pass, not a daemon); the full
  pipeline over the seeded stream. Every row in ClickHouse arrived through the lake
  (Phase 17). One lake per PROFILE, `data/lake/<profile>`, bound by each entry
  point's `--profile` (no default root; `LAKE_ROOT` is a pytest-only tmp override)
- `make run-hot` — engine → lake → load only, no reconciliation; backs the hot-path
  oracle suites (tiny golden/accuracy, medium hardening) and CI, where a
  reconciliation pass would over-credit long-tail organics and shift the pins. Hot
  numbers exclude the deferred shared-IP conversions by design (Phase 16)
- `make reconcile-dagster PROFILE=<p>` — orchestrated reconciliation: materialize the
  day-partitioned `reconciled_conversions` Dagster asset (bucket-aligned over the
  lake) over the candidate days, reload the touched days, finalize — headless
  (ephemeral instance, no webserver). `PARTITION=<YYYY-MM-DD>` materializes one day.
  Output is byte-identical to `make run`'s reconcile pass. Run after `make run-hot`
- `make replay-serving PROFILE=<p> [CONFIRM=yes]` — rebuild the ClickHouse serving
  tables FROM THE LAKE, no Kafka: TRUNCATE `exposures_landed` + `eval_meta` +
  `attributed_conversions` (destructive → prompts unless CONFIRM=yes), reload every
  day the lake holds (current rows: hot or reconciled), stamp `eval_meta` in the
  same process; `make eval` then reproduces the pins (Phase 17)
- `make lake-reset PROFILE=<p> [CONFIRM=yes]` — one of the three sanctioned destructive
  paths (`lake/destructive.py`; the others are `replay-serving` and `lake-maintain`),
  beside `make down`: delete this profile's lake (`data/lake/<p>/`); prompts
  unless CONFIRM=yes. `make down` never touches `data/lake/`. The clean-stack
  `test-int-*` targets pass CONFIRM=yes for their own profile (a clean stack is a
  clean lake — the lake outlives `make down` and ClickHouse is loaded from it)
- `make lake-maintain PROFILE=<p> [CONFIRM=yes]` — the `lake_maintenance` Dagster job (prompts unless CONFIRM=yes — it rewrites the record's data files): compact
  each day partition that accumulated >1 file per bucket (one file per (day,
  bucket)) and expire snapshots older than `LAKE_SNAPSHOT_MAX_AGE_DAYS` (7). Off
  the `make run` path; rows unchanged (Phase 17)
- `make dagster-ui PROFILE=<p>` — optional dev-only Dagster asset-graph viewer;
  materialize works for the one profile bound via `DAGSTER_PROFILE` (no default
  lake root, so an unbound code location only renders the graph),
  bound to loopback 127.0.0.1:3000 (DAGSTER_HOME under gitignored `data/`). Not needed
  for `make reconcile-dagster`. A containerized/published webserver is a deployment
  lever, not built (Phase 12)
- `make rollup-bench PROFILE=<p>` — full rollup rebuild vs the Phase-18a dirty-set
  refresh on a populated stack: asserts the two leave `campaign_hourly` FINAL identical
  (6dp) and that the incremental refresh WRITES fewer rows (direction only; rows read
  are printed with the granule counts explaining why they do not fall at profile size),
  and gates the loader↔rollup contract — every key whose rollup row changed is in the
  dirty set above the refresh watermark (`changed ⊆ dirty`; equality is evidence, not
  the rule). Rewrites the "Rollup refresh" block in `docs/RESULTS.md`. Run after
  `make run PROFILE=<p>` on a profile whose reconcile pass restates something
  (`long_delay`)
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
  scattered), and PREWHERE (WINS). Reuses `bench_common.py`'s canonicalization + summary
  reader; magnitude-free direction asserts + 6dp row-equality; rewrites the "Query cost
  levers" block in `docs/RESULTS.md`. Run after `make lake-reset PROFILE=bench_large
  CONFIRM=yes && make up && make seed PROFILE=bench_large
  && make run PROFILE=bench_large` (Phase 13)
- `make scale-curve` — measured hot-window scaling curve (offline, no compose):
  drain the engine over tiered event counts (1k/10k/100k exposures resident), print
  the measured STRUCTURAL per-exposure window-state cost and the occupancy curve (deep
  sys.getsizeof of retained state ÷ entries — deterministic; `tracemalloc` peak printed
  as a console-only cross-check, never asserted or committed), then rewrite the
  measured-constant block in `docs/SCALING.md`. Occupancy (state size), not throughput
  (Phase 14)
- `make metrics-capture PROFILE=<p>` — dump each stage's terminal Prometheus
  registry from a REAL run to `data/out/<p>/metrics/*.prom` (provenance of the
  promtool alert fixtures; a CLEAN-STACK capture: `make down && make lake-reset
  PROFILE=<p> CONFIRM=yes && make up && make seed PROFILE=<p>` first — over a
  populated lake the reconcile candidates are the lake's current rows and a second
  capture differs; recaptured in Phase 18a)
- `make test-alerts` — `promtool check rules` + `test rules` from the digest-pinned
  prometheus image: the four alert rules fire on long_delay's captured values;
  on tiny's only RestatementMagnitude fires (the Phase-16 deferral landing restates
  ROAS) and the other three stay silent (offline; needs the image, not the stack)
- `make check-docs` — the one docs guard (`scripts/check_docs.py`, Phase 19; was
  `check-runbook`): every link/anchor in README.md + docs/ resolves; each
  `make`-generated block (`scale-curve`, `cost-levers`) is present under its
  generator's marker and the README first-screen copies of its numbers match it;
  every guard/alert/`make` target the docs name exists in source as an EXACT token
  (offline; a standalone script, not a pytest file, so a docs-only edit does not
  re-trigger the full suite — `tests/test_check_docs.py` does run the trace/target
  half under `make test` on purpose; runs in the CI lint job). Accuracy TABLE cells:
  `tests/test_docs_accuracy_pins.py`
- `make agent-run PROFILE=<fault>` — one agent invocation (API tokens; ask first)
- `make agent-eval` — full fault → diagnosis table incl. no-fault baseline
  (API tokens; ask first)
- `make test` — pytest, no services, no network
- `make test-int` — pytest against running compose stack (tiny profile)
- `make test-int-medium` — clean medium-only stack + lake (make down && lake-reset && up && seed
  medium && run-hot) → the Phase-5 live hardening proof (hot engine only — a
  reconcile pass would shift the pinned hot precision); isolated because
  tiny/medium share conversion_id space (DECISIONS Phase 5)
- `make test-int-long-delay` — clean long_delay-only stack + lake (make down && lake-reset && up && seed
  long_delay && run long_delay) → the Phase-6 live reconciliation proof; isolated
  for the same shared-conversion_id reason (DECISIONS Phase 5/6)
- `make test-int-shared-ip` — clean shared_ip_spike-only stack + lake (make down && lake-reset && up &&
  seed shared_ip_spike && run-hot; the test runs the reconcile pass itself) → the
  Phase-8/16 live fault-harness proof: hot `caused_wrong_household=0` (ambiguous
  deferred), post-reconcile 69/80 correct / 11 wrong-household (the fault observed,
  same pick the old hot reduce made), and the `AttributionContext` is populated;
  isolated for the same shared-conversion_id reason
- `make test-int-agent` — clean shared_ip_spike-only stack + lake (make down && lake-reset && up && seed &&
  run) → the Phase-9 live read-only proof: the SELECT-only `agent_ro` user cannot write
  (INSERT/ALTER/DROP/CREATE → ACCESS_DENIED) and the whole collector+probe read path
  runs under it (SN2). No LLM call, no API tokens; isolated for the same reason
- `make test-int-lakehouse` — clean long_delay-only stack + lake (make down &&
  lake-reset && up && seed long_delay && run-hot long_delay) → the Phase-17 live
  lake-of-record proof (the module writes only to a tmp lake it owns): lake-loaded
  serving rows == the direct-write oracle (`tests/oracle.py`) rows; an ACCUMULATED
  lake (3 more appends) reloads byte-identically; the bucket-aligned lake pass ==
  the same candidates matched against exposures read from ClickHouse
  (`exposures_landed FINAL`, a test-local read); the Dagster-orchestrated pass
  writes the same reconciled rows. No API tokens; isolated for the same
  shared-conversion_id reason
- `make lint` — ruff via pre-commit

Canonical clean-state demos (a clean state is a clean lake too — the lake outlives
`make down`, and a `run-hot` over a lake that already holds a reconcile pass's rows
would load those):
- Hot-path headline (fast, stable pins — tiny's only caused hot-misses are the
  3 deferred shared-IP conversions, so `run-hot` keeps the hot pins and avoids
  reconciliation over-crediting its organics):
  `make down && make lake-reset CONFIRM=yes && make up && make seed PROFILE=tiny && make run-hot && make eval && make report`
- Reconciliation + restatement (where the long tail earns its keep — recall
  0.587→0.973, ROAS restated up):
  `make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up && make seed PROFILE=long_delay && make run PROFILE=long_delay && make eval PROFILE=long_delay && make report && make restate`
- Replay from the lake (no Kafka) — after either demo:
  `make replay-serving PROFILE=<p> CONFIRM=yes && make eval PROFILE=<p>`

## Event model facts (from ARCHITECTURE.md; update if empirical findings differ)

- Exposure: exposure_id, event_time, ingest_time, campaign_id, household_id,
  ip, app_id, program_genre, spend. Key: household_id.
- Conversion: conversion_id, event_time, ingest_time, device_id, ip,
  conversion_type (site_visit | purchase), revenue, order_id. Key: device_id.
- Resolved conversion adds: household_id, resolution (device | ip),
  ambiguous (bool), candidate_count.
- Attributed record adds: exposure_id (nullable), assists (list),
  attributed (bool), processed_at, path (hot | reconciled), reason
  (ambiguous_ip | state_miss, null when attributed — Phase 16),
  candidate_households (list; the shared IP's sorted owner set when
  candidate_count > 1, else empty; kept on the reconciled credit — Phase 17).
  19 columns, one order everywhere (`tests/test_column_contract.py`).
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
  back, never metadata. Since Phase 17 every row reaches ClickHouse through the
  lake (engine → lake → Dagster load), and the guarantee is on ROW CONTENT: the
  tiny golden, every pin in `tests/pins.py`, and lake-loaded == direct-write-oracle
  rows hold byte-for-byte (DECISIONS Phase 12/17).
- The lake is an append-only log; "current row per conversion_id" is computed in
  SQL (`argMax(processed_at)`) on every read that needs it — never assumed.
  Re-landing is idempotent on read; re-loading is idempotent by the
  ReplacingMergeTree. Loads are driven by the days a landing TOUCHED, never by
  the wall clock.
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
- Prometheus metric names prefixed by stage: producer_, resolve_, engine_, lake_ (the lake → ClickHouse load),
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
- Specs follow `specs/TEMPLATE.md`. Its three added sections are mandatory:
  **Evidence** (every Done-when item names the test or command output that
  proves it — an item without evidence is not a Done-when item), **Record
  updates** (the explicit list of record files the phase must change; checked
  off in the report, diffed by the coherence auditor), and **Threat model**
  (required whenever the phase adds a Makefile target that takes a variable,
  deletes anything, or takes user input).
- A phase spec is finalized only after its predecessor merges. The FIRST
  commit on a phase branch is a spec-reconciliation amendment against main as
  it is (Phase-17 precedent: F1–F3 → commit 1, stop for approval). No
  implementation before that commit is approved.
- A spec carries at most ~6 pinned decisions / Done-when items. Larger scope
  is split into sub-phases (18a/18b), each with its own branch, PR, and
  review gate.
- Before a phase: restate its "Done when" from docs/PHASES.md.
- At each phase exit: run the coherence audit and review BACKLOG.md for due
  items.
- Build at tiny scale first (fixtures/tiny), prove correctness, then turn
  up the profile.
- Fixtures in fixtures/tiny/ are read-only after Phase 1.
- Stack surprises (ClickHouse, Redpanda, pyiceberg/DuckDB/Dagster): check official docs
  before working around; log the finding under ARCHITECTURE.md "Gotchas".
- Do not add features outside ARCHITECTURE.md without asking. Out of scope
  v1: co-viewing inside the engine, multi-touch models. (Iceberg landing was
  out of scope v1 until the approved Phase-12 reversal — ARCHITECTURE §3.5.)
- Destructive commands (volume removal, DROP, TRUNCATE): only via `make
  down`, or with explicit confirmation.
- API-token commands (`make agent-run`, `make agent-eval`): ask first.
- Commit at every green state with a descriptive message.
- End each loop with a summary: what changed + decisions the spec didn't
  cover, listed explicitly for human review.

### Before reporting DONE

Run this self-review before the review gate; the phase report must include its
output (item by item, not "done"):

1. For every symbol deleted or renamed: grep the whole repo (docs, comments,
   specs, Makefile, CI, .claude/) and list each hit you updated.
2. For every Done-when item: name the test or command output that proves it.
3. For every new Makefile target with a variable or a delete: show behavior
   for empty value, "../x", a value containing `"; `, and the variable set
   from the environment rather than the command line.
4. For every new write path: can it give a different answer on re-run, on a
   non-UTC machine, or with equal sort keys? Name the test pinning each.
5. For every new top-level package: in test_truth_isolation? Metrics prefix
   in CLAUDE.md?
6. List every record file touched and every one the change implies you
   should have touched.
7. For each decision the spec didn't cover: the two alternatives not taken
   and why, one line each.

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
- CI (GitHub Actions) runs `make lint`, `make check-docs`, `make test`, and (on PRs and pushes to main)
  `make up && make test-alerts && make lake-reset CONFIRM=yes && make seed
  PROFILE=tiny && make run-hot && make test-int && make test-int-long-delay &&
  make bench` (hot-path oracles on tiny; reconciliation proven on its own
  long_delay stack).
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
  pytest and blocks on red; treats "no tests collected" as skip. Since Phase 17
  a bare pytest SKIPS `tests/integration` unless `CTV_INT=1`, which only the
  `make test-int*` targets export — so the hook cannot seed the live broker or
  re-stamp `eval_meta`. WIRING is
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

**Current phase: 18a (cost and ops levers) — in build** on `phase-18a-cost-and-ops`
(spec `specs/phase-18a-cost-and-ops.md`, RECONCILED 2026-08-22 — the branch's commit 1).
**Last merged: Phase 19 (PR #33, 2026-08-22).** Next in order: 18b (its spec carries a
"Pre-branch reconciliation required" banner; its branch's commit 1 is that amendment —
DECISIONS "Process"). Open BACKLOG rows: **33** (`grep -cE '^\| \*\*' BACKLOG.md` — the un-struck rows;
reviewed at every phase exit). The per-phase table (0–17, 19 + the fix PRs) lives in `README.md` → History;
rationale in `DECISIONS.md` ("Decisions still in force", then the per-phase appendix);
headline numbers in `docs/RESULTS.md`. No API keys in repo.

(Update this section at the end of every working day.)
