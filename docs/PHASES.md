# Build Phases

Each phase is one focused session. Before starting a phase, restate its
**Done when** check. Do not start the next phase until the current one is green.
Phases 4, 7, and 10 are checkpoints: stopping at any of them yields a coherent
submission.

Spec: `ARCHITECTURE.md`. Invariants, commands, and git workflow: `../CLAUDE.md`.
Every phase is its own branch and PR (`phase-N-<slug>`); see CLAUDE.md → Git workflow.

---

## Phase 0 — Skeleton and infra

**Goal.** Repo layout, `uv` project, ruff and pytest configured, Docker Compose
with Redpanda (+ schema registry), ClickHouse, Prometheus, Grafana, Alertmanager.
Makefile with `up`, `down`, `test`, `lint`.

Also: git repo initialised, `main` branch protected on GitHub, GitHub Actions
workflow running `make lint` + `make test` on every PR (integration job added
in Phase 3), PR template with the Done-when / files / decisions / risks
sections. This phase is itself the first PR (`phase-0-skeleton`).

Also: tooling review. Inspect `~/dev/trail-signal-assistant/.claude/` and the
user-level `~/.claude/` hooks/skills; produce an adopt / adapt / not-needed table
with one-line reasons; wait for approval; wire the approved items; replace the
placeholder "Project tooling" section in CLAUDE.md with the real index.

**Done when.** `make up` brings every service healthy (health checks, not sleeps),
`make test` runs an empty suite green, CI is green on the Phase 0 PR, and
CLAUDE.md "Project tooling" lists what is actually wired.

---

## Phase 1 — Producer and contracts

**Goal.** Pydantic models for exposure, conversion, device graph, truth link. JSON
Schemas generated from the models and registered in the schema registry. Seeded
generator with all knobs (throughput, late injector, duplicate injector, shared-IP
fraction, co-view multiplier), writing events to Redpanda, the graph to the
`device_graph` compacted topic, and truth links to a side file. A `tiny` profile
(≈10 households, ≈200 events) committed as golden fixture data.

**Done when.** `make seed PROFILE=tiny` produces byte-identical messages on two
runs; unit tests cover graph generation and each knob; schemas are registered and
validated on produce.

---

## Phase 2 — Resolve stage

**Goal.** Consumer on `conversions`, graph loaded from the compacted topic, lookup
by device then IP with fan-out for ambiguous matches, republish to
`conversions_resolved` keyed by `household_id`. Prometheus metrics for resolve
rate, ambiguity rate, fan-out.

**Done when.** Tests cover device hit, unique IP fallback, and ambiguous IP
fan-out; a run over the tiny fixture yields the expected resolved records
(committed under `fixtures/tiny/expected/`).

---

## Phase 3 — Attribution engine, minimal

**Goal.** Bytewax dataflow joining `exposures` and `conversions_resolved` on
`household_id`. In-order events, last-touch, no windowing tricks yet. Emit
attributed and unattributed records to ClickHouse `attributed_conversions`
(ReplacingMergeTree) and land raw exposures in `exposures_landed`. DDL under
`clickhouse/`.

**Done when.** On the tiny fixture the engine attributes exactly the conversions
the truth file says it should, verified by an integration test comparing engine
output against truth.

---

## Phase 4 — Accuracy eval and reporting v1 · CHECKPOINT

**Goal.** Script computing precision and recall against truth links from
ClickHouse. The four metric queries (ROAS, CPA, CVR, site-visit rate) against raw
tables.

**Done when.** `make eval` prints an accuracy table and `make report` prints the
four metrics for the tiny profile. Project is demoable end to end.

---

## Phase 5 — Engine hardening

**Goal.** Add, one feature at a time, each with its own test driven by a producer
knob: dedup with TTL'd state; watermarks and allowed lateness; hot-window eviction;
assists recorded; `processed_at` and `path` fields on every record.

**Done when.** The `medium` profile with duplicates and hour-late arrivals enabled
reaches the same precision/recall as the clean run, and the join-state metric
shows eviction working.

---

## Phase 6 — Reconciliation and restatements

**Goal.** Periodic job matching unattributed conversions against
`exposures_landed` within the long window, writing corrected rows with
`path=reconciled`. `campaign_hourly` rollup with scheduled refresh.
`report_snapshots` written on each refresh. Restatement query.

**Done when.** A run with days-late arrivals recovers those conversions on the
next reconciliation pass, and the restatement query shows the metric change
between snapshots.

---

## Phase 7 — Benchmark and observability · CHECKPOINT

**Goal.** Naive-vs-optimized harness reporting latency, rows read, bytes read, with
a written explanation of why each change worked. Grafana dashboards exported as
JSON. Alertmanager rules for lag, watermark stall, match-rate band, restatement
magnitude.

**Done when.** `make bench` produces the before/after table and every alert can be
triggered by a producer knob.

---

## Phase 8 — Fault harness and signal collectors

**Goal.** Named fault scenarios as producer profiles (shared-IP spike, late burst,
co-view multiplier bug, duplicate flood, real performance lift). Deterministic
collectors building the typed `AttributionContext` from ClickHouse.

**Done when.** Each fault profile runs reproducibly and the context object is
populated and unit-tested without any LLM call.

---

## Phase 9 — Agent loop

**Goal.** Hypothesis catalog as an enum. Probe registry as tools over a
SELECT-only ClickHouse user. Ranking. Typed `AttributionFinding`. Webhook endpoint
for Alertmanager.

**Done when.** The agent runs against one fault profile end to end and emits a
valid finding; a test asserts the agent's DB user cannot write.

---

## Phase 10 — Agent eval and the near-miss demo · CHECKPOINT

**Goal.** Run every fault profile plus a no-fault baseline, repeated, producing the
*fault → top hypothesis → correct?* table with false-positive rate. Run the
near-miss pair (real lift vs shared-IP inflation).

**Done when.** `RESULTS.md` has both tables. Confirm with the user before running;
this costs API tokens.

---

## Phase 11 — Docs

**Goal.** README as a design doc (problem → architecture → results → run it in two
commands). `SCALING.md` (50k and 500k tiers, partition math, state backend,
Flink mapping, ClickHouse tier changes). `RESULTS.md` finalized. "Next steps"
section listing what was cut and why.

**Done when.** A reader can go from README to a running demo with `make up` and
`make seed && make run`.
