# Build Phases

Each phase is one focused session. Before starting a phase, restate its
**Done when** check. Do not start the next phase until the current one is green.
Phases 4, 7, and 10 are checkpoints: stopping at any of them yields a coherent
project.

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
`household_id`. In-order events, last-touch, no windowing tricks yet. The engine
MUST include the `conversion_id`-keyed ambiguous reduction (DECISIONS.md /
ARCHITECTURE §3.3): the frozen tiny fixture contains 5 ambiguous shared-IP
fan-outs, so collapsing each `conversion_id` to a single most-recent-exposure
row cannot be deferred. Emit attributed and unattributed records to ClickHouse
`attributed_conversions` (ReplacingMergeTree) and land raw exposures in
`exposures_landed`. DDL under `clickhouse/`.

**Done when.** On the tiny fixture the engine produces the committed
deterministic expected-attributed fixture, verified by an integration test.
The engine credits each conversion by the rules above (last-touch;
most-recent-exposure among ambiguous shared-IP candidates), which attributes
every caused conversion to its truth household (household grain — see
ARCHITECTURE §4.3). For the 5 ambiguous shared-IP fan-outs the reduction
**mechanism** is exercised — each `conversion_id` fans out to one candidate row
per household and collapses to a single deterministic most-recent-exposure
winner — but the wrong-household **outcome** does not occur on tiny: the 3
caused ambiguous conversions (c-000014/16/25) all resolve to their truth
household, and the other 2 (c-000041/42) are organic. So tiny's Phase-4
precision (0.673) reflects last-touch **organic over-credit** (17 organic
conversions credited to a coincidentally-recent in-window exposure), NOT
shared-IP misattribution; shared-IP wrong-household misattribution is exercised
by the fault profiles (Phase 8), not tiny. (Compare against the
expected-attributed fixture, not directly against truth.)

---

## Phase 4 — Accuracy eval and reporting v1 · CHECKPOINT

**Goal.** Script computing household-grain precision and recall (ARCHITECTURE
§4.3) by joining `attributed_conversions` from ClickHouse against the truth-link
**side file** (`data/truth/<profile>/`) in the eval harness — truth is never
loaded into ClickHouse (determinism / truth-isolation). The four metric queries
(ROAS, CPA, CVR, site-visit rate) against raw tables.

**Done when.** `make eval` prints an accuracy table and `make report` prints the
four metrics for the tiny profile. Project is demoable end to end.

---

## Phase 5 — Engine hardening

**Goal.** Add, one feature at a time, each with its own test driven by a producer
knob: dedup with a full seen-set (batch drain — TTL'd eviction is a
continuous-follow concern, SCALING.md / DECISIONS Phase 5); watermarks and
allowed lateness; hot-window eviction. (`assists`, `processed_at`, and `path`
were already delivered in Phase 3; Phase 5 regression-guards them through the
evicting window rather than re-adding them.)

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

**Entry condition (first task).** Author the **no-fault baseline** producer profile —
it does not exist yet. Phase-9's `EVAL_REPS=5 × 6 scenarios` (`agent/config.py`)
already assumes it (5 faults + baseline), so the sweep cannot run until it is built.
Phase-10 scoring must key an escalation on `verdict == AMBIGUOUS_NEEDS_HUMAN` as an
abstention, never read the escalation-default `top_hypothesis = upstream_data_change`
as a diagnosis (DECISIONS Phase 9 forward-note).

**Watch first — the near-miss NEGATIVE half is the untested behavior (§4.3 headline).**
Phase 9 proved only the positive half: `shared_ip_spike → device_graph_mismatch` (the
agent CATCHING a fault). The real checkpoint test is the negative half — `real_lift`
must be ruled a **clean lift**: the agent runs `ip_cluster_detail`, sees benign
candidate counts and a FLAT `ip_resolved_fraction`, and DECLINES to fire
`device_graph_mismatch`. That is a negative-confirmation path (probe returns nothing
alarming → rule out), exactly where a model over-eager to diagnose false-positives.
Everything to support it is in place (the discriminator is a context field, the probe
exists), but it is unexercised — watch it FIRST in the sweep, right after the no-fault
baseline.

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

---

# Proposed extensions (post-plan)

Phases 12–15 are **proposed**, not part of the original plan above (which ends at
the Phase 11 checkpoint and yields a coherent project on its own). They extend the
data-platform surface — lakehouse storage + compute, an orchestrator, a real
query-cost story, a measured scaling point, and a runbook.
Each has a spec under `specs/phase-N-<slug>.md` and is marked PROPOSED there: none
opens a branch until approved, and Phase 12 additionally needs dependency sign-off
and an ARCHITECTURE §3.5 scope reversal. They are independent except that Phase 13
reuses Phase 14's volume profile if that lands first.

## Phase 12 — Lakehouse landing + orchestrated reconciliation (PROPOSED)

**Goal.** Land raw exposures to a local Iceberg table (day-partitioned); move the
reconciliation pass to a day-partitioned Dagster software-defined asset that reads
exposures from Iceberg via DuckDB, feeding the unchanged `attribute_household` leaf.
ClickHouse `exposures_landed` stays as the serving copy (dual-write). Adds the
lakehouse storage angle (Iceberg), compute angle (DuckDB), and an orchestrator.

**Done when.** Reconciled output is byte-identical to the current ClickHouse-sourced
pass (asserted on row content — Iceberg metadata is carved out of the determinism
guarantee); a Dagster partition backfill is demonstrated; `make eval PROFILE=long_delay`
reproduces long_delay recall 0.973 (`PROFILE=long_delay` required — bare `make eval`
defaults to tiny; DECISIONS Phase 12). **Needs first:** 5-package allowlist add + ARCHITECTURE §3.5
reversal. Spec: `specs/phase-12-lakehouse-landing.md`.

## Phase 13 — Query cost levers (PROPOSED)

**Goal.** A real "made this query measurably cheaper, and why" story: projection,
data-skipping index, and PREWHERE each measured before/after on the report query via
`X-ClickHouse-Summary`, reusing `bench.py`'s `OPTIMIZE FINAL` canonicalization.
Requires a multi-granule profile (levers are no-ops below one 8192-row granule).

**Done when.** Levers 1 (projection ordered by `event_time`) and 3 (PREWHERE) each
direction-assert a `read_bytes` reduction; Lever 2 (FINAL-avoidance / skip index) lands
as a **documented negative result**, asserted to NOT improve — this schema does not
reward a secondary skip index (leading key already prunes campaign, non-key columns
scattered), and showing *when not to add one* is the phase's point. (Corrected from the original "each lever reduces read_bytes" framing — the
proposed levers named a `campaign_id` column `attributed_conversions` never had and a
skip index on `exposures_landed`'s leading sort key; see the spec and **DECISIONS
Phase 13** for the schema-reality correction, which are authoritative.) Every
measurement returns identical result rows (6 dp) and carries a written why + tradeoff
in RESULTS.md; gate-0 tiny golden byte-identical (lever DDL off the golden path). No
new deps. Spec: `specs/phase-13-query-cost-levers.md`.

## Phase 14 — Measured scaling curve (PROPOSED)

**Goal.** Replace SCALING.md's asserted ~200 B/exposure with a measured occupancy
curve: drain the engine over tiered event counts, report structural
`bytes_per_exposure` and `engine_join_state_current` per tier, re-derive the TB
extrapolation from the measured constant. Offline, no compose.

**Done when.** `make scale-curve` emits the curve and rewrites the SCALING.md
constant; the reported number is structural (deterministic), with `tracemalloc` as a
labeled cross-check only; gate-0 tiny golden byte-identical. No new deps.
Spec: `specs/phase-14-scaling-curve.md`.

## Phase 15 — Runbook and incident log (PROPOSED)

**Goal.** Elevate the two real incidents already in ARCHITECTURE §8 (the `FINAL
read_rows` benchmark non-determinism; the clickhouse-connect timezone round-trip)
into `docs/RUNBOOK.md` post-incident writeups, plus the batch-drain operational
boundary as a documented known-limitation — a real operational artifact for the next
on-call engineer. Docs-only, invent nothing (every claim traces to a recorded
fact).

**Done when.** `docs/RUNBOOK.md` exists with both incidents in symptom → detection →
root cause → fix → generalization form; a trace check confirms every cross-reference
resolves; un-alerted failure modes are named as un-alerted. No new deps.
Spec: `specs/phase-15-runbook.md`.

## Phase 16 — Simplify the core (PROPOSED)

**Goal.** Deletion-first: remove three boxes that were neither a seam nor a scale
boundary. (1) Ambiguous shared-IP conversions are deferred hot (unattributed, reason
ambiguous_ip) and settled by reconciliation with the ONE most-recent-exposure
tiebreak (moved, not rewritten) — hot wrong-household 0 by construction. (2) Resolve
becomes an in-process map step; the `conversions_resolved` topic, subject and stage
producer go (two event topics + `device_graph`). (3) Bytewax is removed; `dataflow.py`
drives the pure core directly. Producer zero-diff; agent contract untouched.

**Done when.** `make test && make lint && make down && make up && make seed
PROFILE=tiny && make run-hot && make eval && make test-int && make test-int-shared-ip`
passes against the once-re-frozen tiny golden and re-pinned `tests/pins.py` (tiny hot
47/35/32, medium hot 129/92/91, long_delay 80/75/44 → 112/75/73; post-reconcile tiny
and medium equal the pre-Phase-16 hot numbers; shared_ip_spike post-reconcile 69/80
== the old hot reduce). Spec: `specs/phase-16-simplify-core.md`.
