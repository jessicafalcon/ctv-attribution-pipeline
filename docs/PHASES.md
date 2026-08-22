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

(Superseded in part by Phase 16: the consumer, the `conversions_resolved` topic
and its subject are gone — resolve is the same function called in-process by the
engine (`resolve_one`, the Phase-2 signature). The Done-when still holds as written
for the function and the fixture; see `specs/phase-16-simplify-core.md` and
DECISIONS Phase 16, which are authoritative.)

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

(Corrected by Phase 16 — the text above is the Phase-3 contract as written and is
kept as history: the `conversion_id`-keyed reduction no longer exists, and the
"MUST include" clause is superseded. An ambiguous shared-IP conversion is now
emitted unattributed on the hot path (`reason='ambiguous_ip'`, one placeholder
row per `conversion_id`) and settled by reconciliation's `pick_household` — the
same most-recent-exposure rule, moved. Bytewax is gone too; the engine is a
plain-Python batch attributor. tiny's hot precision is therefore 0.681 (32/47,
the 3 caused ambiguous conversions deferred), and 0.673 (35/52) is its
post-reconcile number. `specs/phase-16-simplify-core.md`, `tests/pins.py` and
DECISIONS Phase 16 are authoritative.)

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

Phases 12–19 are post-plan extensions, not part of the original plan above (which
ends at the Phase 11 checkpoint and yields a coherent project on its own). They
extend the data-platform surface — lakehouse storage + compute, an orchestrator, a
real query-cost story, a measured scaling point, a runbook, a simplified core, a
lake of record, cost/ops levers, and a docs reshape. Each has a spec under
`specs/phase-N-<slug>.md`; the spec keeps its "(PROPOSED)" title as the record of
how it was approved (a spec reconciled against main before its branch opened —
Phase 19 on — is titled "(RECONCILED)"), and none opens a branch until approved.
Status: **12–17 merged**, **19 built, in review** (`phase-19-docs-reshape`;
reordered before 18a/18b — DECISIONS "Process"), **18a/18b specs written, not
reconciled** (each branch's commit 1 is its reconciliation amendment). Phase 12
additionally needed dependency sign-off and an ARCHITECTURE §3.5 scope reversal.
The per-phase results table lives in `README.md` → History.

## Phase 12 — Lakehouse landing + orchestrated reconciliation (PROPOSED)

**Goal.** Land raw exposures to a local Iceberg table (day-partitioned); move the
reconciliation pass to a day-partitioned Dagster software-defined asset that reads
exposures from Iceberg via DuckDB, feeding the unchanged `attribute_household` leaf.
ClickHouse `exposures_landed` stays as the serving copy (dual-write — superseded by
Phase 17, where the lake is the record and ClickHouse is loaded from it). Adds the
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

## Phase 17 — Lake of record (PROPOSED)

**Goal.** Flip the arrow: the Iceberg lake becomes the system of record and
ClickHouse a replayable serving projection, with bucket-aligned reconciliation.
Spec: `specs/phase-17-lake-of-record.md` (amended before the branch opened with
decisions D1–D12 against the Phase-16 coherence audit's F1–F3).

**Done when.** `make test && make lint && make down && make lake-reset
CONFIRM=yes && make up && make seed PROFILE=tiny && make run-hot && make eval &&
make test-int && make test-int-lakehouse && make test-int-long-delay` — tiny
through the lake is the gate-0 proof (a clean stack is a clean lake); long_delay
is the reconcile-through-lake + 0.587→0.973 proof.

**Delivered (2026-08-21).** `candidate_households` on the attributed row (the
engine keeps the full candidate set at deferral time; 19-column contract pinned
across model/loader/oracle/DDL/Iceberg/read-back) so reconciliation explodes the
row and needs no device graph or broker — the BACKLOG "land `device_graph` as a
lake table" fix is superseded; `raw.exposures` + `raw.attributed_conversions`
partitioned `day × bucket(8, household_id)` (N a table property, guarded);
engine → lake → Dagster load (touched days) → ClickHouse on every path,
`--lake-land` / dual-write gone, the direct sink now `tests/oracle.py`;
bucket-aligned reconcile over the lake == the single pass byte-for-byte
(long_delay, shared_ip_spike); `make replay-serving` (Kafka-free), `make
lake-reset` (one of three sanctioned destructive paths, all in `lake/destructive.py`; per-profile lake root), `make
lake-maintain` (Dagster job). Every pin in `tests/pins.py` unchanged; the tiny
golden re-frozen once for the additive column (0 decision changes — now a rule,
DECISIONS Phase 17). Found live: clickhouse-connect writes naive datetimes as
local time (ARCHITECTURE §8).

## Phase 19 — Docs reshape (RECONCILED)

**Goal.** Move, merge, delete — never invent. The reader meets the constraint
equation before the phase history: README first screen (≤ 60 lines) = what it is →
the measured constraint (~571 B/exposure → ~8.6 TB) → the two-path answer → the
pinned accuracy/restatement table → the Phase-13 cost-lever table → the 30/30
agent line → the honesty boundary → one demo command; the phase table moves from
CLAUDE.md to README `## History`; DECISIONS gains a "Decisions still in force"
section (≤ 20 entries, grouped by component, superseded entries annotated in place,
nothing deleted) over the chronological per-phase appendix; ARCHITECTURE §3 is the
post-17 end state; one docs guard, `make check-docs` (`scripts/check_docs.py` =
`docs/check_runbook.py` moved + extended: links/anchors, generated blocks, exact-token
traces). Docs-only. Reordered 2026-08-22 to run BEFORE 18a/18b (DECISIONS "Process"
entry): the consolidation removes the drift tax every later phase would otherwise
pay again. Spec: `specs/phase-19-docs-reshape.md` — reconciled against main as its
branch's commit 1 (the earlier goal text here named a `streaming/` rename and a
BACKLOG triage the spec never carried; corrected at exit — the spec is authoritative).

**Done when.** `make test && make lint && make check-docs` — plus the three hand-run
negative tests (a broken anchor, a stale generated block, a renamed guard) each
failing `make check-docs`, pasted in the PR body.

**Delivered (2026-08-22).** README first screen ≤ 60 lines (`make check-docs` asserts it); `make check-docs` (links /
anchors across README + docs/, the two generated blocks under their generators'
markers with the README copies compared, exact-token traces + every `make <target>`
the docs name); BACKLOG 37 closed (partial-rename failure pinned by
`tests/test_check_docs.py`), 47 re-deferred (trigger: next `tests/pins.py` change);
the `check-runbook` target gone (Makefile, CI lint job, CLAUDE.md). No pin, golden or
pipeline file changed.

## Phase 18a — Cost and ops levers: incremental rollup, dirty-set gate, part-count and merge-lag (PROPOSED)

**Goal.** Incremental rollups from a dirty set (the Phase-16
`engine_conversions_ambiguous_deferred_total` / `reason` column are its
precursors) with the loader-owned dirty-set gate (BACKLOG, the loader-owned dirty-set row), part-count and
merge-lag metrics + alert rules; the alert rules get recaptured here (revisit the
`MatchRateOutOfBand` headroom then). Split 2026-08-22 from Phase 18 under the
phase-size rule (≤ ~6 pinned decisions / Done-when items per spec; CLAUDE.md
Workflow rules). Spec: `specs/phase-18a-cost-and-ops.md` — carries a "Pre-branch
reconciliation required" banner; the branch's commit 1 is that amendment.

## Phase 18b — Cost and ops levers: async inserts, query cost, BACKWARD compat, live alert firing (PROPOSED)

**Goal.** Async inserts measured, a query-cost table (`query_cost_daily`), schema
compatibility BACKWARD, the live alert firing path (Pushgateway) + webhook
`groupKey` dedupe, the shard-key note. The other half of the 2026-08-22 split;
depends on 18a merged. Spec: `specs/phase-18b-cost-and-ops.md` — same
reconciliation banner, same commit-1 rule.
