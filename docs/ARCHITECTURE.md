# CTV Attribution Pipeline — Architecture

*A streaming attribution engine with an AI measurement-integrity agent.*

This document is the spec. Code that contradicts it is either a bug or a reason
to update this file first.

---

## 1. Why this problem

Streaming TV (CTV) is increasingly sold as a **performance** channel: advertisers
expect the precision and transparency they get from search and social, but on TV.
The hard part, and the most-cited pain point in the category, is **measurement and
attribution**.

The core difficulty: the ad is seen on a TV, but the conversion happens on a phone
or laptop. Bridging that gap means joining two high-volume, separately-keyed event
streams across a household/device graph, inside a configurable time window, while
handling duplicates and conversions that arrive days later.

Every attribution method has a blind spot. Click attribution barely registers a
channel that rarely produces clicks; view-through attribution treats "conversion
followed impression" as causation and over-credits about as often as it
under-credits. That ambiguity is why advertisers distrust the numbers, and why
getting the pipeline right matters commercially.

## 2. Scope and honesty boundary

This is a **simulation of the pipeline shape** attribution requires, not a
reproduction of any vendor's proprietary device graph or third-party integrations.
The goal is to demonstrate the engineering: two-stream ingestion, device-graph
resolution, windowed cross-device joins, dedup, late-event handling, a
reconciliation path, an OLAP serving layer with restatements, and the operational
tooling around all of it.

**Scale posture.** The pipeline runs end to end on a laptop. It does not attempt a
live 500k/sec demo; that is impossible on free
infrastructure and fools no one. Instead the **scaling story is a written
deliverable** (`SCALING.md`): where the design breaks at 50k/sec and 500k/sec, and
precisely what changes at each tier.

**Cost posture.** Runs on a 16 GB laptop in Docker Compose. Agent evals cost under
$10 in API tokens.

## 3. Architecture

### 3.1 Design principles

- The pipeline is **deterministic given a seed**.
- The agent is **read-only and off the critical path**; run with the agent off and
  attribution output is byte-identical.
- Every event carries both `event_time` and `ingest_time`, so lateness is
  measurable, not assumed.
- Every write to the serving layer is **idempotent**, so replays and corrections are
  safe by construction.
- Two attribution paths, one truth: a **hot streaming path** for the near-term
  window, and a **reconciliation path** for the long tail. Both write to the same
  table with the same key.

### 3.2 Diagram

```
PRODUCER (seeded)
  ├─ device graph: household → devices → IPs (shared-IP noise knob)   [reference data]
  ├─ exposures   keyed household_id ─────────────────┐
  ├─ conversions keyed device_id ──────┐             │
  └─ truth links (hidden from pipeline)│             │
                                       v             v
REDPANDA           topics: exposures | conversions
                   + schema registry (JSON Schema per topic)
                   + device_graph (compacted topic, reference data)
                                       │
          exposures ───────────────────┤
                                       v
ATTRIBUTION ENGINE (deterministic batch attributor — no stream framework)
                   ├─ resolve step, in-process: conversion → device graph lookup →
                   │  household_id (device hit, else IP fallback; shared IP = ambiguous)
                   ├─ hot path: join both sides on household_id
                   ├─ hot window state (configurable, default 7d of exposures)
                   ├─ last-touch match, all candidates recorded as assists
                   ├─ ambiguous shared-IP conversion → UNATTRIBUTED (reason ambiguous_ip),
                   │  never a hot guess; reconciliation owns it
                   ├─ dedup on exposure_id / conversion_id (seen-set; TTL'd under continuous follow)
                   ├─ watermarks + allowed lateness (minutes–hours late)
                   └─ emits attributed + unattributed conversion records; a deferred row
                      carries its full candidate_households
                                       │  lands (append) — never writes ClickHouse rows
                                       v
ICEBERG LAKE (system of record)   raw.exposures · raw.attributed_conversions
                   both partitioned day(event_time) × bucket(N, household_id), N a table
                   property; append-only logs (hot row, later the reconciled row);
                   "current row" = argMax(processed_at) in SQL (DuckDB here; Spark/Trino at scale)
                                       │  Dagster load, driven by the days TOUCHED
                                       v
CLICKHOUSE (derived serving projection, loaded from the lake; replayable with no Kafka)
                   attributed_conversions  (ReplacingMergeTree, key conversion_id, version processed_at)
                   exposures_landed        (raw exposures, for the naive benchmark + reconcile parity)
                   campaign_hourly         (rollups, refreshed on schedule; not insert-triggered)
                   report_snapshots        (reported_at × period → metrics; enables restatements)
                                       ^
RECONCILIATION JOB (periodic, reads the LAKE, no broker)
                   current hot-unattributed rows of raw.attributed_conversions per day:
                   state-miss → bucket-local join vs raw.exposures in [day−90d, day];
                   ambiguous_ip → EXPLODE over candidate_households BEFORE bucketing →
                   bucket-local join → reduce by conversion_id across buckets, most-recent
                   exposure wins (the ONE tiebreak) → append corrected rows to the lake
                   → reload touched days → refresh rollups → write new report snapshot
                                       │
REPORTING          ROAS / CPA / CVR / site-visit rate
                   restatement view: metric for period P as of time T
                   naive (full-scan) vs optimized (rollup) benchmark

──── off the critical path ──────────────────────────────────────────────
PROMETHEUS ← input backlog, resolve ambiguity rate, join-state size, match rate,
             watermark lag, insert batch size, reconciliation volume
     │
GRAFANA (dashboards)      ALERTMANAGER (deterministic thresholds)
                                       │  webhook
                                       v
ATTRIBUTION-INTEGRITY AGENT (§4)  read-only ClickHouse user, probe registry, typed findings
```

### 3.3 Components

#### Producer

Seeded generator. First builds the **device graph**: N households, each with a few
devices and one or more IPs; a configurable fraction of IPs are shared across
households (CGNAT / office / campus), which is the sole source of wrong-household
matches. Then emits two streams.

- **Exposures** (TV ad impressions), keyed by household: `exposure_id`,
  `event_time`, `ingest_time`, `campaign_id`, `household_id`, `ip`, `app_id`,
  `program_genre`, `spend`.
- **Conversions** (pixel fires), keyed by device: `conversion_id`, `event_time`,
  `ingest_time`, `device_id`, `ip`, `conversion_type` (`site_visit` | `purchase`),
  `revenue`, `order_id`.
- **Truth links**: a controllable fraction of conversions are caused by a prior
  exposure and carry a hidden `truth_exposure_id`, written to a side file the
  pipeline never reads. The rest are organic.
- **Knobs**: throughput; late-arrival injector (delta between `event_time` and
  `ingest_time`, from minutes to days); duplicate injector; shared-IP fraction;
  unknown-device fraction (conversions from devices the graph never learned —
  guest/roommate/id churn — forcing the resolve step's IP fallback);
  co-view multiplier per genre; fault profiles (§4.3).

Schemas are pydantic models; JSON Schemas are generated from them and registered.

#### Redpanda

Two event topics plus one reference topic. `exposures` is partitioned by
`household_id`; `conversions` by `device_id` (the engine re-keys it to
`household_id` in-process at the resolve step). `device_graph` is a compacted
topic holding the current graph. Each topic has a JSON Schema in the registry;
the producer and every consumer validate against it. Partition count is a
documented scaling lever (SCALING.md notes the household-keyed re-partition a
continuous multi-partition engine would need). There is no intermediate
resolved-conversions topic: the resolve step is in-process (why: DECISIONS
"Decisions still in force" → resolve; history: DECISIONS Phase 16).

#### Resolve step (in-process, `resolve/`)

A pure function the engine calls per conversion — `resolve_one(conversion,
graph) -> list[ResolvedConversion]` — with the device graph loaded once from the
compacted topic at startup. Look up `device_id` in the graph; if found, one
record for that household. If not found, fall back to IP; if the IP maps to
several households, one record per candidate with an ambiguity flag and
candidate count (the shared-IP fan-out). Device-graph match beats IP match.
The hot path does **not** pick among ambiguous candidates: a
`candidate_count > 1` conversion is emitted unattributed (reason ambiguous_ip)
and reconciliation — which holds every exposure — applies the most-recent-
exposure rule across the candidate households. `resolve/` is a module, not a
service; it becomes a separate service again when the device graph is owned by
another team or a vendor — the interface is the function, not a topic
(DECISIONS Phase 16). Metrics (`resolve_`, emitted from the engine process): resolve
rate, ambiguity rate, fan-out factor, input backlog.

#### Attribution engine (hot path)

A deterministic batch attributor (`streaming/dataflow.py` drives the pure core
in `streaming/attribute.py` directly; no stream framework — DECISIONS
"Decisions still in force" → engine). It drains `exposures` and `conversions`, resolves in-process, and
joins on `household_id`. Read the phrases below ("when a conversion arrives",
"kept for the hot window") as the semantics of a **batch drain** with event-time
windowing, not a live continuous follow: the engine drains both topics once and
runs an arrival-ordered, watermark-gated, evicting pass per household (§8
gotcha; DECISIONS Phase 5).

- **Hot window state**: exposures kept for the hot window (default 7 days),
  evicted by watermark. Window-state size is the central scaling constraint.
- **Matching**: when a conversion arrives, find candidate exposures in the same
  household within the attribution window; credit the last one (**last-touch**),
  record the others as **assists**, emit an attributed record. If none, emit an
  **unattributed** record so reconciliation can retry later.
- **Ambiguous deferral**: the hot path attributes only when the
  household is certain — a device hit or a single-owner IP. A shared-IP
  conversion (`candidate_count > 1`) is collapsed to one placeholder row
  (lowest `household_id`) and emitted **unattributed, reason ambiguous_ip** —
  never a hot guess. Exactly one row per `conversion_id` still reaches
  ClickHouse, so `conversion_id` stays a safe ReplacingMergeTree key;
  `processed_at` is the version, never a tiebreaker (DECISIONS "still in force"
  → serving; history Phase 2/3/16).
  The cross-household most-recent-exposure pick lives in reconciliation only.
- **Dedup**: on `exposure_id` and `conversion_id`. The batch drain keeps
  a full seen-set (it already holds the whole topic in memory); TTL'd eviction
  sized to the max plausible duplicate delay is the continuous-follow target, not
  the batch mechanism — the seeded duplicate is timestamp-identical to its
  original, so an event-time TTL has nothing to size against (see §8, DECISIONS
  Phase 5, SCALING.md).
- **Lateness**: a conversion is a **pure probe** — it is never dropped by a
  conversion-side lateness gate. The watermark (`max(event_time) −
  allowed_lateness`) only gates *when* a conversion is released for matching and
  *when* an exposure is evicted. A conversion becomes **unattributed** for one
  reason — its matching exposure has aged out of the hot window (a state-miss),
  which happens when arrival lateness exceeds the tolerance — and is then picked
  up by reconciliation. (The `medium` profile keeps late ≤ `allowed_lateness`, so
  it has no state-misses; `long_delay` exercises the path end to end.)
- Every emitted record carries `processed_at` and `path` (`hot` | `reconciled`);
  an unattributed record also carries `reason` (`ambiguous_ip` | `state_miss`,
  null once credited) — the explicit contract reconciliation and the agent read
  instead of re-deriving it from `candidate_count` — and a deferred record
  carries `candidate_households`, the full sorted owner set of the shared IP:
  the array is the truth, the placeholder `household_id` is the key (DECISIONS
  Phase 16 / 17). The engine never writes ClickHouse rows: it lands its output in the lake
  (below) and the Dagster load carries it to the serving tables.

#### Lake of record (`lake/`)

A local **Iceberg** lake (SqlCatalog on SQLite, `file://` warehouse under
gitignored `data/lake/<profile>/`) is the system of record; ClickHouse is derived
from it. Two raw tables, both partitioned **`day(event_time)` × `bucket(N,
household_id)`** with N recorded as a table property (laptop default 8, identical
on both tables, set once per deployment — SCALING.md):

- `raw.exposures` — the engine's deduped exposures.
- `raw.attributed_conversions` — the ClickHouse table's 19 columns in the same
  order (a pinned contract: model = loader = DDL = Iceberg schema = reconcile
  read-back).

Both are **append-only logs**: the hot row and, later, the reconciled row for the
same `conversion_id` both live here; exact re-lands accumulate. "Current row per
key" is a READ-side question answered in SQL (`argMax(processed_at)`, the
ReplacingMergeTree rule) — never assumed. The lake → ClickHouse **load** is a
Dagster asset per (table, day), materialized for exactly the days a landing
TOUCHED (`land()` returns them; a late row lands in an old day and reloads that
day) — never for "today". Loading is idempotent because the ReplacingMergeTree
collapses the re-insert. `make replay-serving` rebuilds the serving tables from
the lake with no Kafka involvement (Kafka retention is hours; the lake is
forever). Hygiene is a Dagster job (`make lake-maintain`: compact a day's small
files, expire old snapshots), off the write path. Iceberg metadata (snapshot
ids, commit times) is non-deterministic and carved out of the byte-identical
guarantee exactly like the agent; every asserted check reads rows back.
DuckDB is the laptop compute; the SQL is the contract and Spark/Trino the port.

#### ClickHouse (serving layer — a derived projection)

Loaded from the lake by the Dagster load assets (`lake/load_serving.py` is the
one writer of the two landed tables; the direct engine sink lives on only as the
test oracle, `tests/oracle.py` — DECISIONS Phase 17).

- `attributed_conversions`: **ReplacingMergeTree** keyed on `conversion_id` with
  `processed_at` as version, so a replay or a reconciliation correction supersedes
  the earlier row. Readers use `FINAL` or `argMax` at read. Inserts are synchronous
  today; async inserts are a scaling lever (see SCALING.md), not a current property.
- `exposures_landed`: raw exposures, for the naive benchmark and the reconcile
  source-equivalence proof.
- `campaign_hourly`: rollup table **refreshed on a schedule** (or a refreshable
  MV), never an insert-triggered summing MV, so corrections cannot double-count.
- `report_snapshots`: per refresh, metrics for each (campaign, period) with
  `reported_at`, which makes restatements queryable.
- `eval_meta`: a single-row marker (the profile string) the populate path stamps
  so `make eval` refuses to score a profile whose truth file does not match the
  populated DB (BACKLOG 43). OFF the golden-compared path — not attribution data,
  no version/timestamp, so it is deterministic and gate-0 stays byte-identical.
- Sort keys chosen for the query pattern (`campaign_id`, `hour`).
- A SELECT-only user exists for the agent.

#### Reconciliation job (periodic, reads the lake)

Per event-time day, selects the CURRENT hot-unattributed rows of
`raw.attributed_conversions` whose `event_time` is still within the long window
(up to 90 days) — two channels, told apart by `reason` (DECISIONS Phase 17):
**state-misses** (certain household, causing exposure aged out of the hot
window) join bucket-locally — only the `raw.exposures` partitions in `[day −
90d, day]` with the same `bucket(household_id)`; **ambiguous_ip** rows are
**exploded** into one row per entry of their persisted `candidate_households`
BEFORE bucketing, each exploded row joins bucket-locally, and the results are
reduced by `conversion_id` ACROSS buckets with the most-recent-exposure rule
(ties: `exposure_id`, then `household_id`) — the one implementation of that
tiebreak (`reconcile.pick_household`), the same last-touch leaf per household.
The architecture's claim is no fan-out on the HOT path; batch fan-out with the
full 90-day picture is cheap and correct. No device graph, no broker. Appends
corrected rows (`path=reconciled`, a strictly later `processed_at`) to the lake,
reloads the touched days into ClickHouse, triggers a rollup refresh, and writes a
new report snapshot. This is the second attribution path and it is what makes a
90-day window possible without 90 days of processor state; advertisers get a
late correct credit instead of a fast wrong one.

#### Reporting

The four advertiser metrics (ROAS, CPA, CVR, site-visit rate) plus a restatement
query ("ROAS for day D as reported on day D+1 vs now"). Co-view multiplier is
applied here, at read time, keyed by genre. The benchmark harness runs the same
questions against `exposures_landed` + `attributed_conversions` with a full scan
versus `campaign_hourly`, reporting latency, rows read, bytes read, and why each
change worked.

#### Observability

Prometheus metrics from producer, engine (including the in-process resolve step's `resolve_` series), the lake → ClickHouse load (`lake_rows_loaded_total{table}`, emitted by whichever process ran the load), and reconciliation job.
Grafana dashboards committed as JSON. Alertmanager rules for the deterministic
conditions (lag, watermark stall, match rate outside band, restatement magnitude).
Alerts fire a webhook to the agent, which is the second-stage triage.

### 3.4 Decisions and their reasons

| Decision | Chosen | Why |
|---|---|---|
| Where device-graph resolution happens | In-process map step inside the engine (`resolve/` module, `resolve_one`) | A separate consumer/topic/subject bought nothing for an in-memory dict lookup; the seam worth keeping is the function, not the topic. Becomes a service again when the graph is owned by another team or a vendor (DECISIONS Phase 16) |
| Attribution rule | Last-touch, assists recorded | Industry default; multi-touch becomes a query, not a re-run |
| Long window | Hot path (7d) + periodic reconciliation | 90d of processor state is infeasible at any real throughput |
| Write model | ReplacingMergeTree + scheduled rollup refresh | Simplest model that stays correct under replays and corrections |
| Restatements | `report_snapshots` with `reported_at` | Advertisers care; agent's late-arrival detector needs it |
| Co-viewing | Read-time multiplier | Keeps the join clean |
| Stream processor | None — a deterministic batch attributor in plain Python | A stream framework over a bounded drain did no work (DECISIONS Phase 16). Continuous follow on a real framework (Bytewax proper vs Flink) is a Phase-18+ decision; SCALING.md keeps the Flink mapping as the port target |
| System of record | The Iceberg lake; ClickHouse a derived, replayable projection loaded from it | A dual-write with no transactional boundary is a drift generator, and an optional lake leaves ClickHouse as the record. Replay/backfill come from the lake, never Kafka retention; the 90-day reconcile is a partition-pruned (`day × bucket(household_id)`) join, not a scan (DECISIONS Phase 17) |
| Ambiguous rows under bucketing | Persist `candidate_households` on the deferred row; explode per candidate → bucket-local join → cross-bucket reduce | The placeholder sits in one bucket while its true candidates hash to others; the engine knew the set at deferral time and keeps it. No fan-out on the HOT path; batch fan-out is cheap and correct |

### 3.5 Out of scope for v1

Co-viewing inside the engine, multi-touch attribution models, schema evolution
beyond v1. Listed in README "Next steps".

(Iceberg landing was originally out of scope here; the reversal and its two
steps are DECISIONS Phase 12 / 17 — see §3.3 "Lake of record" and §5. Still out
of scope: an object store + REST catalog, Spark/Trino execution, continuous
follow — the SCALING.md tier notes.)

## 4. The agent: attribution-integrity guardian

**Why an agent earns its place here.** An agent is only worth its cost where the
work is ambiguous, high-volume, and judgment-heavy. Threshold alerts already catch
"lag is high." What they can't catch is a **plausible-but-wrong attribution
number**: the ROAS an advertiser is about to act on looks fine but is inflated by a
device-graph mismatch, a window edge effect, or a late-arrival restatement.

**Value proposition in one sentence.** The agent protects measurement integrity by
catching attribution numbers that are probably wrong, and explaining why, before an
advertiser sees them, doing the cross-signal reasoning a data engineer would
otherwise do by hand.

### 4.1 What it watches

- **Match-rate anomalies**: jump/drop in the share of conversions attributed.
- **ROAS / CPA discontinuities**: per-campaign shifts too large or too fast to be
  organic.
- **Co-viewing inflation**: an implausibly high per-genre reach. This is a
  **capability boundary**, not a scored diagnosis: the only serving-data signal is
  RAW per-genre reach, which does not discriminate co-view inflation from noise (the
  co-view-adjusted factor is a won't-do — DECISIONS Phase 10 / BACKLOG 26), so the
  agent's correct outcome is to abstain (AMBIGUOUS_NEEDS_HUMAN), not to name it.
- **Attribution-window edge effects**: spikes clustered at the window boundary.
- **Wrong-household matches**: clusters of conversions matched via shared IPs.
- **Late-arrival distortion**: a restatement that materially changes a period's
  reported ROAS after advertisers may have acted on it.

### 4.2 The loop — bounded, auditable, read-only

- **Trigger**: Alertmanager webhook (deterministic condition fires first) or a
  scheduled sweep.
- **Observe**: collectors snapshot the attribution output and supporting signals
  into a typed `AttributionContext` (match rate over time, per-campaign metric
  deltas, window-edge distribution, IP-cluster stats, restatement volume). Collectors
  are deterministic and testable without an LLM.
- **Hypothesize**: propose likely causes from a **typed catalog** (device-graph
  mismatch, window edge effect, co-view inflation, late-arrival distortion, real
  performance change, upstream data change).
- **Test**: run bounded follow-up queries from a **probe registry** (named,
  parameterized SQL exposed as tools; no free-form SQL) against a **SELECT-only
  ClickHouse user** to confirm or deny each hypothesis.
- **Rank**: order hypotheses by evidence weight; state confidence.
- **Report**: emit a typed `AttributionFinding` with `evidence_for`,
  `evidence_against`, `ruled_out`, a recommended action (e.g. "hold this
  campaign's ROAS pending review"), and a verdict `CONFIDENT` |
  `AMBIGUOUS_NEEDS_HUMAN`. When signals conflict it escalates with an evidence dump
  instead of guessing.

**Determinism guarantee.** Read access only, enforced at the database. It flags
and recommends; humans and deterministic config act. Outputs are schema-constrained.

### 4.3 How it's proven — validate against ground truth

- **Attribution accuracy**: precision/recall of the engine's **household**
  attribution against the household of `truth_exposure_id` (household grain).
  The engine is last-touch, so it credits the most-recent in-window exposure,
  not necessarily the causal one — scoring exact `exposure_id` equality would
  measure last-touch-vs-causal coincidence (a model property), not attribution
  quality; household grain isolates the real failure mode, wrong-household
  (shared-IP) attribution. Exact-exposure-id match MAY be reported as a labeled
  diagnostic, never as the headline accuracy.
- **Agent accuracy**: diagnosable fault profiles (shared-IP spike, late burst, real
  performance lift), each scored on whether the agent's top hypothesis is correct,
  plus **two controls the agent must correctly leave alone** — a **no-fault baseline**
  and **duplicate_flood** (dedup absorbs it; ClickHouse carries no fingerprint, so the
  correct output is no-fault) — each run repeatedly, producing a *fault → top
  hypothesis → correct?* table with a **false-positive rate** measured on the controls.
  (The **co-view multiplier bug** is a third category — a real fault that is NOT
  diagnosable from serving data: the adjusted co-view factor is a won't-do (DECISIONS
  Phase 10 / BACKLOG 26) and raw per-genre reach does not discriminate it, so the
  agent correctly **abstains** — a labeled capability boundary, scored as a
  correct-abstention but kept out of the false-positive denominator.)
- **The near-miss pair**: a genuine performance improvement vs. an inflated match
  rate from shared-IP false positives both raise reported ROAS but demand opposite
  responses. Showing the agent tell them apart on the evidence proves real
  reasoning, not pattern-matching on "ROAS went up."

## 5. Capabilities

| Capability | Where the project delivers it |
|---|---|
| Streaming at scale | Two-stream Redpanda ingestion, in-process resolve, windowed/watermarked engine on a batch drain (framework choice deferred to Phase 18+; SCALING.md Flink mapping) |
| Deep compute / lakehouse | Windowed stateful joins, reconciliation path; the Iceberg lake is the system of record (`raw.exposures` + `raw.attributed_conversions`, `day × bucket(household_id)`), ClickHouse a derived projection loaded by Dagster per touched day, reconciliation a bucket-aligned DuckDB-over-Iceberg join, replay from the lake with no Kafka (Phase 17). Object store / REST catalog / Spark-Trino compute are the SCALING port |
| OLAP reporting stack | ClickHouse: ReplacingMergeTree, scheduled rollups, restatements (synchronous inserts today; async is a scaling lever, SCALING.md) |
| "Faster/cheaper query, and why" | Naive-vs-optimized benchmark with measured deltas and explanations |
| On-call / incident readiness | Prometheus, Grafana, Alertmanager rules, runbook-style SCALING.md |
| Data contracts | Pydantic-derived JSON Schemas enforced via schema registry at produce and consume |
| Ambiguous asks / judgment | Attribution-integrity agent with typed findings and escalation |
| Scale limits before outages | SCALING.md: what breaks at 50k and 500k msgs/sec |
| Responsible AI use | Read-only agent, DB-enforced, off the critical path, schema-constrained outputs |

## 6. Stack and repository

**Stack.** Python 3.12 (uv, ruff, pytest, pydantic). Redpanda (Kafka API, schema
registry). ClickHouse. Prometheus, Grafana, Alertmanager. Anthropic Python SDK.
Docker Compose. No JVM, no stream framework (Bytewax removed in Phase 16).

**Repository shape.**

```
producer/        generator, device graph, profiles/, schemas
resolve/         conversion → household resolution (in-process map step + offline replay)
streaming/       attribution engine: pure core + batch-drain driver
reconcile/       periodic long-window matcher, rollup refresh, snapshots
clickhouse/      DDL, users, migrations
queries/         reporting SQL + benchmark harness
observability/   prometheus, grafana dashboards (JSON), alert rules
agent/           collectors, hypothesis catalog, probe registry, loop, eval
docs/            ARCHITECTURE.md  PHASES.md  SCALING.md  RESULTS.md  RUNBOOK.md
tests/           unit; tests/integration/ against compose
fixtures/        golden tiny-profile data and expected outputs
README.md        problem → architecture → results, reads like a design doc
CLAUDE.md        invariants, commands, conventions
```

## 7. Build order

The plan is `PHASES.md`; what each phase delivered, with its gate and PR, is the
table in [`README.md` → History](../README.md#history); the reasons are
`DECISIONS.md` (per-phase appendix). Each phase stands on its own, so the project
degrades gracefully: even without the agent, a clean two-stream attribution spine
with reconciliation, benchmark, and ground-truth accuracy stands on its own.

## 8. Gotchas

*Populated during the build: stack behaviours that surprised us and how they were
handled.*

- **Pydantic docstrings become JSON Schema `description`s, and any change
  registers a new schema version.** The registry compares the full schema
  document, so editing a model's docstring bumps the subject version even
  though no field changed (observed on `device_graph-value` during
  Phase 1). Harmless under BACKWARD compatibility, but don't be surprised
  by cosmetic version bumps.
- **`localhost` vs `127.0.0.1` for Redpanda from the host.** The compose
  ports bind IPv4 only, but the broker advertises `external://localhost:19092`,
  so librdkafka occasionally logs a one-line `Connect to ipv6#[::1]`
  connect-refused before falling back to IPv4. Benign; clients default to
  `127.0.0.1` to minimize it.
- **The engine is a batch drain, not a continuous follow.** (Phase 3 origin:
  Bytewax's Kafka source follows forever — `bytewax.connectors.kafka` is an
  unbounded source that never signals end-of-input, so a dataflow built on it
  would not terminate on the finite seeded stream. The engine instead drained
  both topics to memory once and fed a bounded `TestingSource`.) Phase 16 removed
  Bytewax entirely — the wrapper only regrouped lists — and the engine now drains
  the two event topics start→end once (EOF-driven, `common.kafka.drain`) and runs
  the pure core in-process. The drain also guarantees every fan-out row for a
  `conversion_id` is present when `one_row_per_conversion` collapses it
  (DECISIONS Phase 3 (b)). Windowing (watermarks, allowed lateness, eviction)
  landed in Phase 5 **on the batch drain**; continuous Kafka follow and the
  framework to run it on (Bytewax proper vs Flink) are a Phase-18+ decision — the
  two resolve BACKLOG rows re-defer on exactly that trigger.
- **The seeded duplicate is timestamp-identical to its original, so batch dedup
  is a full seen-set, not TTL'd.** The duplicate injector re-appends the *same
  payload* (`producer/generate.py` `_with_duplicates`); the later arrival slot is
  a sort key for emit order and is discarded, never a field. So a re-send carries
  the same `event_time` AND the same `ingest_time` as its original — nothing an
  event-time TTL could measure against differs between the pair, and a TTL sized
  to the 300s re-send delay would sit on a seed-dependent knife-edge (a denser
  stream advances the watermark ~300s of event-time between a pair, evicting the
  id before its re-send; ReplacingMergeTree collapse would mask the undercount).
  The Phase-5 batch drain already holds the whole topic in memory, so it keeps a
  full `conversion_id`/`exposure_id` seen-set (O(n), deterministic on the single
  partition). TTL'd eviction is the continuous-follow story only (SCALING.md,
  DECISIONS Phase 5).
- **ClickHouse 24.8 images lock the `default` user to loopback.** With no
  `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`, the entrypoint writes a
  `users.d/default-user.xml` limiting `default` to `::1`/`127.0.0.1`; host
  clients arrive via the docker gateway IP and get `AUTHENTICATION_FAILED`,
  while the in-container healthcheck (local socket) still passes — so `make up`
  reports healthy but the engine can't connect. Fixed with a mounted
  `users.d/allow-network.xml` (DECISIONS Phase 3).
- **`create ... ;` split must ignore semicolons in SQL comments.** A `;` inside
  a `--` comment in `clickhouse/ddl.sql` split a statement into a comment-only
  chunk that ClickHouse rejects as an empty query; `clickhouse/apply.py` strips
  line comments before splitting on `;`.
- **clickhouse-connect renders DateTime columns in the client's local timezone,
  so a datetime read from ClickHouse and written back lands at a different
  wall-clock across processes.** Reading `max(ingest_time)` into Python and
  re-inserting it stamped the Phase-6 `report_snapshots.reported_at` 6h apart (the
  local UTC offset) between the `make run` subprocess and an in-process caller —
  so the "two snapshots per run" became four and the restatement's before/after
  collapsed. Fix (Phase 6): the reconciliation job never round-trips a timestamp
  through Python for storage — `reported_at` is computed **server-side** as
  `max(ingest_time) + offset_ms` inside the INSERT, and `_max_ingest` reads an
  **epoch-millis integer** (`toUnixTimestamp64Milli`, timezone-free) for the
  `reconciled_at` version. The exposure is READ-side only: `client.insert` of a
  timezone-aware UTC datetime (the producer emits `AwareDatetime` UTC, preserved
  through the model) stores the correct instant regardless of client offset — the
  engine's own `event_time`/`ingest_time`/`processed_at` are NOT shifted (verified:
  `min(event_time)` in `exposures_landed` equals `sim_start`
  `2026-08-01T00:00:00Z` exactly, `toStartOfHour` on the correct UTC axis). Rule:
  when a stored timestamp must round-trip through Python for storage or
  cross-process comparison, read it as `toUnixTimestamp64Milli` or compute it
  server-side — never as a rendered DateTime.
- **A ClickHouse aggregate aliased to a filtered column name raises
  ILLEGAL_AGGREGATION.** `select count() as attributed ... where attributed = 1`
  fails with code 184: ClickHouse binds the `attributed` in `WHERE` (and inside
  `countIf(attributed = 1)`) to the SELECT alias — the `count()` aggregate — rather
  than the table column, and an aggregate in a filter is illegal. Observed while
  authoring the Phase-8 collector reads (`agent/readers.py`). Fix: alias aggregates
  to names that don't collide with any filtered column (`attributed_count`,
  `ambiguous_count`); consumers that unpack rows positionally don't care about the
  name. Same class as the ORDER BY/GROUP BY alias-vs-column ambiguities — when an
  alias shares a column's name, the filter is the surprising place it bites.
- **A `FINAL` scan's `read_rows` counts un-merged version-parts, not logical
  rows.** A ReplacingMergeTree keeps every superseded version as physical rows in
  separate parts until a background merge collapses them; `SELECT ... FINAL`
  returns the collapsed result but ClickHouse still physically **reads** all the
  un-merged versions to do the collapse. So `read_rows`/`read_bytes` (from
  `X-ClickHouse-Summary`) grow with each un-merged write and *drift with
  background-merge timing* — they are NOT the logical table size. It bit the Phase-7
  benchmark: `campaign_hourly` (rewritten wholesale each rollup refresh) and
  `attributed_conversions` (a new higher-`processed_at` version per reconciled
  conversion) both version-stack, so `make bench` measured a different rollup
  read-size depending on how many refreshes/corrections had happened and whether a
  merge had fired — in CI, running right after a test that refreshed twice more, the
  rollup measured 1020 physical rows and *lost* to the naive scan (0.8×), while
  locally after one refresh it read 340 and won 2.5×. Fix (`queries/bench.py`
  `queries/bench_common.py` `canonicalize`): `OPTIMIZE TABLE ... FINAL` every read
  table before measuring, so
  `read_rows` reflects merged steady state — deterministic, re-run-identical, and the
  honest apples-to-apples comparison (a scheduled rollup serves its merged form in
  production). `OPTIMIZE ... FINAL` is synchronous on single-node (`alter_sync=1`)
  and a no-op when already merged (`optimize_throw_if_noop=0`) — both relied-upon
  ClickHouse **defaults**, not overridden in `clickhouse/`; overriding either would
  silently reintroduce this non-determinism (a re-run could throw, or measure before
  the merge completes). Rule: never treat a `FINAL` scan's `read_rows` as a stable
  structural number without first forcing the merge.
- **`toDecimal64(<Float64>, s)` TRUNCATES the binary value; a Float64 `sum()` is
  not order-independent.** Two ClickHouse facts behind RUNBOOK incident 3. (1)
  `sum(spend)` over the same rows (camp-01, long_delay) gave `8.529999999999996`
  and `8.53` depending
  on the order the parts were visited, so two reconcile passes wrote
  "identical" versioned rows that differed in the 15th digit. (2) The Decimal
  fix's first cut, `toDecimal64(revenue, 4)`, truncated the binary float:
  `26.08` (really `26.0799999…`) became `26.0799`, understating revenue by
  4e-4 (5,228 of the 100,000 cent values 0.00–999.99 lose a ten-thousandth;
  `round()` and `accurateCast` do not help). The exact path is
  `toDecimal64(toString(x), 4)` — the shortest decimal string parses exactly —
  and it is exact only because the producer quantizes money to cents (pinned).
  Money as Float64 is the root cause; Decimal64(4) end-to-end is the BACKLOG
  destination.
- **A projection on a ReplacingMergeTree needs `deduplicate_merge_projection_mode`
  set, and cannot serve a `FINAL` query.** Adding a projection to a
  ReplacingMergeTree (Phase-13 cost levers, `queries/cost_levers.sql`) raises code
  344 (`SUPPORT_IS_DISABLED`) on ClickHouse 24.8 unless the table sets
  `deduplicate_merge_projection_mode = 'rebuild'` (or `'drop'`) — ClickHouse needs to
  know how the projection is handled when a dedup merge collapses versions;
  `'rebuild'` recomputes the projection on merge. Second surprise: even once built, a
  projection is **not** used for a `SELECT ... FINAL` query — ClickHouse can't
  guarantee the projection copy is deduplicated to the same latest-version rows the
  base table's `FINAL` returns, so it falls back to the full base scan. The Phase-13
  projection lever is therefore measured non-FINAL, valid because the table is
  canonicalized to a single version per key first (so FINAL and non-FINAL return
  identical rows). Both facts kept the projection off the golden DDL path
  (`clickhouse/ddl.sql`) — it is added only inside `make cost-levers` against the
  `bench_large` run, so gate-0 tiny golden stays byte-identical.
- **clickhouse-connect writes a NAIVE datetime as the client's LOCAL wall-clock,
  but reads a `DateTime64(3,'UTC')` column back as a naive UTC wall-clock.** The
  mirror image of the gotcha above, on the write side, and asymmetric with it: a
  value read back naive-UTC and re-inserted naive is shifted by the machine's UTC
  offset (probe on an MDT laptop: naive `12:02:51` stored as `18:02:51`; the same
  instant inserted tz-aware UTC stored exactly). Found by inspection in Phase 17
  when the first lake-loaded rows sat +6h off the in-memory oracle — and it had
  been shifting the reconciled rows' `event_time`/`ingest_time` since Phase 6 on
  every non-UTC machine (RUNBOOK incident 4). Rule: every datetime is tz-aware UTC
  at every I/O boundary; a naive datetime never reaches a client call
  (`lake/load_serving.py` `_utc`; guard `tests/test_tz_invariance.py`).
- **pyiceberg 0.11.1 snapshot expiry is metadata-only — orphaned data files stay
  on disk.** `table.maintenance.expire_snapshots()` drops old snapshots and
  manifests, but there is no `remove_orphan_files`, so the Parquet files those
  snapshots referenced (and the files a compaction replaced) are never deleted:
  `make lake-maintain` (compaction = a rewrite of the day's data files, row content
  unchanged; then expiry) bounds the LIVE file count, not the directory size
  (measured: 24 → 32 files on disk across one compaction + expiry). Only `make
  lake-reset` reclaims today (BACKLOG 45, re-qualified; pinned so a pyiceberg bump
  fails loud).
- **Phase-17 first-hour stack check (spec D11) — all four supported by the pinned
  versions, no workaround needed.** pyiceberg 0.11.1 + pyiceberg-core, duckdb 1.5.5,
  verified on a scratch catalog before any lake-of-record code was written:
  (1) a `PartitionSpec(day(event_time), bucket[8](household_id))` is accepted on
  `create_table` and the bucket count round-trips as a table property
  (`ctv.bucket_count`); (2) `table.append` WRITES the bucketed layout — 3 days × 8
  buckets landed as 24 Parquet files under `event_time_day=…/household_bucket=…/`;
  (3) `table.maintenance.expire_snapshots().older_than(ts).commit()` exists on this
  version (3 snapshots → 1); (4) DuckDB `iceberg_scan` with an
  `event_time >= … and household_id = …` predicate reported `Total Files Read: 1`
  of 24 — it prunes on BOTH the day and the bucket transform, which is what makes
  the 90-day reconcile join partition-local instead of a scan.
- **ClickHouse has no `alter table … modify engine`, so adding a ReplacingMergeTree
  VERSION to a live table is a rebuild** (Phase 18a, verified on the pinned 24.8
  image: `alter table … add column` is accepted, `alter table … modify engine` fails
  with code 62 — the parser expects STATISTICS / COLUMN / ORDER BY / SAMPLE BY / TTL /
  SETTING / QUERY / SQL SECURITY / DEFINER / REFRESH / COMMENT). The pattern used for
  `report_snapshots` (`clickhouse/apply.py`): add the column → `create <t>_v2 as <t>
  engine = ReplacingMergeTree(version)` → `insert … select * replace (…)` → compare
  row counts → `exchange tables` (atomic on the default `Atomic` database engine) →
  drop the scratch. Nothing is dropped before it has been copied; a crash before the
  exchange leaves the original intact, and one after it leaves a scratch table the
  next apply drops. This matters more than it looks: `report_snapshots` is the one
  serving table `make replay-serving` does NOT rebuild from the lake
  (`orchestration/replay.py` `SERVING_TABLES`), so a drop-and-recreate migration
  would be unrecoverable data loss (BACKLOG).

---

*The one-sentence summary: this project builds the pipeline
attribution actually runs on, a windowed, late-tolerant, cross-device stream join
with a reconciliation path serving an OLAP reporting layer with restatements,
validates it against ground truth, and puts an AI agent where it earns its keep:
guarding the integrity of the numbers advertisers bet their budgets on.*
