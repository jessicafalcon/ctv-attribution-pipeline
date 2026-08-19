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

**Why this beats a generic ad-metrics dashboard.** Aggregating impressions into a
dashboard is table stakes. A stream-to-stream join across a device graph, with
windowed and late-tolerant matching and a reconciliation path, is a harder
data-engineering problem, and it is the machinery an attribution business runs on.
Because the producer emits ground-truth causal links, attribution accuracy can be
scored against truth we control, which almost no portfolio project can do.

## 2. Scope and honesty boundary

This is a **simulation of the pipeline shape** attribution requires, not a
reproduction of any vendor's proprietary device graph or third-party integrations.
The goal is to demonstrate the engineering: two-stream ingestion, device-graph
resolution, windowed cross-device joins, dedup, late-event handling, a
reconciliation path, an OLAP serving layer with restatements, and the operational
tooling around all of it.

**Scale posture.** The pipeline runs end to end at a few thousand messages/sec on a
laptop. It does not attempt a live 500k/sec demo; that is impossible on free
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
REDPANDA           topics: exposures | conversions | conversions_resolved
                   + schema registry (JSON Schema per topic)
                   + device_graph (compacted topic, reference data)
                                       │
                                       v
RESOLVE STAGE      conversions → device graph lookup → household_id(s)
                   republish to conversions_resolved, keyed by household_id
                   ambiguous IP matches fan out (one record per candidate)
                                       │
          exposures ───────────────────┤
                                       v
ATTRIBUTION ENGINE (Bytewax)   hot path: stateful join, both sides keyed by household_id
                   ├─ then a conversion_id-keyed reduction collapses ambiguous
                   │  shared-IP fan-outs to one most-recent-exposure row
                   ├─ hot window state (configurable, default 7d of exposures)
                   ├─ last-touch match, all candidates recorded as assists
                   ├─ dedup on exposure_id / conversion_id (seen-set; TTL'd under continuous follow)
                   ├─ watermarks + allowed lateness (minutes–hours late)
                   └─ emits attributed + unattributed conversion records
                                       │
                                       v
CLICKHOUSE         attributed_conversions  (ReplacingMergeTree, key conversion_id, version processed_at)
                   exposures_landed        (raw exposures, for reconciliation + naive benchmark)
                   campaign_hourly         (rollups, refreshed on schedule; not insert-triggered)
                   report_snapshots        (reported_at × period → metrics; enables restatements)
                                       ^
RECONCILIATION JOB (periodic)          │
                   unattributed conversions still inside long window (up to 90d)
                   → match against exposures_landed → write corrected rows
                   → refresh rollups → write new report snapshot
                                       │
REPORTING          ROAS / CPA / CVR / site-visit rate; co-view factor applied at read time
                   restatement view: metric for period P as of time T
                   naive (full-scan) vs optimized (rollup) benchmark

──── off the critical path ──────────────────────────────────────────────
PROMETHEUS ← consumer lag, resolve ambiguity rate, join-state size, match rate,
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
  guest/roommate/id churn — forcing the resolve stage's IP fallback);
  co-view multiplier per genre; fault profiles (§4.3).

Schemas are pydantic models; JSON Schemas are generated from them and registered.

#### Redpanda

Three event topics plus one reference topic. `exposures` and
`conversions_resolved` are both partitioned by `household_id` so matchable events
land in the same partition. `conversions` is partitioned by `device_id`.
`device_graph` is a compacted topic holding the current graph. Each topic has a
JSON Schema in the registry; the producer and every consumer validate against it.
Partition count is a documented scaling lever.

#### Resolve stage

A small consumer. For each conversion, look up `device_id` in the graph; if found,
emit one record keyed by that household. If not found, fall back to IP; if the IP
maps to several households, emit one record per candidate with an ambiguity flag
and candidate count. The engine treats ambiguous candidates as lower priority:
device-graph match beats IP match; among IP matches, prefer the household with the
most recent exposure inside the window, else drop. Because each candidate is keyed
by its own `household_id`, the household-keyed join cannot compare candidates
partition-locally — the engine applies this "most recent exposure" preference in a
`conversion_id`-keyed reduction *after* the join (see §3.3 engine and DECISIONS.md).
The graph is loaded from the compacted topic at startup and refreshed on change. Metrics: resolve rate,
ambiguity rate, fan-out factor.

#### Attribution engine (Bytewax, hot path)

Bytewax dataflow joining `exposures` and `conversions_resolved` on `household_id`.
Read the phrases below ("when a conversion arrives", "kept for the hot window")
as the semantics of a **batch drain** with event-time windowing, not a live
continuous follow: the engine drains both topics once and runs an arrival-ordered,
watermark-gated, evicting pass in the pure core (§8 gotcha, DECISIONS Phase 5).

- **Hot window state**: exposures kept for the hot window (default 7 days),
  evicted by watermark. Window-state size is the central scaling constraint.
- **Matching**: when a conversion arrives, find candidate exposures in the same
  household within the attribution window; credit the last one (**last-touch**),
  record the others as **assists**, emit an attributed record. If none, emit an
  **unattributed** record so reconciliation can retry later.
- **Ambiguous reduction**: the household-keyed join is followed by a second,
  `conversion_id`-keyed stage. An ambiguous shared-IP conversion arrives as one
  candidate row per household (resolve fan-out); this reduction keeps the
  candidate with the most recent last-touch exposure (ties: `exposure_id` then
  `household_id`), so exactly one row per `conversion_id` reaches ClickHouse.
  `processed_at` is the ReplacingMergeTree version, never the candidate
  tiebreaker (DECISIONS.md, Phase 2).
- **Dedup**: on `exposure_id` and `conversion_id`. The Phase-5 batch drain keeps
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
  up by reconciliation. (Phase-5 `medium` keeps late ≤ `allowed_lateness`, so it
  has no state-misses; the path is exercised by tests and, end to end, Phase 6.)
- Every emitted record carries `processed_at` and `path` (`hot` | `reconciled`).

#### ClickHouse (serving layer)

- `attributed_conversions`: **ReplacingMergeTree** keyed on `conversion_id` with
  `processed_at` as version, so a replay or a reconciliation correction supersedes
  the earlier row. Readers use `FINAL` or `argMax` at read. Async inserts on.
- `exposures_landed`: raw exposures, for reconciliation lookups and the naive
  benchmark.
- `campaign_hourly`: rollup table **refreshed on a schedule** (or a refreshable
  MV), never an insert-triggered summing MV, so corrections cannot double-count.
- `report_snapshots`: per refresh, metrics for each (campaign, period) with
  `reported_at`, which makes restatements queryable.
- Sort keys chosen for the query pattern (`campaign_id`, `hour`).
- A SELECT-only user exists for the agent.

#### Reconciliation job (periodic)

Selects unattributed conversions whose `event_time` is still within the long
window (up to 90 days), joins them against `exposures_landed` by household and
window, writes corrected rows with `path=reconciled`, triggers a rollup refresh,
and writes a new report snapshot. This is the second attribution path and it is
what makes a 90-day window possible without 90 days of processor state.

#### Reporting

The four advertiser metrics (ROAS, CPA, CVR, site-visit rate) plus a restatement
query ("ROAS for day D as reported on day D+1 vs now"). Co-view multiplier is
applied here, at read time, keyed by genre. The benchmark harness runs the same
questions against `exposures_landed` + `attributed_conversions` with a full scan
versus `campaign_hourly`, reporting latency, rows read, bytes read, and why each
change worked.

#### Observability

Prometheus metrics from producer, resolve stage, engine, and reconciliation job.
Grafana dashboards committed as JSON. Alertmanager rules for the deterministic
conditions (lag, watermark stall, match rate outside band, restatement magnitude).
Alerts fire a webhook to the agent, which is the second-stage triage.

### 3.4 Decisions and their reasons

| Decision | Chosen | Why |
|---|---|---|
| Where device-graph resolution happens | Dedicated stage → third topic | Mirrors real systems; independently observable and testable; makes fan-out explicit |
| Attribution rule | Last-touch, assists recorded | Industry default; multi-touch becomes a query, not a re-run |
| Long window | Hot path (7d) + periodic reconciliation | 90d of processor state is infeasible at any real throughput |
| Write model | ReplacingMergeTree + scheduled rollup refresh | Simplest model that stays correct under replays and corrections |
| Restatements | `report_snapshots` with `reported_at` | Advertisers care; agent's late-arrival detector needs it |
| Co-viewing | Read-time multiplier | Keeps the join clean |
| Stream processor | Bytewax | Pure Python, real state/window primitives, fast iteration; Flink mapping in SCALING.md |

### 3.5 Out of scope for v1

Co-viewing inside the engine, Parquet/Iceberg landing, multi-touch attribution
models, schema evolution beyond v1. Listed in README "Next steps".

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
- **Co-viewing inflation**: co-view-adjusted reach beyond plausible bounds for a
  genre.
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
- **Agent accuracy**: diagnosable fault profiles (shared-IP spike, late burst,
  co-view multiplier bug, real performance lift), each scored on whether the
  agent's top hypothesis is correct, plus **two controls the agent must correctly
  leave alone** — a **no-fault baseline** and **duplicate_flood** (dedup absorbs it;
  ClickHouse carries no fingerprint, so the correct output is no-fault) — each run
  repeatedly, producing a *fault → top hypothesis → correct?* table with a
  **false-positive rate** measured on the controls. (Co-view inflation is
  diagnosable only once the adjusted co-view factor lands — Phase 10, §3.3
  read-time factor; raw per-genre reach does not discriminate it, see BACKLOG.)
- **The near-miss pair**: a genuine performance improvement vs. an inflated match
  rate from shared-IP false positives both raise reported ROAS but demand opposite
  responses. Showing the agent tell them apart on the evidence proves real
  reasoning, not pattern-matching on "ROAS went up."

## 5. How it maps to a data-platform posting

| Requirement | Where the project delivers it |
|---|---|
| Streaming at scale | Two-stream Redpanda ingestion, resolve stage, stateful Bytewax engine |
| Deep compute / lakehouse | Windowed stateful joins, reconciliation path; Iceberg landing as next step |
| OLAP reporting stack | ClickHouse: ReplacingMergeTree, async inserts, scheduled rollups, restatements |
| "Faster/cheaper query, and why" | Naive-vs-optimized benchmark with measured deltas and explanations |
| On-call / incident readiness | Prometheus, Grafana, Alertmanager rules, runbook-style SCALING.md |
| Data contracts | Pydantic-derived JSON Schemas enforced via schema registry at produce and consume |
| Ambiguous asks / judgment | Attribution-integrity agent with typed findings and escalation |
| Scale limits before outages | SCALING.md: what breaks at 50k and 500k msgs/sec |
| Responsible AI use | Read-only agent, DB-enforced, off the critical path, schema-constrained outputs |

## 6. Stack and repository

**Stack.** Python 3.12 (uv, ruff, pytest, pydantic). Redpanda (Kafka API, schema
registry). Bytewax. ClickHouse. Prometheus, Grafana, Alertmanager. Anthropic Python
SDK. Docker Compose. No JVM.

**Repository shape.**

```
producer/        generator, device graph, profiles/, schemas
resolve/         conversion → household resolution stage
streaming/       Bytewax attribution dataflow
reconcile/       periodic long-window matcher, rollup refresh, snapshots
clickhouse/      DDL, users, migrations
queries/         reporting SQL + benchmark harness
observability/   prometheus, grafana dashboards (JSON), alert rules
agent/           collectors, hypothesis catalog, probe registry, loop, eval
docs/            ARCHITECTURE.md  PHASES.md  SCALING.md  RESULTS.md
tests/           unit; tests/integration/ against compose
fixtures/        golden tiny-profile data and expected outputs
README.md        problem → architecture → results, reads like a design doc
CLAUDE.md        invariants, commands, conventions
```

## 7. Build order

See `PHASES.md`. Each phase stands on its own, so the project degrades gracefully:
even without the agent, a clean two-stream attribution spine with reconciliation,
benchmark, and ground-truth accuracy is a strong submission.

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
- **Bytewax's Kafka source follows forever; the Phase 3 engine is a batch
  drain.** `bytewax.connectors.kafka` is an unbounded source (it never signals
  end-of-input), so a dataflow built on it would not terminate on the finite
  seeded stream. The engine instead drains both topics to memory once
  (EOF-driven, the same idiom as the resolve stage) and feeds a bounded
  `TestingSource`, so `fold_final` flushes at end-of-input and the process
  exits. This also guarantees every candidate for a `conversion_id` is present
  when the reduction runs (DECISIONS Phase 3 (b)). Windowing (watermarks,
  allowed lateness, eviction) lands in Phase 5 **on the batch drain**; moving to
  continuous Kafka follow remains deferred (no phase owns it yet — the two
  resolve BACKLOG rows re-defer on exactly that trigger).
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

---

*The sentence to leave a reviewer with: this project builds the pipeline
attribution actually runs on, a windowed, late-tolerant, cross-device stream join
with a reconciliation path serving an OLAP reporting layer with restatements,
validates it against ground truth, and puts an AI agent where it earns its keep:
guarding the integrity of the numbers advertisers bet their budgets on.*
