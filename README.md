# CTV Attribution Pipeline

*A streaming attribution engine with an AI measurement-integrity agent — scored against ground truth.*

*This README is the living design doc: it describes `main` as of its last edit.
Dated facts live in `docs/RESULTS.md` and the per-phase history
(`docs/PHASES.md`, `DECISIONS.md`). Two state stores: the Iceberg lake under
`data/lake/<profile>/` is the record and outlives `make down`; ClickHouse is
derived from it — so "clean state" is `make down && make lake-reset`, two
commands, on purpose.*

The ad is seen on a TV; the conversion happens on a phone. This pipeline joins those
two separately-keyed event streams across a household/device graph, inside a time
window, tolerating duplicates and conversions that arrive days late, and serves the
four numbers an advertiser acts on (ROAS, CPA, CVR, site-visit rate) with
restatements. Because the producer emits ground-truth causal links, the attribution
is **scored against reality**, and an off-critical-path AI agent triages whether a
number that looks fine is actually wrong.

Two commands to a running demo:

```bash
make up
make seed PROFILE=tiny && make run
```

---

## The problem

Streaming TV (CTV) is increasingly sold as a **performance** channel — advertisers
want the precision they get from search and social, but on TV. The hard part, and
the most-cited pain point in the category, is **measurement and attribution**.

The core difficulty is a cross-device join at volume. The exposure (a TV impression)
and the conversion (a pixel fire on a laptop or phone) live in two high-volume
streams keyed by *different* things — the TV's household, the phone's device — and
bridging them means resolving the device to a household through a graph, then joining
inside an attribution window while handling duplicates and late arrivals.

Every attribution method has a blind spot. Click attribution barely registers a
channel that rarely produces clicks; view-through attribution treats "conversion
followed impression" as causation and over-credits about as often as it
under-credits. That ambiguity is where an automated
integrity check on the numbers is useful, and is where the agent fits.

## Scope and honesty boundary

This is a **simulation of the pipeline shape** attribution requires, not a
reproduction of any vendor's proprietary device graph or third-party integrations.
The engineering is real: two-stream ingestion, device-graph resolution, windowed
stateful joins, dedup, late-event handling, a reconciliation path, an OLAP serving
layer with restatements, and the operational tooling around all of it.

- **Scale posture.** It runs end to end on a laptop. It does **not** attempt a live
  500k/sec demo, which free infrastructure can't support. The scaling story is a
  written deliverable:
  [`docs/SCALING.md`](docs/SCALING.md) says where the design breaks at 50k/sec and
  500k/sec and exactly what changes at each tier.
- **Cost posture.** 16 GB laptop, Docker Compose. The agent evals cost under $10 in
  API tokens; every other command is free and offline-capable.
- **Determinism.** Same seed + profile → byte-identical topics and identical
  attribution output. The AI sits at the edge; the pipeline underneath it is
  deterministic (see [Determinism](#determinism-the-core-design-principle)).

---

## Architecture

```
PRODUCER (seeded)  ── device graph (compacted topic) ── truth links (side file, never read)
   │ exposures (key household_id)      │ conversions (key device_id)
   ▼                                   ▼
REDPANDA  exposures | conversions  + schema registry
                                       │
   exposures ──────────────────────────┤
                                       ▼
ATTRIBUTION ENGINE (deterministic batch attributor)
   resolve step in-process: device → household (IP fallback; shared IP = ambiguous)
   hot window (7d) · last-touch + assists · dedup (seen-set) · watermarks + allowed lateness
   ambiguous shared-IP → unattributed (ambiguous_ip) + candidate_households, never a hot guess
                                       ▼ land (append)
ICEBERG LAKE (system of record)  raw.exposures · raw.attributed_conversions
            day(event_time) × bucket(8, household_id) · append-only · argMax(processed_at) read
                                       ▼ Dagster load (touched days)
CLICKHOUSE (derived)  attributed_conversions (ReplacingMergeTree, key conversion_id, ver processed_at)
            exposures_landed · campaign_hourly (scheduled refresh) · report_snapshots
                                       ▲
RECONCILIATION JOB (periodic, reads the lake, no broker)  per day, current hot-unattributed rows:
                               state-miss → bucket-local join vs raw.exposures [day−90d, day];
                               ambiguous_ip → explode over candidate_households → bucket-local
                               join → reduce across buckets, most-recent exposure wins →
                               append to lake → reload → refresh → snapshot
                                       │
REPORTING  4 metrics · restatement view · naive-vs-optimized benchmark
──── off the critical path ──────────────────────────────────────────────
PROMETHEUS → GRAFANA · ALERTMANAGER ──webhook──► AGENT (SELECT-only user, probe registry,
                                                  typed AttributionFinding)
```

The full spec is [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); the build order is
[`docs/PHASES.md`](docs/PHASES.md). A walk through the stages, with the stream
concepts explained where they first appear:

**Producer (seeded).** Builds a device graph — households, each with a few devices
and one or more IPs — then emits two streams plus a hidden truth file. A configurable
fraction of IPs are *shared* across households (CGNAT, office, campus); shared IPs are
the **only** source of wrong-household matches, kept isolated so the fault is
diagnosable. Exposures are **keyed by** `household_id`, conversions by `device_id`.
A key decides which partition a message lands in — Redpanda (a Kafka-API log)
guarantees ordering only *within* a partition, so keying matchable events the same
way is what lets a later stage join them without a shuffle.

**Redpanda.** Two event topics plus `device_graph`, a **compacted topic** — one
where the log retains only the latest message per key, so it holds the *current*
graph as a replayable table rather than an ever-growing history. Each topic carries a
JSON Schema (generated from the pydantic models, never hand-edited) registered in the
schema registry; producer and every consumer validate against it.

**Resolve step (in-process).** For each conversion: look up the device in the
graph → one record for that household. Device unknown (guest, roommate, id churn) →
fall back to IP; if the IP maps to several households, **fan out** one candidate
record per household with an ambiguity flag. This is a function the engine calls
(`resolve/`, the Phase-2 signature), not a separate consumer and topic — that seam
returns only when the device graph is owned by another team or a vendor (Phase 16).

**Attribution engine.** A deterministic batch attributor in plain Python (no stream
framework — the Bytewax wrapper was removed in Phase 16 because it only regrouped
lists; the framework choice for continuous follow is a later decision). It drains
`exposures` and `conversions` start-to-end once, resolves in-process, joins on
`household_id`, and applies event-time windowing over the drain:
- **Hot window.** Exposures are held for 7 days of event-time. Window-state size is
  the central scaling constraint — you cannot hold 90 days of exposures in processor
  memory at real throughput, which is *why* there is a second reconciliation path.
- **Watermarks + allowed lateness.** A watermark is the pipeline's estimate of "we've
  now seen everything up to time T" (`max(event_time) − allowed_lateness`). It gates
  *when* a conversion is released for matching and *when* an exposure is evicted from
  the hot window. A conversion is a pure probe — never dropped by a lateness gate; it
  goes **unattributed** only when its matching exposure has already aged out of the
  window, and reconciliation retries it later.
- **Last-touch + assists.** Credit the most-recent in-window exposure; record the
  others as assists. Multi-touch then becomes a query, not a re-run.
- **Ambiguous deferral.** A shared-IP conversion (several candidate households) is
  never guessed hot: it is emitted **unattributed (ambiguous_ip)** as one placeholder
  row — still exactly one row per conversion — and reconciliation, which holds every
  exposure, credits the household with the most recent exposure. Hot-path
  wrong-household is 0 by construction; a late correct credit beats a fast wrong one.
- **Dedup.** A full seen-set on `conversion_id`/`exposure_id` (the batch already holds
  the whole topic; TTL'd eviction is the continuous-follow target — see SCALING.md).

**ClickHouse (serving layer).** `attributed_conversions` is a **ReplacingMergeTree**
keyed on `conversion_id` with `processed_at` as the version: when a reconciliation
correction or a replay writes the same key with a newer version, reads with `FINAL`
(or `argMax`) return only the latest — so replays and corrections are safe by
construction. `campaign_hourly` is a rollup **refreshed on a schedule**, never an
insert-triggered summing view (a correction would otherwise double-count).
`report_snapshots` stamps each refresh with `reported_at`, which is what makes
restatements ("ROAS for day D as reported on D+1 vs now") queryable.

**Reconciliation job (periodic, reads the lake).** Per event-time day, selects the
current hot-unattributed rows of `raw.attributed_conversions` still inside the
90-day long window — state-misses join bucket-locally against `raw.exposures` in
`[day − 90d, day]`; the deferred ambiguous_ip rows are exploded over their persisted
`candidate_households` BEFORE bucketing, joined bucket-locally, and reduced across
buckets — re-runs the *same* pure attribution leaf, applies the one
most-recent-exposure tiebreak, appends corrected rows (`path=reconciled`) to the
lake, reloads the touched days, refreshes the rollup, and writes a new
snapshot. This is the second attribution path — it is what makes a 90-day window
possible without 90 days of processor state.

**Observability.** Prometheus metrics per stage (`producer_`, `resolve_`, `engine_`,
`reconcile_`), a Grafana dashboard (JSON, committed), and Alertmanager rules for the
deterministic conditions (consumer lag, watermark stall, match-rate band,
restatement magnitude). Alerts fire a webhook to the agent (the live
scrape → Alertmanager → webhook push path is a documented cut — see
[Next steps](#next-steps--what-was-cut-and-why)).

## The agent — attribution-integrity guardian

Threshold alerts catch "lag is high." What they cannot catch is a
**plausible-but-wrong attribution number**: a ROAS that looks fine but is inflated by
a device-graph mismatch, a window-edge effect, or a late-arrival restatement. The
agent does the cross-signal reasoning a data engineer would do by hand, bounded to
make that safe:

- **Read-only, enforced at the database.** It runs as a SELECT-only ClickHouse user
  (`agent_ro`); an integration test proves it cannot INSERT/ALTER/DROP/CREATE.
- **No free-form SQL.** It calls a **probe registry** — named, parameterized queries
  exposed as tools with server-side-bound params.
- **Typed in, typed out.** Deterministic collectors build a pydantic
  `AttributionContext` (no LLM); the agent emits a pydantic `AttributionFinding`
  (`evidence_for` / `evidence_against` / `ruled_out` / recommended action / verdict
  `CONFIDENT` | `AMBIGUOUS_NEEDS_HUMAN`). Validation failure escalates to a human,
  never a silent retry.
- **Off the critical path.** Run the pipeline with the agent disabled and the
  attribution output is byte-identical.

The main test is the **near-miss pair**: a genuine performance lift and a shared-IP
false-positive inflation both raise reported ROAS but demand opposite responses. The
agent has to tell them apart on the evidence — the `ip_resolved_fraction`
discriminator, not "ROAS went up".

---

## Results

Full numbers and method in [`docs/RESULTS.md`](docs/RESULTS.md). Headlines:

**Attribution accuracy** (household grain, engine output vs the truth side file):

| profile | credited | truth | correct | precision | recall | what it shows |
|---|---|---|---|---|---|---|
| `tiny` (hot) | 47 | 35 | 32 | 0.681 | 0.914 | last-touch **organic over-credit**; 0 wrong-household; 3 caused shared-IP conversions deferred to reconciliation (post-reconcile 52/35/35) |
| `medium` (hot) | 129 | 92 | 91 | 0.705 | 0.989 | dedup + hour-late arrivals; 1 shared-IP deferral (post-reconcile 130/92/92) |
| `long_delay` (hot only) | 80 | 75 | 44 | — | 0.587 | days-late conversions miss the 7d hot window |
| `long_delay` (post-reconcile) | 112 | 75 | 73 | — | 0.973 | reconciliation recovers 29 caused misses + settles 3 shared-IP deferrals |

Precision below 1.0 on `tiny`/`medium` is **last-touch over-crediting organic
conversions** to a coincidentally-recent exposure — a model property, not a bug.
Hot recall below 1.0 on them is the Phase-16 deferral: a shared-IP conversion is
never credited hot; reconciliation credits it (same answer after reconciliation,
fewer moving parts — tiny/medium post-reconcile equal the pre-Phase-16 hot numbers).
Wrong-household (shared-IP) misattribution is exercised by the fault profiles, not the
clean ones, and can only occur on the reconciled path now. Household grain is deliberate: the engine is last-touch, so scoring exact
`exposure_id` would measure last-touch-vs-causal coincidence, not attribution quality.

**Benchmark** (naive full `FINAL` scan-and-join vs the `campaign_hourly` rollup, same
four-metric report, `long_delay`): rollup reads **2.5× fewer rows, 1.2× fewer bytes,
~1.8× faster** — measured with both sides at merged steady state — and the structural
point is that the naive scan grows with every event while the rollup read is bounded
by `(campaign, hour)` buckets.

**Agent eval** (every fault + a no-fault baseline, 5× each, 30 live invocations):
**30/30 correct, false-positive rate 0/10 = 0%.** The near-miss holds both ways —
`shared_ip_spike` → CONFIDENT `device_graph_mismatch` (at `ip_resolved_fraction`
0.420); `real_lift` → CONFIDENT `real_performance_change` (at 0.061), never
`device_graph_mismatch`. `co_view_bug` correctly **abstains** — a labeled capability
boundary (undiagnosable from serving data by design), kept out of the FP denominator.

## Run it

Requirements: Docker (16 GB), `uv`, `make`. First-time setup: `make setup` (uv sync +
pre-commit). Bring the stack up with health checks (not sleeps): `make up`.

Two canonical clean-state demos:

```bash
# Hot-path headline — fast, stable pins (a clean stack is a clean lake: the lake
# outlives `make down`, and run-hot loads the lake's current rows)
make down && make lake-reset CONFIRM=yes && make up && make seed PROFILE=tiny && make run-hot && make eval PROFILE=tiny && make report

# Reconciliation + restatement (recall 0.587 → 0.973, ROAS restated up)
make down && make lake-reset PROFILE=long_delay CONFIRM=yes && make up && make seed PROFILE=long_delay && make run PROFILE=long_delay && make eval PROFILE=long_delay && make report && make restate

# Replay the serving layer from the lake — no Kafka (after either demo)
make replay-serving PROFILE=<p> CONFIRM=yes && make eval PROFILE=<p>
```

`make run` is engine (resolve in-process) → Iceberg lake → Dagster load →
ClickHouse → reconciliation (lake → append → reload), a single pass (not a
daemon). Every row in ClickHouse arrived through the lake. `make run-hot` stops before reconciliation and backs the hot-path oracle
suites, where a reconciliation pass would over-credit the tiny/medium long-tail
organics and shift the pinned numbers. Eleven profiles live under `producer/profiles/`: `tiny`,
`medium`, `long_delay`, plus six fault/control profiles (`shared_ip_spike`,
`late_burst`, `co_view_bug`, `real_lift`, `duplicate_flood`, `no_fault_baseline`) and two
volume profiles (`bench_large`, `scale_curve`).

Other entry points — `make report` / `restate` / `bench` / `context` (metrics and the
LLM-free context object), `make eval` (accuracy vs truth), `make test` (offline,
no services), `make test-int` (against the running stack), `make test-alerts`
(promtool on the alert rules). The two LLM paths — `make agent-run` and
`make agent-eval` — cost API tokens; **ask before running** and export
`ANTHROPIC_API_KEY` from `.env`. Full command reference: [`CLAUDE.md`](CLAUDE.md) →
Commands. Service UIs once `make up` is healthy: ClickHouse `:8123`, Prometheus
`:9090`, Alertmanager `:9093`, Grafana `:3000`, Redpanda Kafka API `:19092`.

## Determinism: the core design principle

The AI sits at the edge; the pipeline is deterministic.

- Same `PRODUCER_SEED` + profile → byte-identical topics and identical attribution
  output. Breaking this is a bug.
- Anything computable is computed in Python/SQL, never asked of an LLM — match rates,
  deltas, IP-cluster stats, restatement magnitudes. The collectors build the context
  with zero LLM calls.
- The pipeline **never reads the truth links**. Accuracy is scored in the eval
  harness against the side file; truth never enters ClickHouse.
- Every write to `attributed_conversions` is idempotent (ReplacingMergeTree keyed
  `conversion_id`, version `processed_at`); replaying any topic from offset 0
  converges to the same ClickHouse state.

## Repo map

```
producer/        seeded generator, device graph, profiles/, pydantic schemas (source of truth)
resolve/         conversion → household resolution (in-process map step; device, IP fallback, fan-out)
streaming/       attribution engine: pure core + batch-drain driver (window, dedup, lateness, eviction)
reconcile/       periodic long-window matcher, rollup refresh, snapshots
clickhouse/      DDL, users (agent_ro is SELECT-only), migrations
queries/         reporting SQL, restatement view, naive-vs-optimized benchmark
observability/   prometheus.yml, alert rules, grafana dashboard (JSON)
agent/           collectors, hypothesis catalog, probe registry, loop, webhook, eval/
accuracy/        household-grain precision/recall vs the truth side file
tests/           pytest unit (no services); tests/integration/ against the stack
fixtures/tiny/   golden producer output + expected resolved/attributed rows (read-only)
docs/            ARCHITECTURE.md (spec) · PHASES.md · SCALING.md · RESULTS.md · RUNBOOK.md
DECISIONS.md     why-not-X log · BACKLOG.md deferred findings with revisit triggers
```

## Next steps — what was cut, and why

Everything here was a deliberate scope cut, not an oversight. Each is a written note,
not speculative code.

- **Continuous Kafka follow.** The engine is a bounded batch drain in plain Python
  (the Bytewax wrapper was removed in Phase 16 — it only regrouped lists). Windowing,
  watermarks, and eviction are all real, but on the drain. Moving to an unbounded
  continuous follow — and choosing the framework for it, Bytewax proper vs Flink — is
  a Phase-18+ decision, and it is the trigger for the next two items.
- **TTL'd dedup state.** Batch dedup is a full seen-set, correct because the whole
  stream is in memory. Under continuous follow an unbounded seen-set is a memory leak;
  it becomes TTL'd state keyed on `event_time + max_resend_delay`. The seeded
  duplicate is timestamp-identical to its original, so a real deployment (with
  genuinely-later re-send timestamps) is needed to exercise TTL sizing — see
  SCALING.md.
- **Live Alertmanager firing path.** The webhook endpoint and the alert rules both
  exist and are tested (promtool against real captured registries), but the batch
  stages exit before a scrape, so the live scrape → Alertmanager → webhook chain
  (and the Grafana panels that only populate under it) needs a push path
  (Pushgateway / textfile collector). The scheduled-sweep trigger `make agent-run`
  stands in for it today.
- **Co-view adjustment as a reporting factor** — a won't-do. The honest per-genre
  expected baseline does not exist in serving data, and sourcing it from the
  producer's multiplier would couple reporting to generation parameters. Co-viewing
  stays a producer-realism knob; the `co_view_bug` fault is scored as a labeled
  capability boundary the agent correctly abstains on (DECISIONS Phase 10).
- **Iceberg lake of record + orchestrated reconciliation** — originally cut,
  **added in Phase 12** as an optional dual-write and **made the system of record
  in Phase 17**: the engine lands `raw.exposures` + `raw.attributed_conversions`
  (`day × bucket(8, household_id)`), Dagster loads ClickHouse from the lake per
  touched day, reconciliation is a bucket-aligned DuckDB-over-Iceberg join proven
  byte-identical to the single in-memory pass, and `make replay-serving` rebuilds
  the serving tables from the lake with no Kafka (headless `make
  reconcile-dagster`; local `dagster dev` asset-graph viewer bound to one profile). Object store / REST catalog /
  Spark-Trino compute / a containerized Dagster webserver stay the SCALING port. See
  RESULTS.md and ARCHITECTURE §5.
- **Multi-touch models, schema evolution beyond v1, co-view inside the engine** — out
  of scope for v1 (ARCHITECTURE §3.5). Multi-touch is intentionally a *query* over the
  recorded assists, not a re-run.
