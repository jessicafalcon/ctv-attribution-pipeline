# DECISIONS.md — why-not-X log

One entry per non-obvious choice. Newest last.

## Phase 0

- **Redpanda's built-in schema registry, not a separate container.** Redpanda
  ships the registry in the same binary on port 8081. A separate
  Confluent-style registry container would add a JVM (banned) and a second
  thing to health-check for zero benefit at this scale.
- **`docker compose up -d --wait` for `make up`.** `--wait` blocks on every
  service's healthcheck, which satisfies "health checks, not sleeps" with no
  wrapper script.
- **All image tags pinned.** `latest` would make `make up` behave differently
  across machines/dates — a determinism-policy violation at the infra layer.
- **No runtime Python dependencies in Phase 0.** The allowlist packages
  (bytewax, clickhouse-connect, …) are added in the phase that first imports
  them, so each PR's dependency delta is reviewable.
- **ClickHouse native port (9000) not published to the host.** Everything we
  run from the host uses clickhouse-connect over HTTP (8123); in-network
  services can still use 9000. Avoids a common host-port collision.
- **`run-tests` hook wiring is local-only** (gitignored
  `.claude/settings.local.json`), inherited ruling from trial-signal-assistant:
  a committed settings.json would auto-execute an inbound PR branch's hook +
  pytest + conftest.py for anyone opening it in Claude Code.
- **Redpanda healthcheck uses `rpk cluster health -w -e`, not output grep.**
  The flags make health an exit code (0 when healthy; blocks past the
  timeout when not), so an rpk text-format change can't silently break
  `make up`.
- **Compose ports bind 127.0.0.1** (security-review finding): the stack runs
  passwordless local-dev services (ClickHouse default user, Grafana
  admin/admin, Redpanda admin API); binding to localhost keeps them off the
  LAN. In-network services are unaffected (compose DNS).
- **GitHub Actions pinned by tag, not SHA** — accepted for now: this CI
  holds zero secrets. Revisit (SHA-pin) before the Phase 3 integration job.
- **`tests/test_smoke.py` instead of a truly empty suite.** Bare `pytest`
  exits 5 on "no tests collected", which would make `make test` red; one
  layout assertion keeps the suite meaningfully green.

## Phase 1

- **Schema registration via stdlib urllib, not confluent-kafka's
  schemaregistry extra.** Registration is one POST per subject; the extra
  pulls httpx/authlib/etc. (not on the allowlist) to do the same thing.
  Validation on produce is done by the pydantic models the schemas were
  generated from, so the contract enforced is identical.
- **Profiles are JSON, not YAML.** YAML would add a dependency; JSON is
  stdlib and profiles are small flat config.
- **Counter IDs (`e-000042`) and a fixed profile `sim_start`, no UUIDs or
  wall clock.** Byte-identical output per seed is the phase's core
  guarantee; any wall-clock or entropy source breaks it. Readable IDs also
  make fixture diffs debuggable.
- **Emit order = arrival order (sorted by `ingest_time`), duplicates
  re-sent later with identical bytes.** Mirrors how a real stream hits the
  broker: late events appear late in the stream, duplicates are true
  re-sends. Gives the engine (Phase 5) realistic input for lateness and
  dedup without extra machinery.
- **Single-partition topics at tiny scale.** Keyed partitioning is already
  in place (household_id / device_id); partition count is the documented
  scaling lever (ARCHITECTURE.md), and one partition keeps cross-partition
  ordering out of fixture comparisons.
- **Co-view multiplier = per-genre factor on the caused-conversion rate.**
  Simplest producer-side meaning that creates genre-skewed conversion
  volume for the read-time co-view factor and Phase 8's multiplier-bug
  fault to work against.
- **`data/out/<profile>/` mirror of every produced payload.** Byte-identity
  across runs and against `fixtures/tiny/` becomes a `diff -r`, with no
  topic-consuming harness needed in Phase 1. Truth links are NOT in the
  mirror (they are never produced to a topic); they live only under
  `data/truth/`, with a committed golden copy in `fixtures/tiny/` for eval.
- **Duplicate re-sends carry the original `ingest_time`.** A duplicate is
  the same bytes sent again, so its payload cannot record its later
  arrival. Consequence for later phases: `ingest_time − event_time`
  understates a duplicate's true arrival lateness. Lateness metrics
  (Phase 5+) must be computed on first-seen events, after dedup.
- **`tiny` stays inside the 7-day hot window (max lateness 3h).** Phases
  2–4 prove hot-path correctness on it, so every event must be attributable
  without reconciliation. Days-late arrivals (ARCHITECTURE: "minutes to
  days") are exercised by the `medium`/fault profiles from Phase 5 on —
  the late-injector knob already supports it; `tiny` deliberately doesn't
  use it.
- **Unknown-device conversions (`unknown_device_fraction`).** Without them
  every conversion's device is in the graph, device resolution is always
  correct, and Phase 2's IP-fallback and ambiguous-fan-out branches — plus
  the pipeline's headline failure mode, the shared-IP wrong-household
  match — are unreachable by construction. The unknown device draws its IP
  from the true household (same home network, unrecognized guest/roommate
  device): realistic, and it keeps shared IPs the sole source of
  wrong-household ambiguity. The truth link still records the true causing
  exposure, so a fan-out to the wrong household is measurable in Phase 4.
  [Refined by DECISIONS Phase 4: the eval DOES measure wrong-household
  misattribution via `caused_wrong_household`, but the tiny fixture's
  caused-ambiguous conversions all resolve to their truth household
  (`caused_wrong_household == 0`), so tiny's Phase-4 precision reflects organic
  over-credit; the shared-IP wrong-household OUTCOME is exercised by the Phase-8
  fault profile, not tiny.]
  `u-` ids are a namespace disjoint from graph `d-` ids. tiny is curated
  (fraction 0.3, seed 42) so the frozen fixture reaches all three resolve
  cases; a structural test pins the exact counts. 0.3 is curation-high, far
  above a plausible guest-device rate — `medium` and fault profiles should
  use a realistic ~0.05–0.1 so the headline match rate in RESULTS.md isn't
  skewed. Fixture case counts are stated per distinct `conversion_id`
  (duplicates collapsed); Phase 2 tests counting raw rows will see more.
- **Phase 3 forward-note: assert against ReplacingMergeTree FINAL, not the
  raw stream.** tiny carries duplicates and hour-late arrivals, but
  Phase 3's engine is spec'd in-order/no-dedup (dedup lands in Phase 5). A
  duplicate conversion is processed twice on the hot path; both writes
  share `conversion_id`, so ReplacingMergeTree collapses them. The Phase 3
  integration test must compare FINAL (or argMax) state to truth, else it
  counts conversion-processings instead of conversions.
- **Review-driven hardening (Phase 1 review gate).** Delivery callbacks +
  checked flush (a partially delivered seed now fails instead of exiting
  0), `enable.idempotence` on the producer, request timeout + scheme check
  on schema registration, profile-name allowlisting in `load_profile`,
  min≤max validators and timezone-aware `sim_start` in the profile schema,
  `uv sync --locked` in CI, and a structural test that pipeline-stage code
  never mentions truth links.

## Phase 2

- **`ResolvedConversion` subclasses `Conversion`.** It carries every conversion
  field plus `household_id`/`resolution`/`ambiguous`/`candidate_count`, so the
  engine (Phase 3) attributes from one `conversions_resolved` record without
  re-reading `conversions`. Kept in `producer/models.py` so all topic schemas
  have a single source of truth (schema contract).
- **Resolve is a stateless map — duplicates in, duplicates out.** No dedup
  here; dedup keys on `conversion_id`/`exposure_id` and lives in the engine
  (Phase 5, ARCHITECTURE). A duplicate conversion resolves to identical
  bytes, so it collapses later under ReplacingMergeTree / TTL dedup. Keeps the
  stage a pure function of (conversion, graph).
- **Ambiguous fan-out ordered by `sorted(household_id)`.** The only
  nondeterministic choice in the stage was candidate order; sorting makes the
  emitted record sequence byte-identical across runs.
- **Offline replay is the DONE-command path; the live stage is a batch pass.**
  `resolve/replay.py` resolves the frozen fixture with no broker, so the golden
  diff has zero stream-ordering nondeterminism. `resolve/stage.py` drains
  `conversions` low→high watermark once and exits (not follow-forever): it
  processes the finite seeded stream end-to-end, which is what the integration
  test and a tiny/medium run need. Continuous follow lands when `make run`
  wires the full pipeline (Phase 3+). The two share the one pure resolver, so
  they cannot diverge.
- **Per-subject schema compatibility `NONE` in dev (BACKLOG Phase-2 item).**
  The registry's global default is BACKWARD; under it, re-registering a
  *changed* model (as `ResolvedConversion` will evolve) 409s and fails the
  stage/seed. This is single-writer dev infra with no schema-evolution story
  yet, so every subject is set to `NONE` before its version is posted.
  Verified against Redpanda: `PUT /config/<subject>` works before the
  subject's first version exists, and an incompatible re-register then returns
  a new id instead of 409. Global stays BACKWARD.
- **Integration test compares the DISTINCT resolved payload set.** Seeding is
  deterministic (idempotent bytes) but topics accumulate across re-seeds
  without `make down`; the stage drains from offset 0 by manual assignment
  (ignoring group offsets), so residual/duplicate rows collapse to the same
  bytes and the distinct set equals the golden fixture regardless of history.
- **Ambiguous shared-IP reconciliation (engine, conversion_id-keyed
  reduction).** An ambiguous shared-IP conversion fans out in the resolve
  stage to one `ResolvedConversion` per candidate household, each keyed by
  `household_id` (Phase 2). The engine's join is also keyed by `household_id`,
  so it CANNOT compare exposure recency across candidates partition-locally.
  Decision: after the household-keyed join the engine adds a
  `conversion_id`-keyed **reduction** — for a `conversion_id` with N candidate
  rows, keep the candidate whose last-touch exposure is most recent, ties
  broken deterministically by `exposure_id` then `household_id`. This preserves
  ARCHITECTURE's "most recent exposure inside the window" rule, is
  deterministic, and keeps wrong-household picks as the measured shared-IP
  fault (scored as Phase 4 precision).
  [Refined by DECISIONS Phase 4: the eval DOES measure wrong-household
  misattribution via `caused_wrong_household`, but the tiny fixture's
  caused-ambiguous conversions all resolve to their truth household
  (`caused_wrong_household == 0`), so tiny's Phase-4 precision reflects organic
  over-credit; the shared-IP wrong-household OUTCOME is exercised by the Phase-8
  fault profile, not tiny.]
  `processed_at` must NOT discriminate
  among candidate households — it is the ReplacingMergeTree version for
  idempotent replacement of the *same logical row* (replay/reconciliation), not
  a tiebreaker; the reduction runs BEFORE the ReplacingMergeTree so exactly one
  row per `conversion_id` per processing enters the table. Rejected: (a) naive
  independent per-household attribution of each candidate row — the
  ReplacingMergeTree survivor would then depend on write/version ordering
  (nondeterministic) and the semantics would be wrong (N credited rows for one
  conversion); (b) a simpler deterministic pick that ignores recency (e.g.
  lowest `household_id`) — deterministic but less realistic, and it would blunt
  the shared-IP fault the project exists to measure. Implemented in Phase 3;
  recorded now because Phase 2's fan-out shape surfaced the constraint.

## Phase 3

- **`processed_at` is event-derived (the RMT version).** The
  `attributed_conversions` ReplacingMergeTree version is `processed_at =
  conversion.ingest_time` — deterministic, already in the data, and preserved
  byte-identically on a resend (a duplicate is the same bytes with the original
  `ingest_time`, Phase 1), so duplicates collapse to one row. Wall-clock
  `now()` would violate the determinism policy ("could this step give a
  different answer on a re-run?") and break both the golden `attributed.jsonl`
  fixture and the replay-from-offset-0 convergence the idempotency contract
  requires. Rejected wall-clock. Two invariants this creates:
  - **(a) Phase-6 reconciled-version rule (concrete).** A reconciled row for a
    `conversion_id` must carry a `processed_at` that is *deterministically*
    strictly greater than the hot row's `ingest_time` for that `conversion_id`
    — never equal. Phase 6 stamps reconciled rows with a deterministic
    reconciliation-pass timestamp derived from data (strictly > the
    conversion's `ingest_time`), so the correction always supersedes the hot
    row under RMT. Recorded now so Phase 6 does not reopen it. `processed_at` is
    `DateTime64(3)` (millisecond), so that "strictly greater" rule has
    millisecond resolution as its headroom — the reconciliation-pass timestamp
    must differ from the hot `ingest_time` by at least 1 ms.
  - **(b) `conversion_id` is a safe RMT sort key** only because the
    `conversion_id`-keyed reduction emits exactly one winner per
    `conversion_id`, so RMT sees one hot row per key (plus byte-identical
    resend duplicates). This holds because the batch drain sees all candidate
    rows for a `conversion_id` together; continuous mode does not, without
    windowing (Phase 5). Pairs with the existing continuous-mode BACKLOG rows.

- **`exposures_landed` is ReplacingMergeTree, not plain MergeTree
  (replay-idempotency).** A plain MergeTree never dedups, so re-running the
  engine or replaying `exposures` from offset 0 appends duplicate exposure rows
  — violating the idempotency contract and inflating Phase-6 reconciliation
  match/assist counts, since reconciliation reads this table. Decision:
  ReplacingMergeTree `ORDER BY (campaign_id, event_time, exposure_id)`. RMT
  dedups on the sort key, so leading with `(campaign_id, event_time)` still
  serves the Phase-7 benchmark query pattern while `exposure_id` in the key
  collapses re-landings (a re-landed exposure shares its `exposure_id`).
  Rejected plain MergeTree. Guarded by a double-run integration assertion:
  `exposures_landed` FINAL count == distinct exposure count after two runs.

- **Bytewax owns plumbing, the pure core owns decisions.** `streaming/
  attribute.py` exposes the leaf decision logic as pure functions —
  `attribute_household(exposures_in_household, resolved_in_household, window)` →
  per-candidate attributed rows, and `reduce_conversion(candidate_rows)` → one
  `AttributedConversion`. The offline replay's `attribute(...)` orchestrates
  them over in-memory groups; `streaming/dataflow.py` lets Bytewax do the keyed
  grouping (`key_by` household_id → `attribute_household`; re-key by
  `conversion_id` → `reduce_conversion`) and calls the SAME leaf functions. One
  implementation of every decision → live and replay cannot diverge, and
  Phase 3 already exercises real Bytewax keyed operators, de-risking the
  Phase-5 migration to stateful/windowed versions. Async inserts are deferred
  to Phase 7 (the benchmark phase); Phase 3 inserts synchronously so the
  integration FINAL-comparison is deterministic (async insert buffering makes
  read-after-write flaky for no benefit at tiny scale).

- **ClickHouse `default` user re-enabled for network access (local-dev).**
  ClickHouse 24.8 images, when `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` are
  unset, generate a `users.d/default-user.xml` that restricts the `default`
  user to loopback (`::1`, `127.0.0.1`) — so host clients reaching the HTTP
  interface through the docker gateway are rejected (`AUTHENTICATION_FAILED`),
  while the in-container healthcheck still passes. We mount
  `clickhouse/users.d/allow-network.xml` (networks `::/0`, empty password) to
  restore access. Posture is unchanged from Phase 0: the host publishes 8123 on
  `127.0.0.1` only (compose ports), so the service stays off the LAN even though
  ClickHouse accepts connections from within the docker network — same
  passwordless local-dev stance as Grafana admin/admin and the Redpanda admin
  API. The SELECT-only agent user (Phase 9) is a separate, later concern.
  Rejected setting `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` env: it would put a
  credential in compose/CI for zero security gain over the loopback-bound port,
  against the passwordless-dev posture. Covered by the existing BACKLOG
  "127.0.0.1 binding still admits any local process" row (shared-host caveat).

- **All wire + row schemas co-locate in `producer/models.py` (single source of
  truth).** The file holds the Kafka topic models (`Exposure`, `Conversion`,
  `ResolvedConversion`, graph/truth records) and, from Phase 3, the ClickHouse
  serving-table schema `AttributedConversion` — which is a *table* schema, not a
  registered subject (its columns live in `clickhouse/ddl.sql`, insert order in
  `streaming/sink.py`). Co-located so the output models can subclass
  `Conversion` without a cross-package import cycle (the dependency direction is
  already resolve/ and streaming/ → producer/). Two output models is not a junk
  drawer, but the drift is real (the module is now the whole-pipeline schema
  module, not "the producer's"), so the **split trigger** is written down: move
  the engine models to a shared `schemas` package on the 4th output model, or
  the first model producer/ has no reason to import. Recorded at the coherence
  auditor's Phase-3 flag so the trigger isn't rediscovered later.

- **Kafka batch-drain promoted to `common/kafka.py` (shared, public).** Both the
  resolve stage and the engine drain a topic start→end once (EOF-driven). The
  drain first lived as `resolve.stage._drain`/`_drain_messages`; when the engine
  needed it (Phase 3) it imported the *private* function cross-stage — a hidden
  load-bearing coupling (coherence audit D3). Moved verbatim to public
  `common.kafka.drain` / `drain_messages` (with `_EMPTY_POLL_LIMIT`), imported
  by both stages; behavior identical, covered by the relocated offline test
  `tests/test_kafka.py`.

## Phase 4

- **Eval accuracy is scored at household grain.** ARCHITECTURE §4.3 read as
  exact `exposure_id` equality; PHASES Phase 3 reads at household. The two
  disagreed. Resolved to **household grain**: exact-id scores a last-touch
  engine at ~6% (3 of 52 credited rows match the causal exposure) by measuring
  coincidence between the most-recent and the causal exposure — a model
  property, not attribution quality — which contradicts the last-touch design.
  Definitions: **precision** = caused conversions attributed to the correct
  household / all credited (`attributed=true`) conversions; **recall** = caused
  conversions attributed to the correct household / all truth links. Pinned
  expected tiny output: precision **0.673** (35/52), recall **1.000** (35/35).
  Exact-`exposure_id` match rate is reported only as a **labeled diagnostic**
  ("last-touch → causal-exposure coincidence; expected low, not an accuracy
  measure"), never the headline. ARCHITECTURE §4.3 amended to match; rejected
  exact-id as the headline.

- **Eval joins the truth side file in the harness; truth never enters the DB
  (N1).** PHASES Phase 4's Done-when said accuracy is computed "against truth
  links from ClickHouse," but truth links are a forbidden side file the pipeline
  never reads (determinism policy, truth-isolation guard
  `tests/test_truth_isolation.py`). Resolved: `make eval` reads
  `attributed_conversions` FINAL from ClickHouse and joins it against the
  `data/truth/<profile>/` side file **in the eval harness** — which lives
  outside the pipeline dirs the isolation test guards — never loading truth into
  ClickHouse. Rejected landing truth in a ClickHouse table (would breach
  truth-isolation for a SQL join's convenience). PHASES Phase 4 wording
  corrected from "from ClickHouse" to "side file."

- **tiny demonstrates last-touch organic over-credit, not the shared-IP fault.**
  On the frozen tiny fixture the ambiguous-reduction mechanism is exercised (5
  fan-outs collapse to one deterministic winner each) but no caused conversion
  is misattributed to a wrong household — all 3 caused ambiguous conversions
  (c-000014/16/25) resolve to their truth household; the other 2 (c-000041/42)
  are organic. tiny's precision (0.673) is driven entirely by 17 organic
  conversions last-touch credits to a coincidentally-recent in-window exposure.
  The shared-IP wrong-household fault is a fault-profile story (Phase 8), not a
  tiny story — PHASES Phase 3 corrected at source, and a BACKLOG row pins the
  Phase 8 requirement to engineer and *observe* a caused misattribution
  (recall(household) < 1.0). Fixtures are frozen read-only (Phase 1), so this is
  recorded, not repaired in tiny.

- **Report v1 metric definitions.** ARCHITECTURE §3.3 names the four advertiser
  metrics but does not define them; recorded here (the spec and `queries/`
  already cite this):
  - **ROAS** = attributed revenue / spend.
  - **CPA** = spend / attributed **purchases** (acquisition = purchase). Chosen
    over spend / all-attributed-conversions ("option 1"): the all-conversions
    denominator equals CVR's numerator, so CPA would just restate CVR; keeping
    the denominator to purchases makes CPA an independent money-action signal
    that pairs with ROAS, while CVR and site-visit rate describe funnel activity
    — four metrics, four distinct signals.
  - **CVR** = attributed conversions / exposures.
  - **site-visit rate** = attributed `site_visit` conversions / exposures.
  Three load-bearing rules:
  - **Read FINAL on both RMT tables** (`exposures_landed`,
    `attributed_conversions`). A plain `sum`/`count` over unmerged parts counts
    duplicate exposure landings and pre-reduction rows, silently inflating the
    spend and exposure denominators (this is the payoff of the Phase-3 RMT
    choice for `exposures_landed`).
  - **NULL on zero denominators** via `nullIf(denominator, 0)`, uniformly — a
    campaign with no purchases / spend / exposures yields NULL, never a
    divide-by-zero or a crash. tiny does not exercise this path (every campaign
    has purchases); guarded synthetically in `tests/test_report.py`.
  - **Do NOT filter wrong-household attributions.** An ambiguous shared-IP
    conversion credited to a campaign counts toward its metrics even when truth
    disagrees — it is the advertiser's reported number, and the divergence is
    measured separately by `make eval`. A subtly-inflated ROAS is exactly the
    "plausible-but-wrong" number the Phase-9 agent will diagnose; a feature to
    preserve, not a filter to add.

## Phase 5

- **Batch dedup is a full seen-set, not TTL'd state (why-not-TTL).**
  ARCHITECTURE §3.3 names dedup "TTL'd state sized to the max plausible duplicate
  delay." That sizing cannot work in the Phase-5 batch drain, and the reason is
  structural, not incidental: the duplicate injector re-appends the *identical
  payload* (`producer/generate.py` `_with_duplicates`, lines 56-61 — the returned
  list holds the same object twice; the `+uniform(10,300)` arrival slot is a sort
  key for emit order, then discarded, never a field). So a re-send carries the
  same `event_time` AND the same `ingest_time` as its original — the pair is
  field-indistinguishable in time, and an event-time TTL has nothing to measure
  against. A TTL sized to the 300s re-send delay would also sit on a
  seed-dependent knife-edge: on a denser stream the watermark (max event_time)
  advances ~300s of event-time between a re-send pair, evicting the id from the
  seen-set before its re-send arrives, so `engine_dedup_suppressed_total` silently
  undercounts and is brittle across seeds — while ReplacingMergeTree read-time
  collapse still makes clause-1 parity pass and *hides* the bug. And "TTL
  boundary" cannot be a producer-knob test (CLAUDE.md's knob-driven rule), because
  no knob can push a timestamp-identical duplicate past an event-time TTL.
  Decision: the engine is a bounded batch drain that already holds the whole topic
  in memory, so it keeps a **full `conversion_id`/`exposure_id` seen-set, no
  in-batch TTL** — O(n), same order as the existing grouping, deterministic on the
  single partition. Dedup stays *semantically transparent* (RMT collapses re-sends
  regardless), so its Phase-5 test is a counter (`> 0`, the mechanism fired) plus
  a "FINAL row count == dedup-off run" invariance (transparency), never precision/
  recall. This is the "simplest standard solution now; scaling path is a note, not
  code" contract: TTL'd eviction is real only once the engine follows continuously
  (out of scope this phase) → recorded in SCALING.md, not built now. ARCHITECTURE
  §3.3 and §8 updated to track this (Option A, per the batch-vs-continuous
  convention §8 already uses; not a DECISIONS-only footnote).

- **Windowing lands on the batch drain; continuous follow stays deferred.**
  ARCHITECTURE §8 previously read "Continuous follow with windowing lands in Phase
  5." Phase 5 adds watermarks + allowed lateness + eviction to the *batch drain*
  (deterministic, event-time-driven, no wall clock — determinism policy) and does
  NOT move to continuous Kafka follow. No phase currently owns continuous follow;
  the two resolve BACKLOG rows (graph refresh, conversions-offset reprocessing)
  re-defer on exactly that trigger. §8 corrected in the same pass.
