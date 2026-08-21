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
  bytes, so it collapses later under ReplacingMergeTree / the engine dedup.
  Keeps the stage a pure function of (conversion, graph).
  (Superseded detail: "TTL dedup" here was the pre-Phase-5 plan; the Phase-5
  engine dedup is a full seen-set, not TTL'd — see Phase 5 below.)
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

- **Medium live proof runs on its own clean stack, not the shared `make
  test-int` (profile collision the spec didn't foresee).** The Phase-5 spec first
  listed `tests/integration/test_engine_hardening.py` under `make test-int`, but
  that bundle runs all integration tests against one shared compose stack, and
  the existing tiny tests seed the `tiny` profile into the same Kafka topics and
  the same `attributed_conversions` / `exposures_landed` ReplacingMergeTree
  tables. tiny and medium **share conversion_id space** (both start at
  `c-000000`; tiny's 55 ids ⊂ medium's range), so a shared stack would interleave
  their RMT rows keyed by `conversion_id` and pollute FINAL — whichever test runs
  after a different-profile test reads mixed state. Fixing that inside
  `make test-int` would mean a per-test TRUNCATE (a destructive pattern CLAUDE.md
  restricts to `make down`) plus careful ordering, retrofitted onto passing
  tests. Instead: `make test-int-medium` isolates via the **sanctioned `make
  down`** (fresh medium-only stack), the shared `make test-int` stays tiny-only
  and untouched, and CI (tiny profile) is unaffected. The live proof stays a real
  assertion of Done-when clauses 1/2/3 against the pinned oracle baseline (live
  FINAL P/R == oracle, gauge rose/fell, dedup counter + FINAL-row-count
  invariance under `ENGINE_DEDUP=off`), not an eyeballed `make eval` number.
  Offline `tests/test_medium_parity.py` proves the same three clauses
  deterministically and is the CI-gating coverage; live tiny already exercises
  the Kafka→ClickHouse path. Spec's test section updated to match.

- **Per-household arrival-order re-sort by `(ingest_time, kind, id)`, not a
  reliance on Bytewax delivery order.** `streaming/attribute.py`
  `attribute_household_streaming` sorts each household's interleaved events by
  `_arrival_key = (ingest_time, kind, id)` before the watermark-gated pass.
  Necessary, not a spec violation: Bytewax's `op.merge` of the two `TestingSource`
  inputs does NOT preserve global topic-offset order, so relying on delivered
  order would be nondeterministic. An earlier phase-5 spec draft said "do NOT
  re-sort" — that was too absolute; it feared a re-sort collapsing a duplicate
  adjacent to its original, a hazard the upstream seen-set dedup (feature 1)
  already removes. The tiebreak `(kind, id)` differs from the producer's emit
  tiebreak `(event_id, dup_slot)` (`generate.py:60`), but the output is invariant
  to it: release-gating + EOF flush attribute every conversion against the
  complete eligible set, so ordering within an equal `ingest_time` cannot change
  the result. Gate-0 (tiny byte-identical) and medium parity prove byte-safety.

- **`medium` uses `unknown_device_fraction = 0.1`, per DECISIONS Phase 1.** Phase
  1 set curation-high `0.3` on tiny and explicitly noted `medium`/fault profiles
  should use a realistic `~0.05–0.1` so the RESULTS match rate isn't skewed. The
  first medium.json copied tiny's `0.3`; a phase-exit coherence finding caught the
  conflict. Fixed (not the guidance revised): engine hardening is indifferent to
  the unknown-device rate, so `0.3` bought the Phase-5 proof nothing while skewing
  exactly what Phase 1 protected. Re-pinned the medium baseline to the `0.1`
  numbers (precision 92/130, recall 1.0, 132 rows, dedup suppressed 70) in
  `tests/test_medium_parity.py`, the integration test, and the spec — the numbers
  live only there (no RESULTS.md consumer yet).

## Phase 6

- **Reconciliation reuses the hot last-touch leaf at a 90d window; no second
  matcher.** `reconcile.reconcile` reconstructs `ResolvedConversion` / `Exposure`
  models from ClickHouse FINAL and calls the SAME `streaming.attribute.
  attribute_household` the hot engine calls, with `window = LONG_WINDOW (90d)`, so
  hot and reconciled decisions cannot diverge (the DECISIONS Phase 3 "pure core
  owns decisions" invariant, extended). Reconstruction is lossless: `event_time`/
  `ingest_time` are `DateTime64(3)` and the producer rounds every timestamp to
  `round(..., 3)` (ms), so the round-trip gives the leaf byte-identical inputs.
  Matching happens in the household the hot reduction already settled on — the
  shared-IP fan-out collapsed on the hot path (one unattributed row per
  conversion_id), so reconciliation does NOT re-fan-out (ARCHITECTURE §3.3 reads
  "by household"). Rejected a standalone reconcile matcher (a second implementation
  of last-touch that could drift from the leaf's tie-breaks).

- **Candidates are hot-*unattributed* rows only: `attributed = 0 AND path =
  'hot'`.** Reconciliation must never re-open a hot-*attributed* row — over a 90d
  window it would pick the same last-touch exposure (the 90d winner equals the 7d
  winner whenever an in-7d exposure exists) but rewrite it with a higher
  `processed_at`, flipping `path` hot→reconciled for no change in attribution.
  Scoping to `path = 'hot'` keeps the job "recovery only" and makes the
  second-pass no-op fall out (still-unmatched candidates emit nothing, so a re-run
  re-selects and re-fails, changing nothing).

- **`reconciled_at = max(ingest_time over the fixed serving state) + 1s`.** The
  version stamped on reconciled rows must be deterministically strictly greater
  than the hot row's `processed_at` (= a conversion's `ingest_time`) so RMT keeps
  the correction (DECISIONS Phase 3 (a)). `base` is `max(ingest_time)` over the
  UNION of `exposures_landed` FINAL and `attributed_conversions` FINAL — a **fixed
  input set** independent of which rows get recovered, so a re-run computes the
  identical `reconciled_at` and the job converges (idempotency). `RECONCILE_DELTA
  = 1s` is a documented constant, comfortably above the ms `DateTime64(3)`
  resolution. Rejected `now()` (would break replay convergence and snapshot
  determinism) and a max over only the candidate rows (that set shrinks as rows
  recover → non-stable base).

- **Timestamps never round-trip through Python for storage — clickhouse-connect
  applies the client's local timezone.** A DateTime read into Python and
  re-inserted lands at a different wall-clock across processes (ARCHITECTURE §8),
  which stamped `report_snapshots.reported_at` 6h apart between the `make run`
  subprocess and an in-process caller — four snapshots instead of two,
  restatement delta collapsed. So `reported_at` is computed **server-side** in the
  rollup/snapshot INSERT (`max(ingest_time) + toIntervalMillisecond(offset_ms)`;
  offset 0 for the pre/hot pass, `RECONCILE_DELTA_MS` for the post pass), and
  `_max_ingest` reads a timezone-free **epoch-millis integer**
  (`toUnixTimestamp64Milli`) rebuilt as UTC for the `reconciled_at` version.
  `processed_at` supersession is correct on any machine because BOTH reconciled and
  hot rows are timezone-aware UTC written via `client.insert`, which stores the
  true UTC instant with **no shift** (verified — the engine's `event_time` lands
  exactly on `sim_start`); `reconciled_at = base + Δ > every hot ingest_time ≤ base`
  regardless of client timezone. (Rejected the earlier "same insert path, shift
  cancels" framing — there is no write-side shift to cancel; only reading a stored
  DateTime back into Python localizes it, which is why the read path uses epoch/
  server-side. The write path is tz-safe.)

- **`campaign_hourly` is a versioned-replace ReplacingMergeTree, not a TRUNCATE
  or a summing MV.** Each refresh recomputes ALL `(campaign_id, hour)` keys from
  FINAL and inserts them with a higher `reported_at` version; FINAL keeps the
  latest per key. This is the CLAUDE.md determinism rule (insert-triggered summing
  MVs double-count under corrections) plus the destructive-command rule (no
  TRUNCATE/DROP outside `make down`). A disappeared key would linger at its last
  version (RMT has no tombstone here) — a non-issue at this grain (spend/exposures
  are append-only), noted as SCALING. A background refreshable MV (wall-clock
  refresh, flaky under tests) is the SCALING alternative, not built.

- **`report_snapshots` is per-campaign, serving-derived; the PRE snapshot filters
  `path = 'hot'` so the restatement is order-independent and re-run-safe.** Two
  snapshots per run: PRE at `base` over `attributed = 1 AND path = 'hot'` (the
  hot-pass credited set, invariant under reconciliation — it only rewrites
  hot-UNattributed rows), POST at `reconciled_at` over all attributed rows.
  Filtering PRE by `path = 'hot'` (rather than snapshotting before recovery runs)
  makes it recomputable identically on any later pass, so re-running the whole job
  converges and the restatement delta never collapses. Every column is
  serving-derived — no recall/precision in the DB (N1 isolation; the truth-
  isolation guard structurally forbids the word in `reconcile/`, `clickhouse/`,
  `queries/`). `period` is a fixed `'all'` sentinel (campaign-total grain);
  day-grain slots in later without a schema change. A run-level match-rate was
  dropped from storage (would mix grains); the per-campaign credited-conversions
  delta already shows the recovery magnitude.

- **`make run` reconciles; `make run-hot` (resolve + engine) serves the hot-path
  oracle suites.** Phase 6 makes `make run` the full pipeline (resolve → engine →
  reconcile), per CLAUDE.md. But `tiny` (3 organic hot-misses) and `medium` (2)
  have long-tail organic conversions with no in-7d exposure but one within 90d, so
  a reconciliation pass over them would over-credit those organics (`path =
  reconciled`), shifting the frozen tiny golden, the pinned tiny accuracy (0.673,
  Phase 4), and the medium oracle==engine precision (92/130). Those are HOT-PATH
  oracles, so they run on hot-only output: new `make run-hot` (resolve + engine, no
  reconcile) backs `make test-int` (tiny, via CI), `make test-int-medium`, and the
  CI integration job. Reconciliation is proven on its own `long_delay` stack (`make
  test-int-long-delay`), mirroring how the medium live proof is local-only.
  Consequence noted for docs: the canonical `make run && make eval` demo on tiny
  now reflects post-reconciliation numbers (minor organic over-credit), while the
  pinned tests assert the hot path.

- **`long_delay` profile (seed 6), pinned live numbers.** `conversion_delay_minutes
  [10, 30240]` (10min–21d) straddles the 7d hot window and the 90d long window, so
  caused conversions split into a hot baseline and reconciliation candidates.
  Measured deterministically: 32 candidates (29 caused misses + 3 organic misses),
  29 recovered to the correct household, 3 organics unmatched (no in-90d exposure —
  exercises the "stays unattributed" path live). Recall 0.587 (44/75) → 0.973
  (73/75); the residual gap is 2 pre-existing shared-IP wrong-household hot
  attributions (not misses). Precision rose 0.530 → 0.652 here because the recovered
  rows were all clean caused — profile-specific; the general precision-dip risk
  (organic over-credit at long range) still holds and is called out. Pinned in
  `tests/integration/test_reconcile.py` and `tests/test_long_delay_profile.py`,
  updated together with the profile.

- **File-list deviations from the phase-6 spec, recorded.** (a) The ClickHouse
  row→model reconstruction lives in `reconcile/` (not `clickhouse/client.py`, which
  the spec listed) to keep `clickhouse/` decoupled from `producer/` models — the
  existing readers there return tuples/dicts, and `reconcile/` is the stage that
  owns its schemas. (b) The rollup/snapshot/restatement correctness is inherently
  ClickHouse behavior (versioned-replace collapse, FINAL), so it is proven in the
  live `tests/integration/test_reconcile.py` — the restatement asserts
  `report_snapshots`, AND a `campaign_hourly` assertion pins the versioned-replace
  invariant directly (one FINAL row per `(campaign_id, hour)`, values = the latest
  recompute) — with the offline `tests/test_reconcile.py` covering the pure matcher
  + `reconciled_at` derivation. Chosen over a services-free `test_rollup_snapshot.py`,
  which would have to re-implement the SQL in Python (a second implementation,
  against the shared-core rule). (Review-gate follow-up: an earlier draft of this
  note claimed the rollup was "proven live" before any `campaign_hourly` assertion
  existed — overstated; the assertion was added to make it true.)

## Phase 7

- **Alerts proven by `promtool test rules`, not a live scrape — but from REAL
  captured registries.** The batch stages exit before a 15s scrape, so a live
  Prometheus alert can't fire without continuous-follow infra (deferred). Instead
  each stage dumps its own terminal registry (`--metrics-out` →
  `write_to_textfile`), `make metrics-capture` orchestrates a real knobbed run, and
  `observability/gen_alert_fixtures.py` bakes those real numbers into the promtool
  fixture. The threshold-crossing value comes from a real stage run; only the
  live-scrape hop is simulated. Rejected: recomputing metrics through the offline
  oracle (circular — the fixture would reflect the generator's arithmetic, not the
  stage's) and a Pushgateway/textfile live path (a new service; it's the deferred
  path, BACKLOG). Chosen because it adds no service, stays deterministic, and is the
  honest proof for the two time-based alerts.
- **"Consumer lag" is `resolve_input_backlog` — a batch proxy, not group lag.** The
  stages read from `OFFSET_BEGINNING` every pass with no committed group offsets
  (BACKLOG 19), so redpanda consumer-group lag isn't tracked. The honest batch
  analog is the backlog the consumer clears at drain start (≈ topic size ≈
  f(volume)). It satisfies "triggerable by a knob" (volume), but it trips whenever
  volume crosses the threshold, not when a consumer genuinely falls behind — owned
  as a proxy, not oversold. True group-lag is the continuous-follow metric.
- **`WatermarkStall` is a peak-lateness proxy, not a true stall (fix #3).** PHASES.md
  says "watermark stall"; `engine_watermark_lag_seconds` measures peak
  `ingest−event` arrival lateness. A true stall is a watermark failing to advance
  over wall-time — and a batch drain has no advancing watermark to stall. The alert
  keeps the spec's name; the metric is the batch proxy (large lateness is what
  pushes conversions past the hot window into state-misses). True stall detection is
  a continuous-mode signal, deferred. Surfaced per STOP-on-spec-mismatch, not
  silently relabeled.
- **`engine_watermark_lag_seconds` computed engine-side, not in the pure core.** The
  peak-lateness scan lives in `run_engine` (`streaming/dataflow.py`), so the pure
  `attribute_household_streaming` stays a function of `(events, window,
  allowed_lateness)` only — the invariant that keeps the engine and the offline
  oracle byte-identical (BACKLOG 24). Same for why the restatement delta is computed
  in `reconcile.run` from `report_snapshots`, not in a leaf.
- **`engine_join_state_current` added alongside the monotone peak (BACKLOG 25).**
  `engine_join_state_size` is a high-water peak that only climbs; a dashboard wants a
  gauge that rises AND falls. `join_state_current` is set per household to its
  post-eviction retained count, so it varies across the run; under continuous follow
  it would rise and fall within a household too. Both are kept — peak is the scaling
  high-water, current is the live occupancy.
- **Bench measures via `X-ClickHouse-Summary`, equality asserted rounded to 6 dp
  (fix #2).** `clickhouse-connect`'s `QueryResult.summary` gives deterministic,
  cache-independent `read_rows`/`read_bytes` — the honest structural signal (latency
  at profile scale is noisy). The naive query answers the same question as the
  optimized one (verified: `campaign_hourly` refresh uses `report.sql`'s exact
  definitions), but sum-of-hourly-sums vs a single sum differ at ulp scale, so raw
  float `==` would flake — compare rounded. The profile was not tuned to force an
  optimized win; RESULTS.md reports the measured deltas and SCALING.md carries the
  volume story.
- **Alert rules carry no annotations yet.** promtool matches annotations exactly, and
  templated `{{ $value }}` annotations are brittle to pin across Python/Go float
  formatting. Annotations only surface in an Alertmanager notification, and live
  firing is the deferred push path — so the descriptive text lives in rule comments
  now, and annotations (with rendering) land with the live path. Keeps the fixture
  robust and drift-free.
- **Grafana dashboards committed as correct JSON, not a live render.** The batch
  stages aren't scraped, so panels populate only under the deferred push path. The
  dashboard carries correct queries against the real metric names and provisions
  cleanly (verified: Grafana loads "Attribution Integrity"); a live screenshot needs
  the deferred path. Not oversold as a live dashboard.

## Phase 8

- **`duplicate_flood` is a benign CONTROL, not a diagnosable fault (Ruling A).** The
  duplicate injector re-appends a timestamp-identical payload (Phase 5), the engine
  dedups it (full seen-set), and ReplacingMergeTree collapses any survivor — so
  `attributed_conversions`/`exposures_landed` FINAL are byte-identical with or without
  the flood (proven in `test_fault_profiles.py`: the attribution decision per
  `conversion_id` is identical dedup-ON vs dedup-OFF). A correctly-absorbed flood
  yields a *correct* number, and the agent (§4) exists to catch numbers that are
  *probably wrong*, so duplicate_flood's correct future agent output is **no-fault**;
  Phase 10 scores it as a **false-positive-rate control**, not a fault-recall case.
  Four consequences pinned: (1) collectors are strictly ClickHouse-derived — the
  dedup counter `engine_dedup_suppressed_total` stays a Prometheus/alert-plane concern
  (Phase 7), never a context field (a `.prom` side-channel would break "from
  ClickHouse", couple the collector to gitignored live-run artifacts, and hand Phase 9
  a field with no probe SQL behind it); (2) the fault taxonomy is labeled
  diagnosable-vs-control up front (shared_ip_spike/late_burst/co_view_bug/real_lift
  diagnosable; duplicate_flood + the Phase-10 no-fault baseline control), so Phase 10
  never scores "did the agent name duplicate_flood?"; (3) dedup correctness is already
  proven at the engine layer (Phase 5), the agent doesn't re-prove it; (4) the escape
  hatch for a *diagnosable* duplicate bug is a dedup-DISABLED + flood profile (inflates
  the ClickHouse tables, stays inside the from-ClickHouse model) — recorded, not built.

- **Full §4.2 `AttributionContext`, shape FROZEN at phase exit (Ruling B).** Every
  §4.2 field maps to a named §4.1 hypothesis the Phase-9 agent ranks (match-rate
  series → real-vs-inflation; campaigns → ROAS/CPA discontinuity; restatements →
  late-arrival distortion; window_edge → window-edge effects; ip_clusters →
  wrong-household; genre_reach → co-view inflation) — no speculative fields, so "minimal
  but scalable" means no field without a consumer, not fewest fields. The pydantic
  shape is the contract Phase 9 consumes: it adds probe SQL + ranking over the frozen
  model and must NOT add/rename fields — a genuinely-needed new field is a
  STOP-and-report back-edit to Phase 8 (which means changing `test_context_schema.py`
  in the same deliberate change), never silent churn. Two over-reach guards: (a)
  **co-view stays a RAW genre-reach stat** (exposures / attributed-conversions per
  genre) — the co-view-*adjusted* factor is NOT built (BACKLOG 26, deferred to the
  Phase-10 near-miss; "reporting never reads generation params"); (b) **restatement
  volume comes from `report_snapshots`** (PRE vs FINAL, via `restatement.sql`), never
  the Prometheus `reconcile_restatement_roas_abs_delta` (alert-plane) — same
  from-ClickHouse rule as duplicate_flood.

- **Collectors mirror `accuracy/` (pure core + readers + runner), N1.** `agent/
  collect.py` is pure aggregation over already-fetched rows (no I/O, no clock, no LLM —
  unit-tested with synthetic rows in `test_collect.py`); `agent/readers.py` holds
  fixed, parameter-free SQL over the serving tables (FINAL on both RMT tables);
  `agent/run_context.py` wires them and prints (`make context`). Campaign metrics and
  restatements REUSE `report.sql`/`restatement.sql` (one source with `make report`/`make
  restate`, no Python metric core to drift). The causal side file is never read in
  `agent/` — enforced by the existing truth-isolation guard, which forbids the very
  word outside `eval/` (caught a docstring during the build; the collectors say "causal
  side file" instead).

- **Fault profiles isolate ONE anomaly each; shared-IP found by offline seed search.**
  All five share the medium-scale graph/events baseline and flatten `co_view_multiplier`
  to 1.0 (except `co_view_bug`) and drop shared-IP/unknown-device to low (except
  `shared_ip_spike`), so each profile carries exactly one signal — cleaner for the
  Phase-9/10 agent eval than realistic-but-mixed profiles (the no-fault baseline keeps
  realistic co-view). `shared_ip_spike` (seed 0, `shared_ip_fraction 0.6`,
  `unknown_device_fraction 0.4`) was picked by an offline search over the pure oracle
  (`streaming.attribute`) for a seed where the most-recent-exposure reduction
  misattributes caused conversions to a wrong shared-IP household: **11 caused
  wrong-household, 0 caused misses** (the entire recall gap 0.8625 = 69/80 is
  wrong-household, cleanly isolated) — BACKLOG 20 satisfied, observed not assumed
  (`test_fault_profiles.py` + live `test_context.py`). `real_lift` (seed 3,
  `caused_conversion_rate 0.4`) is the clean near-miss counterpart (truth 157 ≈2× medium,
  0 wrong-household); `ip_clusters.ip_resolved_fraction` is the discriminator (0.42 on
  shared_ip_spike, low on real_lift). `co_view_bug` (seed 5, `sports ×4.0`): sports
  caused-per-exposure 0.768 vs ≤0.276 elsewhere, **below the `min(1.0, rate)` clamp**
  (BACKLOG 15 — observable, not saturated). `late_burst` (seed 7, `late.fraction 0.5`,
  `late.max_minutes 20160`): 5 hot-misses, peak arrival lateness ~13.8d (the WATERMARK
  proxy). Numbers pinned in `test_fault_profiles.py` like the medium/long_delay live
  pins — re-tuning a profile JSON means updating the assertion in the same change.

- **`shared_ip_spike` uses `make run` (full pipeline), not `run-hot`.** Its delays sit
  inside the 7d hot window, so it has no caused hot-misses; reconciliation's only
  candidates are organics (1 on seed 0, 0 recovered), which never touch the caused rows
  — so the caused-side pins (11 wrong-household, 69 correct, 80 truth) are
  reconciliation-invariant and the live `make eval` matches the offline oracle. `make
  run` (vs `run-hot`) is used only so `report_snapshots` exists for the context's
  restatement field (Δ≈0 here, correctly — shared_ip_spike is not a late-arrival fault).

- **ClickHouse aggregate aliases must not shadow a filtered column (§8 gotcha).** An
  aggregate aliased to a column name that a `WHERE`/`countIf` also references
  (`count() as attributed` with `where attributed = 1`) raises `ILLEGAL_AGGREGATION`
  (code 184) — ClickHouse binds the identifier to the SELECT alias, finding an aggregate
  in the filter. `agent/readers.py` aliases the aggregates to non-colliding names
  (`attributed_count`, `ambiguous_count`); `collect.py` unpacks positionally, so the
  names are cosmetic. Recorded in ARCHITECTURE §8.

- **`co_view_bug` is diagnosable @ Phase 10 (needs the adjusted factor), NOT from raw
  genre_reach (review-gate FG1, verified live).** The multiplier bug IS engineered and
  present truth-side — sports caused-per-exposure 0.768 vs ≤0.276 elsewhere, a >2.5×
  skew (`test_fault_profiles.py`, still asserted: it proves the producer knob fires
  below the clamp). But that skew does NOT survive into the ClickHouse-visible proxy
  the collector fills: live `genre_reach` (attributed conversions per exposure, ALL
  conversions) reads sports 0.561 vs comedy 0.522 — a ~7% margin, and comedy has no
  co-view boost. The organic baseline dilutes the caused-only skew, so raw genre_reach
  cannot discriminate co-view inflation from noise. This is why the raw stat is all we
  built and the co-view-*adjusted* factor is deferred (BACKLOG 26 → Phase 10): the
  adjusted factor supplies the per-genre expected baseline that makes reach
  interpretable. Consequences: (a) do NOT add a genre_reach skew assertion (it would be
  flaky/false — the margin is ~7%, not the truth-side 2.5×); (b) the taxonomy re-labels
  `co_view_bug` **"diagnosable @ Phase 10 (needs adjusted factor)"** — it is a fault the
  agent should eventually flag (unlike the duplicate_flood control), just not from
  Phase-8/9 signals alone; (c) **predicted STOP-and-report back-edit (Ruling B):** if
  Phase 9/10's adjusted-factor detection needs a new `AttributionContext` field (an
  expected/normalized per-genre reach), that is the one foreseeable back-edit to the
  frozen §4.2 shape — expected there, not a surprise.

## Phase 9

- **Agent model / effort / rep count pinned as config constants (Ruling A),
  `agent/config.py`.** `AGENT_MODEL = "claude-sonnet-5"`, `AGENT_EFFORT = "medium"`,
  `EVAL_REPS = 5`, `AGENT_MAX_PROBE_ROUNDS = 6`. Model is chosen on **capability, not
  cost** — all three tiers (Haiku/Sonnet/Opus) clear the §2 "$10" posture on the
  Phase-10 sweep (5 reps × 6 scenarios = 30 invocations; the AttributionContext is
  ~1-2k tokens, the cached prefix ~3-8k). The near-miss discriminator
  (`ip_clusters.ip_resolved_fraction`, elevated on shared_ip_spike / flat on real_lift)
  is a **pre-computed labeled context field**: the agent weighs a named number, a
  moderate inference Sonnet handles at medium effort. Opus buys no discrimination
  headroom the deterministic context doesn't already supply; Haiku's risk is misranking
  under the enum on one rep of a repeated sweep, undermining the Phase-10 headline.
  `medium` effort because the near-miss is a bounded weigh-two-signals judgment, not
  deep agentic work; `high` mostly buys output tokens. `EVAL_REPS` is DEFINED here and
  CONSUMED in Phase 10 (the fixed N behind the false-positive-rate table), pinned now
  so the budget is predictable.

- **Prefix caching wired in Phase 9, doubling as a determinism nudge (Ruling B).**
  `cache_control: {"type": "ephemeral"}` on the stable system block (system prompt +
  the hypothesis enum text; the probe tool list renders before it, so the breakpoint
  caches tools + system). This is the ~10× lever on the dominant input term. Caching
  REQUIRES a byte-stable, stably-ordered prompt (render order tools → system →
  messages), which is the discipline the determinism policy wants at the AI edge. The
  ≥1-probe loop contract guarantees a turn 2, so `make agent-run` can assert
  `cache_read_input_tokens > 0` on the second `messages.create`.

- **The agent is NOT byte-reproducible, by construction, and that is correct
  (Ruling C).** Temperature/top_p are removed on the entire Claude-5 family (400 on any
  value), so `temperature=0` is impossible; the loop never sets it. Output varies run
  to run — NOT a determinism-policy violation: the policy carves the AI out of the
  byte-identical guarantee ("AI sits at the edge; the pipeline is deterministic"). It
  is WHY Phase 10 runs each scenario repeatedly — the reps measure residual FP-rate
  stability, not byte-identity. So the Done-when gates `top_hypothesis ==
  device_graph_mismatch` + valid finding + a probe ran; the `CONFIDENT` verdict is
  reported as observed-expected, never asserted on the single run.

- **The SELECT-only user is a `users.d` config file, not SQL DDL (Ruling D) — chosen
  for identity-mechanism consistency, NOT because the SQL path is blocked.**
  *Empirical correction, verified live on `clickhouse-server:24.8`:* the stock
  `default` user ships with `ACCESS MANAGEMENT ... WITH GRANT OPTION`, so `CREATE USER`
  / `GRANT` via the apply.py path (host → HTTP 8123, passwordless `default`) returns
  200. An earlier draft claimed `default` lacked access management and the SQL path
  would fail at apply.py / force widening `default` — that was **wrong**: it inferred a
  missing privilege from a missing config file (absence of config ≠ absence of
  privilege). Both provisioning paths run; the choice is a preference. `agent_ro` lives
  in `clickhouse/users.d/agent-ro.xml` because (1, primary) **identity belongs to the
  compose-up config layer, by one mechanism** — `allow-network.xml` already declares
  principals+access in `users.d`, reconstructed from source at container start;
  `agent_ro` is the same kind of object (a principal + a grant), and a user is not
  schema; (2, supporting) config users **reconstruct from source** each start and can't
  drift, whereas a SQL user persists in the mutable `access/` store and `CREATE USER IF
  NOT EXISTS` then skips it (verified: a probe user created over HTTP persisted until
  explicitly dropped) — though a *correct* SQL path (`CREATE USER OR REPLACE` +
  unconditional `GRANT`) would also reconstruct, so this favors config, it doesn't
  block SQL. **Grant form, not `<readonly>1</readonly>`:** `<grants><query>GRANT SELECT
  ON default.*</query></grants>` is DB-enforced (write-denied test: INSERT/ALTER/DROP/
  CREATE → ACCESS_DENIED, proven live) and leaves the benign session settings
  clickhouse-connect sends alone; `readonly=1` can 400 on driver-set settings. Inline
  `<grants>` supported since CH 22.4 (image 24.8). Live-confirmed: `show grants for
  agent_ro` = `GRANT SELECT ON default.* TO agent_ro`, nothing else.

- **The SELECT-only user covers the WHOLE agent read path, not just probes (Ruling E,
  SN2/CA-Q4).** `connect_agent()` (new, `clickhouse/client.py`; reads
  `CLICKHOUSE_AGENT_USER`, default `agent_ro`) is the single read handle for both the
  Phase-8 collectors and the Phase-9 probes. `agent/run_context.py` is re-pointed to it
  — the one-line `connect()` → `connect_agent()` change the DI already made trivial
  (`collect(client, …)` threads its client into `report.run` / `restatement.run` /
  `readers.query`). `make context` now reads as `agent_ro` (same rows, SELECT-only) —
  a deliberate change fulfilling a Phase-8 forward-note (the user couldn't exist until
  Phase 9), not a Phase-8 back-edit. Without this, the read-only guarantee would hold
  on probes but not the collectors — the drift the coherence-auditor exists to catch.

- **Terminal `submit_finding` tool, not mixed structured-output.** The loop ends when
  the model calls `submit_finding`, whose input schema IS `AttributionFinding`. Keeps
  the whole model→app boundary in the typed-tool idiom and gives one escalation path: a
  pydantic `ValidationError` on the payload → synthesized `AMBIGUOUS_NEEDS_HUMAN`
  finding with the raw payload in `evidence_for`, never a silent retry. The tool is
  rendered with `strict: true` (`loop._strict_schema` inlines the pydantic `$defs`/
  `$ref` into strict's subset), which is **load-bearing, not belt-and-suspenders** — a
  live run proved the NON-strict schema let the model stringify the nested `ranked`
  array and escalate a correct finding (see the FT-1 bullet below for the incident and
  Ruling A). App-side `model_validate` is retained as the net, so the escalate-contract
  stays pure and fires only on a genuine semantic failure.
- **Reusable rule (generalizes the FT-1 lesson): for any tool whose input schema has
  nested arrays/objects, `strict` is load-bearing — default it ON from the start.**
  Non-strict is fine for flat scalar-param tools (the five probes) and dangerous for
  structured nested payloads (the finding). And the corrected fallback: if the API
  cannot accept the strict schema, FLATTEN the schema until it can — never drop to
  non-strict for a terminal payload, because app-side validation only *escalates*
  (masking a correct answer), it does not *correct* the stringification. (This
  supersedes an earlier "if strict is rejected, drop it and rely on app-side
  validation" note.)

- **≥1-probe loop contract (§4.2 "Test" is not skippable).** The observe-step context
  already carries the discriminator, so a confident model could `submit_finding` on
  turn 1 with zero probes — skipping the Test step and leaving no turn 2 to
  cache-assert. The loop tracks the probes it ACTUALLY executed; a `submit_finding`
  before any probe is rejected once ("run a probe first"), then escalated. On success,
  `finding.probes_run` is overwritten with the executed list (the authoritative record,
  not the model's self-report), so every finding cites a Test step and a turn 2 exists.

- **Manual tool-use loop over the SDK Tool Runner.** For control and testability —
  `agent_ro` probe dispatch, param validation, the terminal-tool escalation, prefix
  caching, and a mock-client unit test (`tests/test_loop.py`, zero tokens). Each turn
  appends the FULL `response.content` (thinking + tool_use blocks) back to `messages`,
  not just text — Sonnet-5 adaptive thinking returns `thinking` blocks that must be
  echoed unchanged within the same-model tool loop.

- **Webhook alert payload is a TRIGGER ONLY (security boundary, designed).** The sweep
  re-derives the AttributionContext deterministically from ClickHouse and does NOT feed
  alert labels/annotations into the LLM prompt — alert labels are attacker-
  influenceable, and re-observing from the DB is also the cleanest determinism story.
  The mocked-sweep test takes NO arguments, so alert text structurally cannot reach the
  sweep/LLM; the alertname is echoed in the HTTP response only. SN1 more broadly: the
  context reaches the model as a fenced ```json DATA block, never spliced into
  instruction text (`tests/test_prompt_injection.py`). Live push (scrape →
  Alertmanager → webhook) stays deferred (BACKLOG).

- **Probe SQL returns `day` as a string (`toString(toDate(...))`), found by a live
  probe check before the token run.** ClickHouse `toDate` returns a Python `date`,
  which `CampaignDayRow.day: str` rejects — the same coercion the collector does with
  `str(day)`. Fixed in `campaign_attributed_by_day`; guarded by a live probe test
  (`test_agent_readonly.py::test_every_probe_executes_and_returns_typed_rows`) that runs
  all five probes as `agent_ro` and asserts typed rows.

- **Review-gate dispositions (Phase 9 code-reviewer, applied):** **CR-1** — the probe
  formerly `campaign_match_rate_by_day` is renamed `campaign_attributed_by_day`: its SQL
  returns attributed conversions + revenue per day, NOT a rate (a per-campaign
  `processed` denominator is undefined — an unattributed conversion carries no
  campaign), so the name was the bug, not the SQL. The REAL-vs-UPSTREAM rate
  discrimination lives in the context's global `match_rate_by_day`; this probe supplies
  the per-campaign attributed trend alongside it. **CR-2** — `probes.run` now maps rows
  BY COLUMN NAME (`zip(result.column_names, row)` → `model_validate`), not positionally,
  so a future SQL/field reorder can't silently swap two same-typed values (e.g.
  `roas_as_reported`/`roas_now`); every probe SQL aliases each output column to its
  result-field name (convention enforced by
  `test_probes.py::test_every_probe_sql_aliases_cover_its_result_fields`). Security /
  functionality verdicts: PASS (both non-blocking security notes already tracked in
  BACKLOG). Done-when #1 (live agent-run) closed separately; gate-0 tiny-golden verified
  live post-run.

- **FT-1 residual materialized and closed at the source (strict submit_finding).** The
  first live `agent-run` on shared_ip_spike surfaced the exact risk functionality-test
  flagged: the model REASONED correctly (device_graph_mismatch, CONFIDENT, textbook
  shared-IP evidence — cluster 100.64.0.3 → 3 households, ip_resolved_fraction 0.42) but
  the NON-strict `submit_finding` tool let it return `ranked` as a **stringified JSON
  array** with the rest of the payload collapsed inside it, so `model_validate` failed
  and the app-side net escalated a correct finding to AMBIGUOUS_NEEDS_HUMAN. **Ruling: A
  (strict), not B (tolerant parse) or C (retry)** — A constrains the output to
  well-typed JSON at the source, so the (non-deterministic) malformation cannot occur;
  B/C react to a malformation whose shape varies (this run collapsed the WHOLE payload
  into one field, not just one array), and in a 30-run Phase-10 sweep a repair-based fix
  could silently re-escalate a correct answer on a variant shape, corrupting the
  accuracy table. A also keeps the output contract pure: escalate stays "on genuine
  semantic failure," never "recover from syntactic mangling" (which B would have made it
  — an actual contract amendment). `loop._strict_schema` renders `AttributionFinding`
  into strict's subset (inline `$ref`, `additionalProperties:false` + all-keys
  `required` per object, drop `title`); it fits because the model has no Optional/None
  fields (the `float | None` fields live in the CONTEXT models, not the finding, so no
  `anyOf`-null branch). Confirming live run: strict accepted by the API, native `ranked`
  list, device_graph_mismatch / CONFIDENT, turn-2 cache_read 2857. The exact malformed
  payload is committed as a permanent regression fixture
  (`tests/fixtures/malformed_submit_finding_input.json`, asserted through `_finalize`) — the
  highest-value artifact from the run.

- **Phase-10 forward-note (scoring must not misread the escalation default).**
  `escalation()` sets `top_hypothesis = UPSTREAM_DATA_CHANGE` — a REAL catalog member,
  chosen as a neutral default, not a diagnosis. Phase-10 fault→diagnosis scoring MUST
  key an escalation on `verdict == "AMBIGUOUS_NEEDS_HUMAN"` and treat it as an
  ABSTENTION, never read the `upstream_data_change` default as "the agent diagnosed
  upstream" — otherwise an escalation scores as a wrong diagnosis instead of a
  (correct-to-escalate) abstention, biasing the accuracy/false-positive tables.

## Phase 10

- **BACKLOG 26 (co-view adjusted factor) closed as a won't-do (Ruling A).** Row 26
  anchored the trigger to "the Phase-10 near-miss demo (real-lift vs shared-IP)," but
  that near-miss is a **device-graph / shared-IP** discrimination — it turns on
  `ip_clusters.ip_resolved_fraction` and candidate counts, not on any genre number.
  Walking Phase 10 end to end (no-fault baseline → 5-fault sweep → real_lift/shared_ip
  near-miss), **nothing consumes a genre-adjusted advertiser number** — exactly the
  HARD STOP condition row 26 named. Building it (rejected on the merits, not just scope):
  the honest per-genre expected baseline does not exist in serving data (Phase-8
  finding — the truth-side 2.5× skew collapses to a ~7% margin, sports 0.561 vs comedy
  0.522), so a positive `co_view_bug` diagnosis would need the expected baseline fed
  from the producer's co-view multiplier — the reporting-reads-generation-params
  coupling row 26 forbids — or a 7%-margin detector flaky by construction. Either
  "works" only because it was told the answer, undercutting the determinism/isolation
  spine. **Co-view stays a producer-realism knob, not a reporting factor**;
  `CO_VIEW_INFLATION` stays a caveated enum member the agent never returns as a
  CONFIDENT top hypothesis. Two record-fixes: (a) **row 26's anchor was mis-specified** —
  the near-miss is shared-IP/device-graph, not co-view, which is *why* the stop fires;
  a future reader should not trip on the stale "near-miss needs it" text. (b)
  `co_view_bug`'s abstention is NOT the same as `duplicate_flood`'s: duplicate_flood
  abstains because **nothing is wrong** (a benign control); co_view_bug abstains because
  the real fault is **undiagnosable from serving data by design** (a labeled capability
  boundary). Both score correct-abstention, but the table + RESULTS label them
  distinctly — never conflate "found nothing" with "couldn't see it".

- **Four scoring buckets (Ruling B/C).** Every rep scores against the scenario's `kind`:
  `fault_recall` (shared_ip_spike/late_burst — CONFIDENT ∧ top == expected is correct;
  AMBIGUOUS is an over-cautious `ABSTAINED` miss; CONFIDENT ∧ wrong top is
  `WRONG_DIAGNOSIS`), `negative_confirmation` (real_lift — see next bullet),
  `capability_boundary` (co_view_bug — abstention-expected but for a *seeing* reason,
  excluded from the FP-rate denominator), and `control` (duplicate_flood +
  no_fault_baseline — abstention-expected because nothing is wrong; **these two are the
  FP-rate denominator**, §4.3). `verdict == AMBIGUOUS_NEEDS_HUMAN` is ALWAYS read as
  abstention, never the escalation-default `top_hypothesis` (the Phase-9 forward-note).
  The rubric is a PURE function (`agent/eval/scoring.py`) unit-tested exhaustively with
  synthetic findings, so the token-gated live sweep is a thin data-capture step — every
  scoring path is decided offline before a token is spent.

- **`real_lift` is `negative_confirmation`, not `fault_recall` (design-review fix).** A
  first draft scored real_lift as `fault_recall` requiring a CONFIDENT
  `real_performance_change`. That answer is **structurally unreachable from the frozen
  context**: `AttributionContext` has `match_rate`/`match_rate_by_day`/absolute
  `campaigns` but NO baseline/vs-prior/vs-expected field — the agent sees one run, so
  "match rate/ROAS is UP" is not observable, and the lift is uniform (a 2×
  caused-rate, not a temporal ramp the `campaign_attributed_by_day` probe could key on).
  Flat `ip_resolved_fraction` only rules OUT device_graph; after ruling out every
  distortion the honest read of real_lift is "healthy pipeline," which **Ruling E routes
  to abstention** — so a prompt-compliant agent would abstain and the fault_recall rubric
  would have scored that correct behavior as an `ABSTAINED` miss on the §4.3 checkpoint
  headline (prompt-vs-rubric contradiction). Resolved on the rubric side, matching
  PHASES.md ("DECLINES to fire device_graph_mismatch"): **correct = abstain OR confident
  real_performance_change (bonus); FAILURE = confident device_graph_mismatch (the
  near-miss NEGATIVE-half failure) or any other fault.** This keeps the near-miss's real
  teeth (fire device_graph on shared_ip, do NOT on real_lift — the discrimination that IS
  observable) without penalizing the agent for correctly not inventing a fault. Pure
  scoring.py change, no context back-edit, no reseed. Rejected alternative: adding a
  temporal-ramp knob + reseed — heavier, and a within-window ramp is the wrong model for
  a performance lift.

- **Full `make run` per scenario injects no spurious restatement on the in-window
  profiles (Ruling: pinned).** The sweep runs the FULL pipeline (incl. reconciliation) so
  late_burst's restatement signal exists, but the three in-window scenarios
  (shared_ip_spike / real_lift / no_fault_baseline — all event-time delays inside the 7d
  window) must recover nothing on the long window, else a spurious `roas_delta` could bait
  a false `late_arrival_distortion`. Pinned offline
  (`test_in_window_scenarios_recover_nothing_on_the_long_window`): attributing at
  HOT_WINDOW (7d) and LONG_WINDOW (90d) over the same streams credits the identical
  conversion set for all three, so reconciliation writes no corrected rows and
  report_snapshots PRE == FINAL. late_burst is excluded — its misses are arrival lateness
  / eviction (not event-time), so it genuinely restates.

- **The no-fault baseline is `seed 1`, medium-scale, with REALISTIC co-view.** The five
  fault profiles flatten `co_view_multiplier` to isolate one anomaly; the baseline is the
  one profile that keeps a realistic non-flat co-view (sports 1.5, comedy 1.2) so the
  sweep's control is realistic, not sterile (DECISIONS Phase 8). Seed 1 is offline-clean:
  truth 90, 90 correct, 0 wrong-household, 0 state-misses, recall 1.0, delays inside the
  7d window — nothing for the agent to confidently flag. Precision 0.71 is normal
  last-touch organic over-credit (like tiny), not a fault. Pinned in
  `test_fault_profiles.py` like the other profiles.

- **One prompt sentence added for the no-fault abstain path (Ruling E).** The Phase-9
  prompt asked the agent to decide whether a number is "probably WRONG" but never blessed
  a clean-baseline outcome; the controls are a first-class §4.3 requirement, so
  `SYSTEM_PROMPT` gains: *if no signal indicates a probably-wrong number, do not invent a
  fault — submit AMBIGUOUS_NEEDS_HUMAN.* The cached prefix's byte value changes but stays
  stable within Phase 10 (caching discipline intact). This is the only agent-behavior
  change this phase; it makes "leave the control alone" an explicit instruction rather
  than an inference the model must make unaided across 10 control reps.

- **The agent is non-reproducible; the reps MEASURE residual stability (Phase-9 Ruling
  C).** Temperature is unset on the Claude-5 family, so per-rep output varies. `EVAL_REPS
  = 5` per scenario is exactly why the tables report a rate (k/5), not a single-run claim;
  verdict/hypothesis stability across reps is a measurement, never a gated assertion — the
  AI edge is carved out of the byte-identical guarantee (CLAUDE.md). The sweep drives its
  own clean stack per scenario (`make down && up && seed && run`, full run for the
  restatement field) because profiles share `conversion_id` space (Phase 5); FG2 (BACKLOG
  31) is satisfied by rendering each profile's deterministic live context headline into a
  dedicated per-profile headline table in RESULTS (`tables.headline_table`, all six
  scenarios — not just the near-miss pair, which the coherence audit caught as an early
  overclaim).

## Post-Phase-11 fixes

- **`make bench` canonicalizes both read sides to merged steady state before measuring
  (BACKLOG 29, `fix/bench-direction-guard`).** Adding the magnitude-free direction assert
  (`optimized read_rows < naive`, the row-29 guard) surfaced a latent non-determinism it
  did not create: `read_rows` from `X-ClickHouse-Summary` counts the physical rows a
  `FINAL` scan reads, which for a versioned-replace ReplacingMergeTree includes every
  un-merged version-part (ARCHITECTURE §8 gotcha). `campaign_hourly` gains a full copy per
  rollup refresh and `attributed_conversions` a superseded version per reconciled
  conversion, so the measured read-size drifted with refresh count and background-merge
  timing. CI runs `make bench` right after `test_reconcile.py` refreshes the rollup two
  extra times, so it measured 1020 rollup rows and the benchmark **printed the rollup
  reading MORE than the naive scan (0.8×)** — the RESULTS headline did not reproduce in
  CI's run-state, and on `main` (no guard) that false-green went unnoticed because the
  equality gate does not check direction. **Chosen fix:** `_canonicalize` runs `OPTIMIZE
  TABLE ... FINAL` on all three read tables (`attributed_conversions`, `exposures_landed`,
  `campaign_hourly`) at the top of `run()`. **Why all three, not just the rollup:** the
  naive side version-stacks too (the 29 reconciled rows are higher-`processed_at` versions
  of previously-hot `conversion_id`s), so optimizing only the rollup would compare a merged
  rollup against a maybe-stacked naive scan — still non-deterministic, and apples-to-
  oranges. Optimizing both is the only version that makes `read_rows` deterministic on both
  sides AND an honest steady-state comparison (the form a scheduled rollup serves in
  production). **Honesty consequence, not hidden:** collapsing the naive side dropped it
  835 rows / 25.7 KB (was measured at 864 / 42 KB un-merged), so the steady-state **byte**
  win is 1.2×, not the previously-reported 1.6×; RESULTS was updated to the both-merged
  numbers (rows still 2.5×). Rejected alternatives: reordering CI so `bench` runs before
  the extra-refresh tests (leaves `bench.py` non-deterministic for every other caller);
  weakening/removing the guard (defeats row 29, and CLAUDE.md forbids weakening a failing
  test to make it pass).

## Phase 16

- **Deletion-first: three boxes removed, none added.** The Phase-15 architecture
  review found three components that were neither a seam for another team nor a
  scale boundary: the Bytewax dataflow (a `TestingSource` + `fold_final` wrapper
  over the batch drain), the shared-IP fan-out + `conversion_id`-keyed reduce (two
  operators to make a fast guess reconciliation could make correctly), and the
  resolve stage as a consumer + topic + subject (for an in-memory dict lookup).
  Central constraint, held: **same answer after reconciliation, fewer moving parts**
  — tiny post-reconcile 52/35/35 and medium 130/92/92 equal the pre-Phase-16 hot
  numbers; long_delay hot→post recall 0.587→0.973 is unchanged; shared_ip_spike
  post-reconcile is 69/80 correct / 11 wrong-household, the identical pick the old
  hot reduce made. Hot numbers moved (tiny 47/35/32, medium 129/92/91) and that is
  the point: the hot path no longer credits a household it cannot be sure of.
- **Ambiguous → reconciliation, never a hot guess (the hot rule).** In
  `streaming/attribute.py` a resolved conversion with `candidate_count > 1` is
  emitted unattributed (reason ambiguous_ip) without probing state. The leaf was
  split: `last_touch` (household-local, ambiguity-blind) and `attribute_household`
  (the hot rule over it). Reconciliation scores each candidate household with the
  same `last_touch`. Hot-path `caused_wrong_household` is 0 by construction on every
  profile (shared_ip_spike: was 11). Advertisers get a late correct credit instead
  of a fast wrong one; shared IPs remain the ONLY wrong-household source, now only on
  the reconciled path.
- **One row per `conversion_id` still enters the ReplacingMergeTree — via a
  placeholder, not a reduce.** `one_row_per_conversion` collapses a fan-out to its
  lowest-`household_id` candidate (the rule the old reduce used when every candidate
  was unattributed) before the join. That household is a placeholder, never
  credited; reconciliation re-enumerates the candidates. This keeps DECISIONS Phase
  3 (b) true (`conversion_id` a safe RMT sort key) with no second keyed stage.
  Rejected: emitting N per-household unattributed rows (the RMT survivor would
  depend on write order — exactly the nondeterminism Phase 2 rejected).
  **The placeholder rule, written down because a reader WILL see it in
  ClickHouse:** an unattributed `ambiguous_ip` row carries `household_id` = the
  **minimum `household_id` among the IP's candidate households** (string order,
  the same sorted order `GraphIndex.owners_of` emits), `resolution='ip'`,
  `ambiguous=1`, `candidate_count=N`. It is a deterministic stand-in, not a
  decision: it was never credited and reconciliation ignores it (candidates are
  re-enumerated from the graph). tiny example: c-000016 (cc=3, owners h-0000 /
  h-0001 / h-0005) sits on h-0000.
- **The tiebreak moved, not rewritten, and lives in ONE place.**
  `reconcile.pick_household` is the old `reduce_conversion` body — most-recent
  last-touch exposure, ties `exposure_id` then `household_id`, attributed beats
  unattributed, all-unattributed → no recovery (stays the hot row). `grep
  reduce_conversion` returns nothing. Deterministic: candidate order is the graph's
  sorted owners; the max key is a total order.
- **Reconciliation reads the device graph from the compacted topic, not a landed
  table.** `expand_candidates` re-resolves an ambiguous placeholder with the
  engine's own `resolve_one` against `load_graph_index(broker)` — the same loader,
  the same graph, so `make reconcile-dagster` (Dagster asset passes the same
  loader) stays byte-identical to `make run`'s pass. Cost: the reconcile job now
  needs the broker, which `make run` and the Dagster runner already have up.
  Rejected: a `device_graph` ClickHouse table landed by the engine — a new DDL
  table, a new insert, a new read, and a second copy of the graph to keep equal, in
  a phase whose point is deleting concepts. Phase 17 (lake of record) may revisit if
  reconciliation is ever meant to run without the broker.
- **Resolve stays a module with the Phase-2 signature.** `resolve_one(conversion,
  graph) -> list[ResolvedConversion]`, `GraphIndex`, `resolve_stream`, `make
  resolve` (offline replay, the unit proof) and the `resolve_` metrics are all
  unchanged; the engine calls the function in-process and emits the metrics from
  its registry (`resolve_input_backlog` etc. now land in `engine.prom`). It becomes
  a separate service again when the device graph is owned by another team or a
  vendor; the interface is the function, not the topic. Removed: the
  `conversions_resolved` topic, its `-value` subject, `resolve.stage`'s producer
  path, `tests/test_resolve_schema.py` and `tests/integration/test_resolve_stage.py`.
  No compose change was needed — the topic was created by the stage itself.
- **Remove Bytewax rather than make it real.** `streaming/dataflow.py`
  `run_attribution` buckets by household and runs `attribute_household_streaming`
  directly; `streaming/sink.py` keeps only the row builders; inserts are chunked
  synchronous `client.insert`. The evicting-vs-non-evicting oracle parity (Phase 5)
  holds byte-for-byte — it was always a property of `attribute.py`. `bytewax` is
  out of `pyproject.toml`, `uv.lock` and the CLAUDE.md allowlist (the first package
  removal). Continuous follow is a framework choice (Bytewax proper vs Flink) for
  Phase 17+, chosen with fresh eyes; SCALING.md's mapping table is retained as the
  port target, re-headed "Engine construct here".
- **Fixture re-freeze: the one sanctioned exception, one commit, signed off.**
  `fixtures/tiny/expected/attributed.jsonl`: every line gains the new `reason`
  key (null on the 47 attributed rows, `state_miss` on 3, `ambiguous_ip` on 5),
  and exactly 5 DECISIONS change (c-000014, 16, 25, 41, 42 — the five shared-IP
  conversions: `attributed` false, `exposure_id` null, `assists` empty,
  `household_id` the placeholder); the other 50 decisions and
  `conversions_resolved.jsonl` are byte-identical; producer output is untouched
  (`test_fixtures.py` reproducibility unchanged). The developer reviewed the diff
  before it was committed. Fixtures are read-only again from here.
- **`reason` column added (developer ruling) — the spec's premise was wrong and
  was reported, then resolved by adding the column.** The spec named "a new
  `reason` value alongside the existing state-miss reason"; no such field existed.
  The first cut shipped without it (the implicit contract `attributed=0 and
  path='hot' and candidate_count>1`) and reported the gap; the developer ruled to
  make it explicit NOW so reconciliation, the agent's probes and Phase 18's
  dirty-set never re-derive it, and so tiny is re-frozen once. Shape:
  `AttributedConversion.reason: Literal["ambiguous_ip", "state_miss"] | None =
  None` (null when attributed — a reconciled credit clears it), DDL column
  `reason Nullable(String)` plus an idempotent `alter table … add column if not
  exists` for volumes created before it, sink column appended, `_attributed` sets
  it from `candidate_count`. `_read_candidates` reads it and raises if it disagrees
  with `candidate_count` (a writer bypassing the engine). Zero-diff pin
  clarified: it covers producer OUTPUT (topics, truth links, profiles);
  `AttributedConversion` is the engine's table model that happens to live in
  `producer/models.py`. Schema-registry note: `AttributedConversion` is not a
  registered subject and `ResolvedConversion` no longer is, so this change
  re-registers nothing — the `ResolvedConversion` / `schemas.py` docstrings were
  cleaned in the same commit (the old "Topic: conversions_resolved").
- **The promtool alert fixture was recaptured; tiny now trips `RestatementMagnitude`
  and the fixture says so.** `make metrics-capture` on clean tiny and long_delay
  stacks, then `gen_alert_fixtures.py`: long_delay's hot attributed 83 → 80 and
  restatement 27.0 → 41.4 (three more recoveries), tiny's attributed 52 → 47 and
  restatement 0 → 12.86 — tiny's reconcile pass now credits its 5 deferred shared-IP
  conversions, a real ROAS restatement above the 1.0 threshold. The honest fixture
  claim is therefore "long_delay fires all four; tiny fires RestatementMagnitude
  only" (the other three still discriminate). Rejected: keeping the pre-Phase-16
  fixture values (CI green but the provenance would no longer reproduce from a
  capture) and capturing tiny hot-only (the restatement series comes from the
  reconcile registry). RESULTS "Observability — alert rules" and the CLAUDE.md
  `test-alerts` line updated to match.
- **`make test-int-shared-ip` runs `run-hot` and the test runs the reconcile pass
  itself.** Pinning both sides (hot 61/19/0 → post 69/11) from one stack is not
  possible after `make run` — FINAL collapses the hot rows under the reconciled
  versions — so the target stops at the hot path and `test_context.py` calls
  `reconcile.run()` between its two assertions. `test-int-agent` keeps `make run`.
- **`producer/models.py` still says "Topic: conversions_resolved" in the
  `ResolvedConversion` docstring.** Left as-is under the producer zero-diff pin (a
  docstring edit also bumps the generated JSON schema description); flagged for the
  developer as a one-line follow-up.

## Phase 15

- **The runbook is a retrospective incident log by choice, not a forward playbook.**
  Phase 15's "elevate, invent nothing" constraint (spec central constraint) means
  `docs/RUNBOOK.md` carries only incidents that already happened and trace to an
  ARCHITECTURE §8 gotcha / DECISIONS / RESULTS fact. The named cost: it cannot carry
  speculative first-response guidance ("snapshot count doubled → check client tz first")
  that no recorded gotcha states verbatim — the per-incident *generalization* lines bridge
  that only partly. This was the right trade for this phase (its whole value was provable
  traceability and earning the review gate's trust; speculative remediation would have
  muddied that). A forward-response playbook is a deliberately separate, later artifact
  that would relax invent-nothing **under human review** — recorded here as a boundary
  chosen, not a Phase-15 omission.

## Phase 14

- **The asserted `bytes_per_exposure` is a STRUCTURAL measure; `tracemalloc` is a
  labeled cross-check only.** `tracemalloc` peak drifts with the Python allocator and
  GC timing (nondeterministic run to run), so building the SCALING constant on it would
  break the determinism policy — the same trap as the Phase-7 `FINAL read_rows` fix.
  The reported number is instead `deep sys.getsizeof(retained hot-window state) ÷ entry
  count` (`streaming/scale_probe.deep_sizeof`): shared objects (interned category
  strings) counted once via an id() set, so the total is the real object-graph RAM and
  is identical on every re-run under a fixed seed and single thread. Measured **~571
  B/exposure** (571–573 across the 1k/10k/100k curve), replacing the old ~200 B guess;
  `tracemalloc` (~0.75× the structural total) is printed by `make scale-curve` as an
  independent console cross-check and is never asserted AND never written into the
  committed `docs/SCALING.md` — committing it would make the make target non-idempotent
  (the Phase-14 review-gate blocker: the doc block first shipped a tracemalloc column at
  0.1 MB precision that drifted on re-run). Rule this locks in: every column in the
  byte-stable committed doc is a deterministic structural field, each pinned by the
  `test_scale_probe` determinism assert (`_structural` covers all four table columns +
  `join_state_peak`; it excludes only `tracemalloc_peak_bytes`, which is console-only).
- **Only `bytes_per_exposure` moved asserted → measured; the rate and the product stay
  extrapolation.** SCALING's "order-of-magnitude sizing, not benchmarked capacity"
  framing still governs everything except the one constant. The 25k/sec rate and the
  `rate × window × per-exposure` multiplication remain an assumption — the block labels
  the `~8.6 TB` line "Extrapolation" explicitly. The curve measures event COUNT in-window
  (state **occupancy**), which models `exposure_rate × window` directly; it is NOT a
  msgs/sec throughput benchmark (that needs continuous follow, deferred, owned by no
  phase).
- **Tiers scale households with the event count (fixed per-household density), not one
  giant household.** The batch drain's per-household streaming pass is O(exposures²) per
  key, so 100k exposures in ~20 households (5k each) does not finish; realistic scaling
  keeps each household seeing few ads and adds households, so the drain stays cheap (100k
  in ~2.5s) AND the model is realistic (a household seeing 5k ads in 4 days is not). The
  fixed span (`SPAN_HOURS=100` < 7-day window) means nothing evicts, so the retained
  state equals the deduped input — `measure_tier` asserts `evicted == 0` so the
  structural measure stays valid; if a future retune makes eviction fire it raises rather
  than silently mismeasuring.
- **Spec-vs-repo file convention: the spec named `producer/profiles/scale_curve.py`, but
  profiles in this repo are JSON (`producer/profiles/*.json`).** Followed the real
  convention — a single reusable `scale_curve.json` (the 100k top tier) that
  `streaming/scale_probe.py` scales down per tier via `Profile.model_copy`. Surfaced, not
  silently "fixed" in the spec (workflow rule). The base profile is deliberately the full
  volume tier so Phase 13 (cost levers — skip indexes/projections are no-ops below one
  8192-row granule) can reuse it directly rather than duplicating a volume profile.

## Phase 13

- **Schema-reality correction: the spec's levers named columns/keys the serving schema
  does not have; corrected in the open before any build.** The `specs/phase-13-query-cost-levers.md`
  contract, as written, was wrong on two counts, verified against `clickhouse/ddl.sql`:
  (1) it put a projection `(campaign_id, event_time)` on `attributed_conversions`, but
  that table has **no `campaign_id` column** — campaign is derived by joining
  `attributed_conversions.exposure_id → exposures_landed.campaign_id` (`queries/report.sql`),
  and a projection can only order by columns the table has; (2) it put a bloom
  data-skipping index on `exposures_landed.campaign_id`, but `exposures_landed` is ordered
  `(campaign_id, event_time, exposure_id)` — campaign_id is the **leading** primary-key
  column, so the sparse primary index already prunes a campaign filter and a secondary
  index on the same column is strictly redundant (before ≈ after, the direction assert
  would fail). Per the workflow rule (spec seems wrong → STOP and report, never silently
  repair), this was surfaced to the developer, who approved the corrections and a spec
  edit as the first commit. Corrected levers: projection ordered by **`event_time`** on
  `attributed_conversions` (same alternate-ordering mechanism, buildable column) + a
  **date-scoped report variant** to exercise it; lever 2 **measured** (see below); PREWHERE
  measured `optimize_move_to_prewhere=0` vs explicit `PREWHERE`.
- **The all-time per-campaign report is already near-optimal for this schema; the levers
  win on date-/dimension-scoped access patterns.** Consequence of the two facts above:
  campaign is primary-key-pruned on `exposures_landed`, and the report has no other
  selective predicate, so there is nothing left to prune on the all-time report. That is
  not a gap — it is exactly when a platform reaches for projections/skip indexes/PREWHERE:
  a scoped access pattern (a date range, one genre) that the base sort key does not serve.
  Each lever therefore carries the query variant that exercises it; "a lever needs a query
  that exercises it" is expected, not a fudge.
- **Lever 2 is measured, not assumed; a documented negative result is a first-class
  landing.** A secondary skip index wins only when the indexed column is physically
  clustered (correlated with row order), which on the seeded data depends on the generator,
  not on wishes; and `SELECT ... FINAL` is often already optimized. So lever 2 is decided
  by measurement, ranked FINAL-vs-`argMax(...) GROUP BY conversion_id` (the schema-native
  lever — the serving layer is all ReplacingMergeTree + FINAL, the exact cost RUNBOOK
  incident #1 is about) > a clustered non-leading skip index (clustering measured) >
  a documented negative result (stating precisely *why* a secondary skip index does not
  help on this schema and the condition that would change it). Rejected: adding a
  denormalized `campaign_id` column to manufacture a projection win — that is schema
  surgery that trips the golden gate and is the "tuning the setup to inflate the win" the
  spec's own out-of-scope forbids.
- **New `bench_large` profile, not `scale_curve` reuse.** Phase 14's `scale_curve`
  (100k exposures) was sized for an in-process engine drain, not a full pipeline load into
  ClickHouse; Phase 13 adds `bench_large` sized so `attributed_conversions` and
  `exposures_landed` cross **several** 8192-row granules through the live stack. Counts are
  verified above the granule floor before any lever is measured (a sub-granule table is one
  granule and every lever is a no-op).

## Phase 12

- **ARCHITECTURE §3.5 reversed (approved).** Iceberg landing was listed out of scope
  for v1; Phase 12 adds it. A committed-spec reversal is a STOP-and-report event
  (CLAUDE.md workflow) — the developer approved the reversal and the five-package
  allowlist add (`pyiceberg`, `pyarrow`, `duckdb`, `dagster`, `dagster-webserver`)
  before the branch opened.
- **Iceberg metadata is non-deterministic → carved out of the byte-identical
  guarantee, exactly like the agent.** An Iceberg append stamps a fresh `snapshot_id`
  + commit timestamp per run, so table *metadata* is not byte-identical across runs
  even though the *rows* are. Every asserted check reads row content back from the
  lake; none assert on snapshot ids, commit times, manifest paths, or Dagster run
  ids/wall-clock. The tiny golden gate keeps reading the deterministic ClickHouse copy.
- **Dual-write parity-by-construction; spec hook-wording corrected.** The lake is
  landed from the SAME in-memory deduped `exposures` list that feeds the ClickHouse
  sink, at `streaming/dataflow.py` `run_engine` (the list at the `build_flow` call) —
  NOT the spec's literal "`insert_exposures` call site," which is a per-batch Bytewax
  sink, not a scalar call. Landing one level up gives lake == ClickHouse input set by
  construction (and inherits the `ENGINE_DEDUP=off` case). Surfaced, not silently
  repaired; developer approved. The spec's file-scope wording is corrected to match.
- **Landing is gated `--lake-land`, the SOLE landing site; off for make run/CI.** So
  the engine path stays byte-identical, the lake stack stays out of every non-lake
  run (bounded blast radius, the spec pinned decision), and there is no double-land.
  A re-land is harmless anyway (dedup-on-read).
- **Idempotency is on READ, not write (append accumulates).** An Iceberg append only
  accumulates rows — unlike the ClickHouse ReplacingMergeTree, which collapses re-sends
  on its sort key at FINAL. The DuckDB read does `select distinct` to collapse the
  re-sends, chosen over replace-on-write so append semantics stay honest. The spec's
  "idempotent append" is really idempotent-*on-read*.
- **Invariant the dedup rests on: `exposure_id` is unique per exposure.** The
  deterministic producer maps one `exposure_id` to exactly one
  `(campaign_id, event_time, …)` row, so a distinct-on-`exposure_id` lake read equals
  ClickHouse's FINAL collapse on the full `(campaign_id, event_time, exposure_id)` sort
  key. Recorded so a future producer change that made `exposure_id` non-unique would be
  flagged, not silently diverge the two dedup paths.
- **Lake read returns NAIVE UTC, matching clickhouse-connect.** clickhouse-connect hands
  DateTime64(3,'UTC') back as a naive datetime holding the UTC wall-clock (tzinfo=None);
  the matcher compares candidate (ClickHouse) vs exposure (lake) event_times, so they
  must be the same representation. The DuckDB read does `SET TimeZone='UTC'` (renders the
  timestamptz at the correct UTC wall-clock — the §8 defense; a default local session
  would shift it) then drops tzinfo. Found by the live run (aware-vs-naive TypeError in
  the matcher); the fidelity canary was corrected from aware to the naive-UTC contract.
- **Land as TIMESTAMPTZ, ms-truncated.** Columns land as Iceberg `timestamptz` (a UTC
  instant), never naive; event/ingest times are UTC-normalized and truncated to
  millisecond so the stored value is bit-for-bit the DateTime64(3) ClickHouse holds
  (producer times are already tz-aware UTC + ms-granular, so the truncation is a
  defensive no-op on real data).
- **Day partitions are a STATIC set, not `DailyPartitionsDefinition`.** The idiomatic
  daily-partitions construct validates keys against the real wall clock and rejects any
  day not yet elapsed — but the deterministic producer emits conversion days in the
  wall-clock future (long_delay trails ~33 days past a 2026-08-01 sim_start, past
  "today"), so a `DailyPartitionsDefinition` run would accept a *different* partition set
  depending on when it ran: a determinism violation. Found live (`DagsterUnknownPartitionError`
  after today's date). A fixed `StaticPartitionsDefinition` of day keys is reproducible
  on any re-run and still day-granular + backfillable.
- **Dagster UI is local `dagster dev`, not a compose webserver.** The spec contradicted
  itself (file-scope said "optional dev server," the Review section said "compose
  service"); resolved toward the file-scope + the headless DONE, per the minimal-but-
  scalable rule — a containerized/published Dagster webserver is speculative deployment
  infra, a SCALING/deployment lever, not built (same posture as async inserts and the
  Flink port). `make dagster-ui` binds loopback (127.0.0.1) with `DAGSTER_HOME` under
  gitignored `data/`; the headless `make reconcile-dagster` uses an ephemeral instance
  (nothing persists). The spec's Review-section wording is corrected.
- **`pyiceberg[sql]` reality: no `[sql]` extra in 0.11.1; writes need `pyiceberg-core`.**
  The spec named `pyiceberg[sql]`, but pyiceberg 0.11.1 has no `sql` extra (SqlCatalog's
  deps are base) and the Rust write engine ships as the `pyiceberg-core` extra. Added
  the extra (a sub-extra of the approved pyiceberg, not a new top-level package). Another
  spec-vs-reality precision fix, same class as the earlier phases' `.json`/projection
  corrections.
- **Only the reconcile *source* swaps; everything else stays ClickHouse.** DuckDB is the
  one lake compute engine (Spark/Trino are the SCALING port, not built); `exposures_landed`
  is KEPT as the serving/benchmark copy (dual-write, not replaced). The pass is factored
  into `recover()` (per-day, source-agnostic, given a fixed global `reconciled_at`) +
  `finalize()` (global snapshots + rollup); `run()` composes both with the same operations
  in the same order, so `make run` is byte-identical.
- **The DuckDB `iceberg` extension is the ONE dependency not hash-pinned in uv.lock
  (installed at setup, loaded offline).** DuckDB fetches the `iceberg` extension (a
  signed binary, tied to the duckdb version) from extensions.duckdb.org — it is not a
  Python package, so it lives outside `uv.lock`'s hashed supply chain. To keep the
  offline unit suite (`make test`, run in CI) truly network-free (both code-reviewer
  and security-reviewer flagged the original runtime `install`): `make setup` and the
  CI offline job run `python -m lake.install_extension` once (network allowed there),
  and `lake/read_exposures.py` does `load iceberg` only — so a machine that skipped
  setup fails loud, never silently fetches. The CI edit re-triggered security-reviewer
  (accepted). Provenance recorded here as the single non-uv.lock dependency.
- **DONE-command `make eval` needs an explicit `PROFILE=long_delay` (5th spec-vs-reality
  fix).** `make eval` is `accuracy.run --profile "$(PROFILE)"` with `PROFILE ?= tiny` and
  no "last profile" mechanism, so bare `make eval` scores tiny's truth file against the
  long_delay DB → a meaningless ~0.17, not the Gate's 0.973. The spec's DONE command +
  Gate note are corrected to `make eval PROFILE=long_delay` (proven: recall 0.9733). The
  same latent bug lives in CLAUDE.md's `make eval` prose (:108 "for the last profile") and
  its long_delay canonical demo (:176) — pre-existing since Phase 6, carved into a
  separate `fix/eval-demo-profile` PR (not folded into this lakehouse phase, per the
  "phase reveals an earlier-phase change → its own fix PR" rule); the durable fail-loud
  guard shipped as `fix/eval-profile-guard` (BACKLOG 43 — see the entry below).
- **Eval profile/DB-mismatch guard: a marker table, not a conversion_id-subset check
  (`fix/eval-profile-guard`, BACKLOG 43).** `make eval` scored `data/truth/<PROFILE>/`
  against the ClickHouse serving rows regardless of which profile populated them, so a
  bare `make eval` after seeding a non-tiny profile printed a meaningless number
  (~0.17) silently. Guard = a single-row `eval_meta` marker the populate path stamps
  with its profile, asserted `== --profile` in `accuracy/run.py` (loud
  `ProfileMismatchError` on mismatch or missing).
  - **Why not a conversion_id-subset check ("truth ⊆ DB"):** ids are numbered `c-NNNNNN`
    from 0, so a smaller profile's set is a subset of a larger one's (tiny ⊆ long_delay).
    A subset check therefore false-passes the exact original bug (tiny truth vs a
    long_delay DB) — a guard that greenlights its own failure. A marker is unambiguous
    across the shared id space.
  - **Why a marker table, not a field in the sink:** the live stages
    (`resolve.stage`/`streaming.dataflow`/`reconcile.reconcile`) do not take `--profile`
    — the engine reads from topics and never knows the profile name. Threading it in
    would touch the byte-identical path; a standalone `write_marker.py` step in the
    populate targets keeps the engine untouched.
  - **Why a versionless single-row RMT keyed on a constant, no timestamp:** the marker
    must be deterministic (off the golden-compared path, so gate-0 stays byte-identical)
    and replay-idempotent. Constant key `k=0` makes every write replace the one row; no
    timestamp avoids the §8 clickhouse-connect tz round-trip and keeps re-runs identical.
  - Stamped by every populate target that leaves a scoreable DB (`run`, `run-hot`,
    `lake-land`, `metrics-capture`); `reconcile-dagster` inherits `lake-land`'s stamp.
