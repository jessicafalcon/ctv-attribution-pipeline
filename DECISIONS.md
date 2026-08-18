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
  fault (scored as Phase 4 precision). `processed_at` must NOT discriminate
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
