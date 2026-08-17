# Phase 1 — Producer and contracts

Contract for the `phase-1-producer-contracts` branch. Source: `docs/PHASES.md`
→ Phase 1.

## DONE command

```
make down && make up && \
make seed PROFILE=tiny && cp -r data/out/tiny /tmp/seed-run-1 && \
make seed PROFILE=tiny && diff -r /tmp/seed-run-1 data/out/tiny && \
diff -r fixtures/tiny data/out/tiny && \
make test && make lint
```

Passes when: two seed runs produce byte-identical output, that output matches
the committed golden fixtures, the schema registry holds a subject per topic
(seed fails loudly if registration fails), and tests + lint are green.

## Scope

- `producer/models.py` — pydantic models: `Exposure`, `Conversion`,
  `Device`, `Household`, `DeviceGraph`, `TruthLink`. Source of truth for
  schemas. Fields per ARCHITECTURE.md §3.3.
- `producer/schemas.py` — JSON Schemas generated from the models
  (`model_json_schema()`), registered in Redpanda's schema registry
  (subjects `exposures-value`, `conversions-value`, `device_graph-value`)
  via its HTTP API. Never hand-edited.
- `producer/graph.py` — seeded device-graph generator: N households, 1–few
  devices each, IPs with a configurable shared fraction (the only source of
  wrong-household matches).
- `producer/generate.py` — seeded event generator. Knobs, all in the
  profile: throughput (events/hour → event_time spacing), late injector
  (fraction + delay range), duplicate injector (fraction), shared-IP
  fraction, co-view multiplier per genre, caused-conversion rate. Emits a
  single deterministic sequence of (topic, key, payload) records.
- `producer/seed.py` — entrypoint for `make seed`: builds graph, registers
  schemas, creates topics (`exposures`, `conversions` — `device_graph`
  compacted), publishes graph + events to Redpanda, validates every payload
  against its model on produce, writes truth links to
  `data/truth/<profile>/`, mirrors all emitted records to
  `data/out/<profile>/` as jsonl for determinism checks.
- `producer/profiles/tiny.json` — ≈10 households, ≈200 events, all knobs
  exercised. Fixed `sim_start`; no wall-clock anywhere.
- `fixtures/tiny/` — committed golden copy of `data/out/tiny/` (exposures,
  conversions, device_graph, truth_links). Read-only after this phase.
- `Makefile` — `seed` target (`PROFILE`, `PRODUCER_SEED` env, default 42).
- Unit tests (no services): graph generation + determinism, each knob,
  schema generation, fixture regeneration matches committed fixtures.
- Dependencies added (both on the CLAUDE.md allowlist): pydantic,
  confluent-kafka.

## Determinism rules

- One `random.Random(seed)` drives everything; IDs are deterministic
  counters, timestamps derive from profile `sim_start`.
- Canonical serialization: JSON with sorted keys, compact separators,
  ISO-8601 UTC timestamps. Same seed + profile → byte-identical output.

## Out of scope

`conversions_resolved` topic and resolve stage (Phase 2), ClickHouse DDL
(Phase 3), fault profiles beyond the standard knobs (Phase 8), Prometheus
metrics from the producer (used from Phase 2 on; producer_ metrics land
with the observability phase).
