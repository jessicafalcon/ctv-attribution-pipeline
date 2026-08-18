# Phase 2 — Resolve stage

Contract for the `phase-2-resolve-stage` branch. Source: `docs/PHASES.md`
→ Phase 2, `docs/ARCHITECTURE.md` §3.3 "Resolve stage".

## DONE command

```
uv run python -m resolve.replay --profile tiny --source fixtures && \
diff fixtures/tiny/expected/conversions_resolved.jsonl \
     data/out/tiny/conversions_resolved.jsonl && \
make test && make lint
```

Passes when: replaying the frozen tiny fixture through the resolver yields
byte-identical resolved records matching the committed golden expected file,
and tests + lint are green. The replay is service-free and reads only the
frozen `fixtures/tiny/` input, so the check is deterministic (no broker
ordering in the loop).

## What "resolve" does (ARCHITECTURE §3.3)

For each conversion row consumed from `conversions`:

- **Device hit**: `device_id` is in the graph → emit one record for that
  household, `resolution=device`, `ambiguous=false`, `candidate_count=1`.
- **Unique-IP fallback**: `device_id` unknown, `ip` owned by exactly one
  household → emit one record, `resolution=ip`, `ambiguous=false`,
  `candidate_count=1`.
- **Ambiguous-IP fan-out**: `device_id` unknown, `ip` owned by ≥2 households
  → emit one record **per candidate household**, `resolution=ip`,
  `ambiguous=true`, `candidate_count=N`. Candidates ordered by `household_id`
  for determinism.
- **Unresolvable**: `device_id` unknown and `ip` owned by zero households →
  emit nothing (counts against resolve rate). Not reachable in `tiny` (a
  conversion's IP is always its true household's), but handled.

The stage is a **stateless map**: no dedup here (dedup lives in the engine,
Phase 5). A duplicate conversion row resolves to the identical record(s), so
duplicates in → duplicates out. Republished to `conversions_resolved` keyed
by `household_id` so matchable events land in the same partition downstream.

## Scope

- `producer/models.py` — add `ResolvedConversion` (a `Conversion` plus
  `household_id`, `resolution` `device|ip`, `ambiguous`, `candidate_count`).
  Source of truth for the `conversions_resolved` schema.
- `producer/schemas.py` — register `conversions_resolved-value`; add a
  `set_compatibility(subject, level)` helper and set every dev subject to
  `NONE` before registering (BACKLOG Phase-2-start item: identical/changed
  re-register must not 409 the seed or stage).
- `resolve/index.py` — `GraphIndex`: `device_id → household_id` and
  `ip → {household_id}` lookups, built `from_households(...)` or loaded from
  the compacted `device_graph` topic (consume to end, last write per key
  wins).
- `resolve/resolver.py` — pure `resolve_one(conversion, index)` →
  `list[ResolvedConversion]`; `resolve_stream(...)` over an iterable. No I/O,
  unit-testable without services.
- `resolve/replay.py` — offline entrypoint for the DONE command: read
  `{conversions,device_graph}.jsonl` from `fixtures/<profile>/` (`--source
  fixtures`) or `data/out/<profile>/` (`--source out`), resolve, write
  `data/out/<profile>/conversions_resolved.jsonl` (canonical jsonl).
- `resolve/stage.py` — live consumer: graph from the compacted topic, consume
  `conversions`, resolve, validate-on-produce to `conversions_resolved`,
  register schema, emit `resolve_` Prometheus metrics. The real pipeline
  component; the offline DONE command does not require it.
- `resolve/metrics.py` — `resolve_` Prometheus metrics: resolve rate (
  resolved vs consumed), ambiguity rate, fan-out factor.
- `fixtures/tiny/expected/conversions_resolved.jsonl` — committed golden
  resolved output. Frozen ground truth after this phase.
- `Makefile` — `resolve` target (offline replay; `PROFILE`, `SOURCE`).
- Dependency added (on the CLAUDE.md allowlist): `prometheus-client`.
- Unit tests (no services): device hit, unique-IP fallback, ambiguous
  fan-out, unresolvable; index building; replay reproduces the committed
  expected fixture; raw-row and distinct-conversion counts pinned.
- `tests/integration/test_resolve_stage.py` — live consume→resolve→produce
  round-trip against `make up`; opt-in (`make test-int`, lands in CI Phase 3).

## Expected tiny counts (derived from the frozen fixture)

Per distinct `conversion_id` (duplicates collapsed; matches
`tests/test_fixtures.py`): 38 device, 12 unique-IP, 5 ambiguous with fan-out
shapes `{2:4, 3:1}` → 55 distinct conversions, 61 resolved records. Over raw
rows (duplicates included, what the stateless stage actually emits) the test
pins the exact numbers so a regen cannot silently drop a case.

## Determinism rules

- Fan-out candidate order is `sorted(household_id)`; canonical jsonl
  (sorted keys, compact separators) as Phase 1.
- The resolver is a pure function of (conversion, graph); no wall clock, no
  entropy. Same fixture → byte-identical resolved output.

## Out of scope

Dedup / TTL state (Phase 5), the attribution join and ClickHouse writes
(Phase 3), reconciliation (Phase 6), Prometheus scraping/Grafana wiring
(Phase 7), fault profiles (Phase 8). `medium` profile untouched.
