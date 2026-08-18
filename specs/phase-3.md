# Phase 3 — Attribution engine, minimal

Contract for the `phase-3-attribution-engine` branch. Source: `docs/PHASES.md`
→ Phase 3, `docs/ARCHITECTURE.md` §3.3 "Attribution engine", DECISIONS.md
(Phase 2, ambiguous shared-IP reduction).

## DONE command

```
uv run python -m streaming.replay --profile tiny --source fixtures && \
diff fixtures/tiny/expected/attributed.jsonl \
     data/out/tiny/attributed.jsonl && \
make test && make lint
```

Passes when: replaying the frozen tiny fixtures (exposures +
`expected/conversions_resolved.jsonl`) through the pure attribution core yields
byte-identical attributed records matching the committed golden expected file,
and tests + lint are green. The replay is service-free and deterministic (no
broker, no ClickHouse, no wall clock), so the golden diff has zero stream- or
insert-ordering in it.

**Real-pipeline verification (part of Done-when, PHASES.md):** the live Bytewax
engine + ClickHouse path is proven by an opt-in integration test
(`make test-int` against `make up`) that compares ClickHouse
`attributed_conversions` **FINAL** state to the same expected fixture. The
CI integration job (added this phase) runs it.

## What the engine does (ARCHITECTURE §3.3, PHASES Phase 3)

Bytewax dataflow joining `exposures` and `conversions_resolved` on
`household_id`. In-order events, last-touch, no windowing tricks yet (dedup,
watermarks, eviction land Phase 5).

Two stages, in order:

1. **Household-keyed join → per-candidate attribution.** For a resolved
   conversion in household H, find exposures in H whose `event_time` is at or
   before the conversion's `event_time` and within the attribution window
   (default 7 days before it). Credit the **last-touch** (latest `event_time`;
   ties broken by `exposure_id`); record the rest as **assists**. If no
   candidate exposure, emit an **unattributed** record (`attributed=false`,
   `exposure_id=null`, `assists=[]`) so reconciliation (Phase 6) can retry.

2. **`conversion_id`-keyed ambiguous reduction (mandatory this phase).** An
   ambiguous shared-IP conversion arrives as one candidate row per household
   (resolve fan-out); a resend-duplicate arrives as an identical row. This
   reduction collapses every row sharing a `conversion_id` to exactly one
   winner: the candidate whose last-touch exposure is most recent
   (`event_time`), ties broken `exposure_id` then `household_id`; an attributed
   candidate always beats an unattributed one; if all candidates are
   unattributed, keep the lowest `household_id`. Result: exactly one attributed
   record per distinct `conversion_id`. The tiny fixture's 5 ambiguous fan-outs
   make this non-deferrable (PHASES Phase 3).

For the 5 ambiguous fan-outs the most-recent-exposure pick is sometimes a
different household than truth **by design** — that is the shared-IP fault,
scored as precision in Phase 4, not a Phase 3 failure. The engine is compared
against `expected/attributed.jsonl`, never directly against truth.

## `processed_at` is event-derived, not wall-clock (determinism)

`processed_at` is the ReplacingMergeTree version. Deriving it from the wall
clock would violate the determinism policy ("could this step give a different
answer on a re-run?") and break both the golden fixture and the
replay-from-offset-0 convergence the idempotency contract requires. So the
hot path sets `processed_at = conversion.ingest_time`: deterministic, already
in the data, and strictly before any later reconciliation version.

- Duplicate resend (same bytes, same `conversion_id`, original `ingest_time`)
  → identical attributed row → ReplacingMergeTree collapses to one. This is why
  the fixture and the FINAL-state comparison hold 55 rows, not 68.
- **Forward note (Phase 6):** a reconciled row must carry a `processed_at`
  strictly greater than the hot row it supersedes for the same `conversion_id`.
  Reconciliation stamps its own later version; the invariant is recorded here.

## Scope

- `producer/models.py` — add `AttributedConversion` (a `ResolvedConversion`
  plus `exposure_id: str | None`, `assists: list[str]`, `attributed: bool`,
  `path: Literal["hot", "reconciled"]`, `processed_at: datetime`). Source of
  truth for the `attributed_conversions` schema.
- `streaming/attribute.py` — pure attribution core. Exposes two leaf decision
  functions: `attribute_household(exposures_in_household,
  resolved_in_household, window)` → per-candidate attributed rows (stage 1), and
  `reduce_conversion(candidate_rows)` → one `AttributedConversion` (stage 2).
  `attribute(exposures, resolved, window)` orchestrates them over in-memory
  groups → `list[AttributedConversion]`, one per distinct `conversion_id`,
  sorted by `conversion_id`. `dataflow.py` calls the SAME two leaves via Bytewax
  `key_by` (household_id → `attribute_household`; re-key conversion_id →
  `reduce_conversion`), so live and replay cannot diverge (DECISIONS Phase 3).
  No I/O, no clock, no entropy; unit-testable without services.
- `streaming/replay.py` — offline entrypoint for the DONE command: read
  `exposures.jsonl` + `expected/conversions_resolved.jsonl` from
  `fixtures/<profile>/` (`--source fixtures`) or `data/out/<profile>/`
  (`--source out`), attribute, write canonical
  `data/out/<profile>/attributed.jsonl`.
- `streaming/dataflow.py` — the live Bytewax engine: Kafka sources for
  `exposures` and `conversions_resolved` drained to end (batch, like the
  resolve stage), household-keyed join, `conversion_id`-keyed reduction, sink
  attributed rows to ClickHouse `attributed_conversions` and land raw exposures
  in `exposures_landed`. Shares the one pure core with the replay so they
  cannot diverge. `engine_` Prometheus metrics.
- `streaming/sink.py` — ClickHouse writer (clickhouse-connect over HTTP 8123):
  synchronous, idempotent inserts of `AttributedConversion` and raw `Exposure`
  rows. Connection params from env (`CLICKHOUSE_*`, mirroring `KAFKA_BROKER` /
  `SCHEMA_REGISTRY_URL`), never hardcoded.
- `streaming/metrics.py` — `engine_` Prometheus metrics: conversions processed,
  attributed vs unattributed, assists recorded, ambiguous reductions collapsed.
- `clickhouse/ddl.sql` — `attributed_conversions` (ReplacingMergeTree, order by
  `conversion_id`, version `processed_at`), `exposures_landed` (ReplacingMergeTree,
  order by `(campaign_id, event_time, exposure_id)` — RMT for replay-idempotency,
  DECISIONS Phase 3). Synchronous inserts this phase (async → Phase 7).
  Idempotent `create … if not exists`.
- `clickhouse/apply.py` — apply the DDL against the running ClickHouse (used by
  `make run` / integration setup). Simplest standard applier; no migration
  framework yet.
- `fixtures/tiny/expected/attributed.jsonl` — committed golden attributed
  output, one row per distinct `conversion_id` (55). Frozen after this phase.
- `Makefile` — `run` target (resolve batch → engine batch; reconciliation added
  Phase 6); `test-int` target (pytest `tests/integration`).
- Dependencies added (both on the CLAUDE.md allowlist, first imported here):
  `bytewax`, `clickhouse-connect`.
- `.github/workflows/*` — add the integration job:
  `make up && make seed PROFILE=tiny && make run && make test-int` (Phase 0 /
  Phase 2 deferred it to "Phase 3").
- Unit tests (no services): last-touch pick, assists recorded, window boundary,
  unattributed, ambiguous reduction winner + tiebreaks, duplicate collapse,
  replay reproduces the committed expected fixture; distinct-count pinned at 55.
- `tests/integration/test_engine.py` — live seed→resolve→engine→ClickHouse
  round-trip against `make up`. Compares `attributed_conversions` FINAL to the
  expected fixture, **ordered by `conversion_id`** (emission order over Bytewax
  keys is not stable; RMT + an explicit order-by make the comparison
  well-defined). Asserts `exposures_landed` idempotency: FINAL count ==
  distinct exposure count **after two engine runs** (the RMT-not-MergeTree
  guard, DECISIONS Phase 3). Opt-in (`make test-int`).

## Expected tiny counts

55 distinct conversions → 55 attributed rows in `attributed.jsonl` and in
ClickHouse FINAL state. Attributed vs unattributed split is whatever the frozen
fixture yields (pinned by the test, not asserted here). The 5 ambiguous
conversions collapse from 11 candidate rows to 5 winners; resend-duplicates
collapse under the same `conversion_id` key.

## Determinism rules

- Attribution is a pure function of (exposures, resolved, window): no wall
  clock, no entropy. `processed_at = ingest_time` (event-derived). Same fixture
  → byte-identical `attributed.jsonl`.
- Output sorted by `conversion_id`; canonical jsonl (sorted keys, compact
  separators) as prior phases.
- Replaying either topic from offset 0 converges to the same ClickHouse FINAL
  state (idempotency contract): same version, same key, RMT keeps one row.

## Review & stack risk

- **security-reviewer runs this phase** (mandatory): it touches ClickHouse
  connection/credential handling (`sink.py`, `apply.py`) and adds a CI
  integration job. ClickHouse creds come from env, never hardcoded or logged;
  no secrets into CI.
- **Bytewax stack risk.** Draining two Kafka topics to end inside one batch
  dataflow needs care with Bytewax's Kafka source API. Verify against the
  official Bytewax docs before working around anything; log any surprise under
  ARCHITECTURE §8 (stack-surprise rule).

## Out of scope

Dedup / TTL state, watermarks + allowed lateness, hot-window eviction (all
Phase 5); reconciliation, rollups, snapshots, restatements (Phase 6); the
naive-vs-optimized benchmark and Grafana/Alertmanager wiring (Phase 7); fault
profiles (Phase 8). `medium` profile untouched. Co-view multiplier is a
read-time factor (Phase 4+), not applied in the engine.
