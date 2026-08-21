# Phase 17 — Lake of record (PROPOSED)

Contract for the `phase-17-lake-of-record` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), finding 4 ("the lake is an add-on off the side of the engine") and
finding 5 ("DuckDB-over-Iceberg won't survive the 90d window at scale"). Depends on
Phase 16 merged (two topics, in-process resolve, no Bytewax).

**Status: PROPOSED — do not start until Phase 16 has merged and this is approved.**
No new dependencies expected (pyiceberg, pyarrow, duckdb, dagster already allowlisted).
If the bucket transform or snapshot expiry needs a pyiceberg feature not in the pinned
version, STOP and ask before bumping.

## Why

Phase 12 proved the *pattern* — the same reconcile output whether exposures come from
ClickHouse or from Iceberg via DuckDB — with a dual-write that is off by default
(`--lake-land`). In production a dual-write with no transactional boundary is a drift
generator, and the lake being optional means ClickHouse is still the system of record.
The Data-Platform answer is the other way round: **Iceberg is the record; ClickHouse is
a derived serving projection loaded from it.** Replay and backfill then come from the
lake, never from Kafka retention, and a 90-day reconciliation window becomes a
partition-pruned join rather than a scan.

## The central constraint

**Flip the arrow without moving a number.** Every accuracy pin (`tests/pins.py`) and
every serving-table row is byte-identical before and after — the only thing that
changes is *where ClickHouse's rows come from*. Iceberg metadata (snapshot ids, commit
times) and Dagster run ids stay carved out of the byte-identical guarantee exactly as
Phase 12 recorded; every asserted check reads row content back.

## DONE command

```
make test && make lint \
  && make down && make up && make seed PROFILE=long_delay && make run \
  && make eval PROFILE=long_delay && make test-int-lakehouse
```

- `make run` now lands to the lake always (no `--lake-land` flag) and loads ClickHouse
  from it; `make eval PROFILE=long_delay` reports the pinned 0.587 → 0.973.
- `make test-int-lakehouse` becomes the arrow-flipped parity proof: serving rows loaded
  from the lake == serving rows the engine would have written directly (the Phase-12
  direct-write path is kept as a test-only oracle, not a product path).

## Done-when

1. **Engine writes to the lake, not to ClickHouse.** `streaming/` lands deduped
   exposures to `raw.exposures` and hot-path attributed rows to
   `raw.attributed_conversions` (new Iceberg table, same columns as the ClickHouse
   table), both day-partitioned by `event_time` and **bucketed by `household_id`**
   (Iceberg `bucket(N, household_id)`, N fixed per profile tier and recorded in the
   table properties — laptop default 8). The `--lake-land` flag is removed; landing is
   the only write path.
2. **ClickHouse is loaded from the lake.** Dagster assets `clickhouse_exposures_landed`
   and `clickhouse_attributed_conversions` depend on the raw tables and load the day
   partition into ClickHouse (ReplacingMergeTree semantics unchanged: keyed
   `conversion_id`, version `processed_at`). Re-materializing a partition is idempotent.
   `make run` = engine → lake → load → reconcile; `make reconcile-dagster` is the same
   graph with day partitions selectable.
3. **Reconciliation is a bucket-aligned partitioned join.** For partition
   `(day, bucket)`, candidates (`attributed=false` rows in `raw.attributed_conversions`
   for that day and bucket) join ONLY `raw.exposures` partitions in `[day − 90d, day]`
   with the same bucket. Dagster asset becomes `MultiPartitionsDefinition(day × bucket)`.
   The SQL is engine-agnostic (DuckDB runs it locally; the same statement is the
   Spark/Trino target). This closes BACKLOG rows "reads ALL candidates per partition"
   and "global `min_event_time` prune".
4. **Lake hygiene is an asset, not a footnote.** A `lake_maintenance` Dagster job
   expires snapshots older than a configured age and rewrites small files per
   partition. `make down` still removes compose volumes only; a documented `make
   lake-reset` (explicit confirmation, like `down`) clears `data/lake/`. Closes the
   BACKLOG "`data/lake` accumulates unboundedly" row.
5. **Replay is from the lake.** `make replay-serving` (new) truncates-and-reloads the
   ClickHouse serving tables from the lake with no Kafka involvement, and `make eval`
   afterwards reproduces the pins. This is the backfill story for a petabyte tier:
   Kafka retention is hours; the lake is forever.
6. **Parity proof inverted.** `tests/integration/test_lakehouse.py` asserts
   lake-loaded serving rows == direct-write oracle rows (row content, sorted, 6dp), and
   that reconciling over an **accumulated** lake (≥ 3 appends) is byte-identical to a
   fresh one. Closes the BACKLOG "no test pins reconcile determinism over an
   accumulated lake" row.
7. **Docs flip with the code.** ARCHITECTURE §3.2 diagram shows lake → ClickHouse;
   §3.3 gains a "Lake (system of record)" component; SCALING.md's lakehouse paragraph
   becomes the baseline, with the object-store + REST catalog + Spark/Trino port as the
   tier note; CLAUDE.md commands/architecture/determinism-policy bullets updated;
   RESULTS.md unchanged except provenance lines.

## Pinned decisions (do not re-litigate)

- **Iceberg is the record; ClickHouse is derived.** Not the reverse, not a dual-write.
- **Bucket by `household_id`, partition by day.** Bucketing is what makes the 90d join
  shuffle-free at scale; it is set once (Kafka-partition-count logic, SCALING.md) and
  recorded as a table property.
- **DuckDB stays the laptop compute; the SQL is the contract.** No Spark/Trino is run
  here (no JVM — CLAUDE.md convention). The port is a note, not code.
- **Landing is always on.** The Phase-12 "off by default to keep `make run` byte-
  identical" carve-out is retired because row-content checks (not metadata) are the
  guarantee; DECISIONS Phase 12 entry gets a superseded-by pointer.
- **Serving-table DDL unchanged.** RMT key/version, sort keys, `eval_meta` marker,
  `agent_ro` grants: zero diff.

## Scope (files)

- `lake/` (new `raw.attributed_conversions` table, bucket transform, maintenance
  helpers), `streaming/sink.py` (lake sink replaces the ClickHouse sink on the engine
  path; the direct ClickHouse writer moves to `tests/` as the oracle),
  `orchestration/assets.py` + `definitions.py` (load assets, multi-partition
  reconcile, maintenance job), `reconcile/sources.py` + `reconcile.py` (bucket-aligned
  SQL), `Makefile`, `.github/workflows` (CI integration job runs the lake path),
  `tests/integration/test_lakehouse.py`, BACKLOG (close 3 rows), DECISIONS Phase 17,
  PHASES.md, CLAUDE.md, ARCHITECTURE §3.2/3.3/§5, SCALING.md.

## Review & stack risk

- **code-reviewer** (mandatory): determinism carve-out honoured (row content only),
  idempotent partition re-materialization, no truth-link read, bucket count recorded.
- **security-reviewer** (mandatory — CI workflow change): lake path is local
  `file://`; no new service exposure; `agent_ro` unchanged.
- **functionality-tester**: DONE command; accumulated-lake parity; `make
  replay-serving` reproduces pins from a cold ClickHouse.
- **coherence-auditor** at exit: every "dual-write" / "`--lake-land`" / "off by
  default" sentence is gone.
- Stack risk: pyiceberg bucket transforms and snapshot expiry in the pinned version —
  verify in the first hour; if unsupported, STOP and report before any workaround.

## Out of scope (deferred, recorded)

- Object store + REST catalog; Spark/Trino execution — SCALING.md tier note.
- Continuous follow / stream framework choice — still the open Phase 17+/18 question;
  this phase keeps the batch drain.
- Incremental rollups, part-count alerts, async inserts, query cost in dollars, schema
  compat BACKWARD — Phase 18 (cost & ops).
