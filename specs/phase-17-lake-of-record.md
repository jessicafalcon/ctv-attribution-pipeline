# Phase 17 — Lake of record (PROPOSED)

Contract for the `phase-17-lake-of-record` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), finding 4 ("the lake is an add-on off the side of the engine") and
finding 5 ("DuckDB-over-Iceberg won't survive the 90d window at scale"). Depends on
Phase 16 merged (two topics, in-process resolve, no Bytewax).

**Status: ACTIVE (2026-08-21).** Phase 16 merged (#28); the Phase-16 coherence audit
found three gaps (F1–F3, BACKLOG "Phase 17 spec needs a Phase-16 follow-up edit") and
the developer's decisions D1–D12 below amend this spec. The amendment is the first
commit on the branch; no code before it.
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
  && make down && make lake-reset CONFIRM=yes && make up && make seed PROFILE=tiny \
  && make run-hot && make eval && make test-int \
  && make test-int-lakehouse && make test-int-long-delay
```

- `make lake-reset CONFIRM=yes` (tiny's lake) after `make down`: a clean stack is a
  clean lake (D9 + review gate). `make down` does not touch `data/lake/`, and
  `run-hot` loads the lake's CURRENT rows — so over a lake that already holds a
  `make run`'s reconciled rows the hot-only pins would shift. Every clean-state
  chain in the repo (this DONE command, the `test-int-*` targets, CI, the README /
  CLAUDE.md demos) carries the reset; `tests/test_clean_state_chains.py` pins it.

- tiny through the lake is the gate-0 proof: `make run-hot` is now engine → lake →
  Dagster headless load, and `make eval` + `make test-int` reproduce the tiny golden
  rows and `TINY_HOT` unchanged.
- `make test-int-lakehouse` (its own clean long_delay stack) is the arrow-flipped parity
  proof: serving rows loaded from the lake == serving rows the direct-write oracle (now
  under `tests/`) would have written; plus the accumulated-lake check (Done-when 6).
- `make test-int-long-delay` (its own clean long_delay stack) is the reconcile-through-
  lake proof: `make run` = engine → lake → load → reconcile → lake → load, and
  `LONG_DELAY_HOT` → `LONG_DELAY_POST` (recall 0.587 → 0.973) holds unchanged.

## Done-when

1. **Engine writes to the lake, not to ClickHouse.** `streaming/` lands deduped
   exposures to `raw.exposures` and hot-path attributed rows to
   `raw.attributed_conversions` (new Iceberg table — the ClickHouse table's 19
   columns in the same order, incl. `reason` and the new `candidate_households`; D3),
   both day-partitioned by `event_time` and **bucketed by `household_id`**
   (Iceberg `bucket(N, household_id)`, N fixed per profile tier and recorded in the
   table properties — laptop default 8). The `--lake-land` flag is removed; landing is
   the only write path.
2. **ClickHouse is loaded from the lake.** Dagster assets `clickhouse_exposures_landed`
   and `clickhouse_attributed_conversions` depend on the raw tables and load the day
   partition into ClickHouse (ReplacingMergeTree semantics unchanged: keyed
   `conversion_id`, version `processed_at`). Re-materializing a partition is idempotent.
   The load is driven by the set of `event_time` days the landing TOUCHED, never by
   wall-clock (D6). `make run` = engine → lake → load → reconcile → lake → load;
   `make run-hot`, CI and `metrics-capture` = the same minus the reconcile leg (D5);
   `make reconcile-dagster` is the same graph with day partitions selectable.
3. **Reconciliation is a bucket-aligned partitioned join, in two channels (D2).**
   Candidates are the current (`argMax(processed_at)`, D4) `attributed=false`,
   `path=hot` rows of `raw.attributed_conversions` for the day.
   - **state-miss** (`candidate_count == 1`): the row's `household_id` is certain, so
     it joins bucket-locally — `raw.exposures` partitions in `[day − 90d, day]` with
     the same bucket — no explode.
   - **ambiguous_ip** (`candidate_count > 1`): each deferred row is EXPLODED into one
     row per entry of its `candidate_households` array BEFORE bucketing (the existing
     `expand_candidates`, now reading the array instead of the device graph), each
     exploded row joins bucket-locally against `raw.exposures` in its candidate's
     bucket, and the results are REDUCED by `conversion_id` across buckets with
     `pick_household` — the one implementation, unchanged. This is why the placeholder
     `household_id` does not matter for recovery: the array is the truth, the
     placeholder is only the RMT key.
   The Dagster asset stays **day-partitioned with the bucket loop inside it**
   (`reconcile.recover_day`: one bucket-local lake read per bucket, then the
   cross-bucket reduce in the same process). *Amended at the review gate — the
   D2 draft said `MultiPartitionsDefinition(day × bucket)`; built and recorded as:
   the bucket is the unit of the READ, not of orchestration; it becomes a
   partition dimension when a bucket no longer fits one worker. At this scale a
   day × bucket asset would be 8× the Dagster run overhead for no measured gain
   (the per-bucket loop has none over one read — it exists to prove the unit), and
   the reduce would need an IO manager to carry per-bucket scores between runs.*
   The SQL per (day, bucket) is engine-agnostic
   (DuckDB runs it locally; the same statement is the Spark/Trino target). This closes
   BACKLOG rows "reads ALL candidates per partition" and "global `min_event_time`
   prune". **Gate: one reconcile pass == the Phase-16 output byte-for-byte** (every
   `tests/pins.py` POST pin, long_delay and shared_ip_spike included).
4. **Lake hygiene is an asset, not a footnote.** A `lake_maintenance` Dagster job
   expires snapshots older than a configured age and rewrites small files per
   partition. `make down` still removes compose volumes only; a documented `make
   lake-reset` (explicit confirmation, like `down`) clears `data/lake/<profile>`.
   *Amended at the review gate — the draft said "clears `data/lake/`" and "closes
   the BACKLOG '`data/lake` accumulates unboundedly' row". Built and measured:
   the reset is per profile (one lake per PROFILE), and snapshot expiry on
   pyiceberg 0.11.1 is METADATA-ONLY — there is no `remove_orphan_files`, so
   compaction + expiry bound the live file count but on-disk Parquet grows (24 →
   32 in a scratch lake; 211 → 252 on long_delay). The BACKLOG row stays OPEN,
   re-qualified: disk reclaim needs `remove_orphan_files`; only `lake-reset`
   reclaims today; pinned so a pyiceberg bump fails loud (ARCHITECTURE §8).*
5. **Replay is from the lake.** `make replay-serving` (new) truncates-and-reloads the
   ClickHouse serving tables from the lake with no Kafka involvement — genuinely
   broker-free, since reconciliation no longer reads the device graph (D1) — stamps
   the `eval_meta` marker (D8), and `make eval` afterwards reproduces the pins. This is the backfill story for a petabyte tier:
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
- **Serving-table DDL unchanged except ONE additive column.** RMT key/version, sort
  keys, `eval_meta` marker, `agent_ro` grants: zero diff. The single addition is
  `candidate_households Array(String)` (D1), by idempotent `add column if not exists`,
  the Phase-16 `reason` pattern.
- **Producer OUTPUT zero-diff.** `AttributedConversion` is the engine's table model
  that lives in `producer/models.py` (Phase-16 ruling); topics, truth links and
  profiles do not change.
- **One `pick_household` implementation.** Not duplicated into SQL; the cross-bucket
  reduce calls it.
- **No magnitude pins.** Runtime/size numbers are reported, never asserted.

## Scope (files)

- `producer/models.py` (`candidate_households` on `AttributedConversion`),
  `clickhouse/` DDL (additive migration), `streaming/attribute.py` or
  `dataflow.py` (the engine writes the full candidate set at deferral time),
  `lake/` (new `raw.attributed_conversions` table, bucket transform, maintenance
  helpers), `streaming/sink.py` (lake sink replaces the ClickHouse sink on the engine
  path; the direct ClickHouse writer moves to `tests/` as the oracle),
  `orchestration/assets.py` + `definitions.py` (load assets, multi-partition
  reconcile, maintenance job), `reconcile.py` (bucket-aligned; `reconcile/sources.py` was deleted at the review gate — one reader, the lake
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
- Stack risk (D11): pyiceberg 0.11.1 bucket transforms on WRITE via pyiceberg-core,
  `expire_snapshots`, and DuckDB `iceberg_scan` reading bucketed layouts with partition
  pruning — verify in the first hour; if unsupported, STOP and report before any
  workaround. Findings go under ARCHITECTURE §8.

## Out of scope (deferred, recorded)

- Object store + REST catalog; Spark/Trino execution — SCALING.md tier note.
- Continuous follow / stream framework choice — still the open Phase 17+/18 question;
  this phase keeps the batch drain.
- Incremental rollups, part-count alerts, async inserts, query cost in dollars, schema
  compat BACKWARD — Phase 18 (cost & ops).
- `engine_join_state_current` last-household-wins fix — Phase 18 (BACKLOG row).
- `make agent-eval` re-run (API tokens) — BACKLOG 49, unchanged.
- Landing `device_graph` as a lake table — NOT needed: D1 makes the candidate set part
  of the attributed row, so neither reconcile nor replay reads the graph (the BACKLOG
  broker-dependency row's suggested fix is superseded by D1).

## Amendments (2026-08-21) — post-Phase-16 decisions D1–D12

Written before the branch opened, against the Phase-16 coherence audit's F1–F3.
Each is pinned; the implementation order follows them.

- **D1 — `candidate_households` column (closes F2 + the BACKLOG broker-dependency
  row).** `AttributedConversion.candidate_households: list[str]` (engine output model
  in `producer/models.py` — same ruling as the Phase-16 `reason` column; producer
  OUTPUT zero-diff), ClickHouse `candidate_households Array(String)` by additive
  migration (empty when not ambiguous), the sink, and the Iceberg schema. The engine
  writes the FULL candidate set (the graph's sorted owners) at deferral time.
  Consequences: reconcile no longer needs the device graph or the broker;
  `expand_candidates` reads the array; `load_graph_index` leaves `reconcile/` and
  `orchestration/`; `make replay-serving` is genuinely Kafka-free. The Phase-16
  placeholder `household_id` (min candidate) STAYS — a nullable ClickHouse key column
  is worse than a documented placeholder. The array is the truth, the placeholder is
  the key.
- **D2 — Ambiguous path under bucketing (closes F1).** Reconcile's ambiguous channel
  explodes each deferred row into one row per `candidate_households` entry BEFORE
  bucketing, joins bucket-locally against `raw.exposures`, then reduces by
  `conversion_id` ACROSS buckets with `pick_household` (one implementation, unchanged).
  The state-miss channel joins bucket-locally with no explode. Spec + DECISIONS
  wording: *the architecture's claim is no fan-out on the HOT path; batch fan-out with
  the full 90-day picture is cheap and correct.* Gate: one reconcile pass == Phase-16
  output byte-for-byte.
- **D3 — `raw.attributed_conversions` column contract (closes F3).** Columns = the
  ClickHouse table incl. `reason` and `candidate_households` — 19 columns, same order.
  Pinned in a test (model fields == sink columns == DDL == Iceberg schema).
- **D4 — Lake tables are append-only logs; current state lives in ClickHouse (RMT).**
  Reconciled rows are APPENDED to `raw.attributed_conversions` with
  `path=reconciled` and a later `processed_at`; any lake read that needs "current row
  per `conversion_id`" uses `argMax(processed_at)` in SQL — never assumes one row per
  key. The loader is idempotent because the RMT dedups on load.
- **D5 — Every path goes through the lake.** `make run`, `make run-hot`, CI and
  `metrics-capture` = engine → lake → Dagster headless load → (reconcile → lake →
  load). The direct ClickHouse writer in `streaming/sink.py` moves to `tests/` as the
  parity oracle; `make lake-land` and `--lake-land` are removed. Gate-0 golden and
  every pin in `tests/pins.py` are byte-identical/unchanged — this phase moves rows,
  it does not change them. CI integration job runtime grows (Dagster + DuckDB):
  accepted, noted in the PR.
- **D6 — Load is driven by days TOUCHED, not wall-clock.** `land()` returns the set
  of `event_time` days it wrote; the Dagster load asset materializes exactly those
  partitions (late rows land in old days and must reload those days). Partition keys
  come from data min/max (the existing `_day_keys` static set), never from today's
  date.
- **D7 — Bucket count.** `bucket(8, household_id)` on BOTH raw tables, identical N,
  recorded as a table property and asserted equal in a test. N is a SCALING.md lever
  (tier table), set once per deployment — never changed on a populated lake.
- **D8 — `eval_meta` stamping.** The populate targets still stamp the profile marker
  (PR #25 / #29 guard); `make replay-serving` stamps too. If a new `test-int-*`
  target is added, the Makefile guard's target discovery (`tests/test_makefile.py`)
  is extended.
- **D9 — `make lake-reset`.** The second sanctioned destructive path beside `make
  down`; requires explicit confirmation; documented in CLAUDE.md Commands. `make
  down` still does not touch `data/lake/`.
- **D10 — Maintenance asset.** Expire snapshots older than a configured age + rewrite
  small files per partition, as a Dagster job, run by `make lake-maintain`. Not part
  of `make run`.
- **D11 — First-hour stack check (STOP and report on any failure, no workaround).**
  pyiceberg 0.11.1 (pinned) supports bucket partition transforms on WRITE via
  pyiceberg-core, `expire_snapshots`, and DuckDB `iceberg_scan` reads bucketed layouts
  with partition pruning. Findings recorded under ARCHITECTURE §8.
- **D12 — Out of scope, unchanged.** Spark/Trino, object store/REST catalog,
  continuous follow, `engine_join_state_current` fix (Phase 18), agent-eval re-run
  (tokens).

**Implementation order** (one commit per green state, `phase-17:` prefix): D1
model/DDL/sink/schema + column-contract test → D7/D3 lake tables with bucketing →
D5 engine→lake + Dagster load assets + direct writer to `tests/` → D2 bucketed
reconcile (ambiguous explode + state-miss) with the byte-identical gate → D6
touched-days loader → replay-serving + lake-reset + maintenance → docs
(ARCHITECTURE §3.2/§3.3/§5 + determinism bullet, CLAUDE.md commands/determinism/
status, SCALING baseline + tier table, RESULTS provenance lines, DECISIONS Phase 17
incl. "supersedes the Phase-12 off-by-default carve-out", PHASES.md row, BACKLOG:
close the F1–F3 row, the broker row, the `data/lake` growth row, the
accumulated-lake row).
