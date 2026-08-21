# Phase 12 — Lakehouse landing + orchestrated reconciliation (PROPOSED)

Contract for the `phase-12-lakehouse-landing` branch. Source: post-plan
extension — **not** in the original `docs/PHASES.md` plan (Phases 0–11). Covers
sketches 1 (Iceberg landing) + 2 (Dagster/DuckDB reconcile), which are intertwined:
the orchestrated job reads the lake the landing step writes.

**Status: PROPOSED — do not open a branch until approved.** This phase adds four
packages to the allowlist and **reverses** ARCHITECTURE §3.5 ("Parquet/Iceberg
landing … out of scope for v1"). Both need explicit sign-off first (see Approvals).

## Why

Adds the data-platform capabilities the base project deliberately left out, in one
phase: a lakehouse **storage** angle (Iceberg), a lakehouse **compute** angle (DuckDB
over the Iceberg table), and a modern **orchestrator** (Dagster). The reconciliation pass is already
a periodic, time-windowed batch job — the natural first software-defined asset. Today
it is a `make run` subprocess with no lineage, no partition, no independent backfill.

## The central constraint

**Iceberg table metadata is non-deterministic; the pipeline's output must stay
deterministic.** An Iceberg append stamps a fresh `snapshot_id` and commit timestamp
per run, so the *table metadata* is not byte-identical across runs even though the
*data rows* are. This is the same class of carve-out already made for the agent
(CLAUDE.md determinism policy): keep Iceberg **off** the byte-identical guarantee.
The determinism-asserted checks assert on **row content** read back from the lake,
never on snapshot ids or commit times, and the tiny golden gate keeps reading the
deterministic ClickHouse copy.

## DONE command

```
make down && make up && make seed PROFILE=long_delay && make lake-land && \
make reconcile-dagster PROFILE=long_delay && make eval && make test && make lint
```

- `make lake-land` appends the seeded exposures to the Iceberg `raw.exposures` table
  (Gate 1: table exists, row count == exposures produced, day-partitioned).
- `make reconcile-dagster` materializes the day-partitioned reconciliation asset,
  reading exposures from Iceberg via DuckDB (Gate 2).
- `make eval` reproduces long_delay recall **0.973**, unchanged from the
  ClickHouse-sourced reconcile — the swap of exposure source is output-invariant.
- `make test` + `make lint` green; gate-0 tiny golden byte-identical.

## Done-when

1. **Iceberg landing.** Exposures land to a local Iceberg table (`raw.exposures`,
   day-partitioned on `event_time`) during the seed/run, via a local SqlCatalog +
   file warehouse under `data/lake/` (gitignored). Row content round-trips.
2. **Reconciliation reads the lake.** The long-window matcher reads its candidate
   exposures from Iceberg **via DuckDB** (`iceberg_scan`, day-partition pruned),
   feeding the **unchanged** pure `attribute_household` leaf. Output
   (`attributed_conversions` FINAL, `path='reconciled'`) is **byte-identical** to the
   current ClickHouse-sourced reconcile — asserted on row content.
3. **Orchestration.** Reconciliation is a Dagster day-partitioned software-defined
   asset (`reconciled_conversions`, dep on `exposures_iceberg`); a headless make
   target materializes one partition, and a backfill over a date range is
   demonstrated (screenshot or CLI, recorded in RESULTS.md).
4. **Determinism carve-out documented.** DECISIONS entry: Iceberg metadata off the
   byte-identical guarantee; asserts on row content. Gate-0 tiny golden byte-identical.
5. **Records reconciled.** ARCHITECTURE §3.5 updated (Iceberg no longer out of scope;
   §5 mapping-table "next step" rows made real), DECISIONS, BACKLOG, CLAUDE.md status
   + commands + allowlist.

## Pinned decisions (do not re-litigate)

- **ClickHouse `exposures_landed` is KEPT as the serving copy — dual-write, not
  replaced.** Only the reconcile *source* swaps to the lake; the naive benchmark and
  serving reads stay on ClickHouse. This bounds the blast radius: the join, the RMT
  sink, and every existing gate are untouched.
- **DuckDB is the one lake compute engine here.** Spark and Trino are the 1:1
  mapping in SCALING.md, not built (DuckDB proves the compute-on-lake pattern on a
  laptop; Spark/Trino are the scale-tier port).
- **Dagster is orchestration only.** The pure `attribute_household` leaf and the
  idempotent RMT sink do not change — determinism of the *output* is preserved by
  construction (idempotency policy, CLAUDE.md).
- **Local, no cloud.** SqlCatalog on SQLite + `file://` warehouse under `data/lake/`.
  No object store, no REST catalog — those are a SCALING note.
- **Iceberg metadata is non-deterministic** (see central constraint) → carved out of
  the byte-identical guarantee, exactly like the agent.

## Scope (files)

- `lake/iceberg_catalog.py` (catalog + `EXPOSURE_SCHEMA` + day `PartitionSpec`),
  `lake/land_exposures.py` (append, idempotent-on-read). Dual-write from the deduped
  `exposures` list in `streaming/dataflow.py` `run_engine` (gated `--lake-land`) —
  the SAME list that feeds the ClickHouse sink, NOT the "insert_exposures call site"
  (a per-batch Bytewax sink). Parity by construction (DECISIONS Phase 12).
- `orchestration/assets.py` (`exposures_iceberg`, `reconciled_conversions`,
  daily partitions, job, schedule), `orchestration/definitions.py`.
- `reconcile/` — factor the exposure read behind a source interface so ClickHouse
  and DuckDB/Iceberg are swappable; the matcher logic is unchanged.
- `Makefile`: `lake-land`, `reconcile-dagster`, `dagster-ui` (optional dev server).
- Tests: lake round-trip (row content), reconcile-source equivalence
  (ClickHouse-sourced == Iceberg-sourced, row-identical), Dagster asset unit test.
- Records: this spec, ARCHITECTURE §3.5/§5, DECISIONS (Iceberg determinism carve-out;
  dual-write rationale), BACKLOG, CLAUDE.md.

## Approvals required before the branch opens

- **Dependency asks (ALL new — allowlist change):** `pyiceberg`, `pyarrow`,
  `dagster`, `dagster-webserver`, `duckdb`. CLAUDE.md requires asking before ANY new
  package; this is five at once — needs explicit sign-off. (Reality: pyiceberg 0.11.1
  has no `[sql]` extra — SqlCatalog deps are base — and writes need the `pyiceberg-core`
  extra, a sub-extra of the approved pyiceberg; DECISIONS Phase 12.)
- **ARCHITECTURE §3.5 reversal:** Iceberg landing is currently listed out of scope.
  Reversing a spec statement is a STOP-and-report event (CLAUDE.md workflow) —
  confirm the scope change before building.

## Review & stack risk

- **security-reviewer TRIGGERED** — run before commit. Charter (corrected — the UI
  is local `dagster dev`, NOT a compose service, so "compose service exposure" does
  not apply): (1) the five-package supply surface (`pyiceberg` + `pyiceberg-core`,
  `pyarrow`, `duckdb`, `dagster`, `dagster-webserver`) — the real new risk; (2)
  confirm `make dagster-ui` binds loopback only (127.0.0.1, never 0.0.0.0/published);
  (3) DAGSTER_HOME / instance-storage hygiene — the headless `make reconcile-dagster`
  uses an ephemeral instance (nothing persists), and `dagster dev`'s DAGSTER_HOME
  sits under gitignored `data/` and carries no secrets.
- **code-reviewer** determinism focus: truth-isolation still holds (lake carries no
  truth links), Iceberg carve-out correct, RMT sink untouched.
- **functionality-tester** after code-reviewer.
- **coherence-auditor** at phase exit: the §3.5 reversal is a deliberate drift to
  reconcile across ARCHITECTURE / DECISIONS / README.

## Out of scope (deferred, recorded)

- Spark / Trino compute (SCALING mapping only), cloud object store, REST catalog.
- Iceberg schema evolution beyond v1.
- Removing the ClickHouse `exposures_landed` serving copy (dual-write stays).
- Continuous Kafka follow — unchanged existing deferral.
