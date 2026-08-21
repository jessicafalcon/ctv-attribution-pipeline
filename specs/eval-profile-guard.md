# Spec — eval profile/DB-mismatch guard (BACKLOG 43)

Post-plan fix, own branch `fix/eval-profile-guard` off main. Closes BACKLOG 43
and folds in its Makefile:128-129 comment-twin rider.

## Problem

`make eval` (`accuracy.run --profile "$(PROFILE)"`, `PROFILE ?= tiny`) reads the
truth side file from `data/truth/<profile>/` but reads credited/exposure rows
from ClickHouse **regardless of which profile was seeded**. So a bare `make eval`
after seeding a non-tiny profile scores tiny truth against a long_delay DB
(~0.17) — a meaningless number, silently, not a loud failure. Surfaced by Phase
12's DONE run.

## Design (developer-approved — BACKLOG 43)

A **marker table** the populate path writes and eval asserts against.

**Rejected: conversion_id-subset check.** Conversion ids are `c-NNNNNN` numbered
from 0, so a smaller profile's id set is a subset of a larger one's
(tiny ⊆ long_delay). A "truth ⊆ DB" guard therefore false-passes the exact
original bug (tiny truth vs long_delay DB) — a guard that greenlights its own
failure. Verified: `producer/generate.py` numbers ids sequentially.

**Marker, not sink-embedded.** The live stages (`resolve.stage`,
`streaming.dataflow`, `reconcile.reconcile`) do not take `--profile` — the engine
reads from topics and never knows the profile name. Threading profile into the
engine would touch the byte-identical path. Instead a **standalone marker-writer**
(`clickhouse/write_marker.py`, `--profile`) is invoked by the populate make
targets, where `PROFILE` is already in scope. The engine path stays untouched
and byte-identical.

## Deliverables

1. **`eval_meta` table** (new, in `clickhouse/ddl.sql`) — off the golden-compared
   path (`attributed_conversions` / `exposures_landed` FINAL), so gate-0 stays
   byte-identical:
   ```sql
   create table if not exists eval_meta
   (
       k       UInt8,        -- constant 0: single-row table
       profile String
   )
   engine = ReplacingMergeTree
   order by k;
   ```
   Single row keyed on the constant `k=0` → a re-run's insert replaces it →
   replay-idempotent. **No timestamp** — the marker is fully deterministic (only
   a profile string), so it needs no §8 tz-safe epoch-millis handling.

2. **`clickhouse/write_marker.py`** — `--profile P`: `insert into eval_meta values
   (0, P)`. Idempotent (single-row replace). Invoked as one extra step in **every
   populate make target that leaves a scoreable DB** — `run`, `run-hot`,
   `lake-land`, AND `metrics-capture` (it runs the same resolve→engine→reconcile
   sink stages with `--metrics-out`, so an un-stamped `metrics-capture` would
   leave a stale marker and re-open the silent false-pass) — with `--profile
   $(PROFILE)`. The DB then self-describes which profile it holds.
   `reconcile-dagster` needs no stamp: it runs after `lake-land` (same PROFILE
   already stamped) and its reconcile output is byte-identical.

3. **`accuracy/run.py` assertion** — after `connect()`, read the marker
   (`select profile from eval_meta final`) and assert it equals `--profile`:
   - marker missing (no populate run yet) → loud `sys.exit`: run `make run
     PROFILE=<p>` first.
   - marker != `--profile` → loud `sys.exit`: DB holds `<marker>`, asked to score
     `<profile>` — reseed/rerun or pass the matching `PROFILE=`.
   The pure `accuracy/score.py` is untouched (offline scoring tests keep working
   with no DB).

4. **Makefile:128-129 rider** — the `eval` target comment still reads "for the
   last seeded profile" (the code-side twin of the CLAUDE.md prose fixed in PR
   #23). Correct it to match: "for the given `PROFILE` (default `tiny`)".

## Determinism / idempotency

- `eval_meta` is off the golden path → gate-0 tiny golden byte-identical
  (`make test-int`).
- Engine path unchanged (marker written by a separate process step, never by the
  sink) → `make run` output byte-identical.
- No timestamp → marker write deterministic; replay from offset 0 → same marker.

## Tests

- **Offline unit** (`tests/test_eval_guard.py`): a pure assertion helper
  `assert_profile_marker(marker: str | None, profile: str) -> None` (extracted so
  it needs no DB) — raises on `None` (missing) and on mismatch, returns on match.
  Pins all three cases. Keeps the guard logic offline-testable.
- **Live integration** (`tests/integration/test_eval_guard_live.py`, under
  `make test-int`): 4 tests pinning the DB-glue path the unit test bypasses —
  match passes, long_delay-vs-tiny mismatch exits loud, the no-marker None-path
  fails loud (points eval at a db without `eval_meta` — no destructive truncate),
  and `write_marker` idempotency (re-stamp → `count() FINAL == 1`). CI-safe on the
  shared tiny stack.
- **Live proof** (acceptance, needs a stack): after `make up && make seed
  PROFILE=tiny && make run`, `make eval PROFILE=tiny` passes and `make eval
  PROFILE=long_delay` exits loudly (marker=tiny). Demonstrates the guard fires on
  the exact original bug.

## DONE command

```
make test && make lint
```

plus the live acceptance proof above (own clean stack). `make test` includes the
new `tests/test_eval_guard.py`; gate-0 tiny golden byte-identical via
`make test-int` on a clean tiny stack.

## Out of scope

- Storing the seed (int) in the marker — profile string is sufficient for the
  guard; add later only if a consumer needs it.
- Auto-detecting the profile ("last profile") — explicit `PROFILE=` is more
  deterministic and matches every other target (BACKLOG 43).
