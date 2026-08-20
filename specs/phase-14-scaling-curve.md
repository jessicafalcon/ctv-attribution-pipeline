# Phase 14 — Measured scaling curve (PROPOSED)

Contract for the `phase-14-scaling-curve` branch. Source: JD-alignment follow-on —
**not** in the original `docs/PHASES.md` plan (Phases 0–11). Covers sketch 4 (turn
SCALING.md's order-of-magnitude sizing into one measured occupancy curve).

**Status: PROPOSED — do not open a branch until approved.** No new dependencies.
Offline (no compose stack), like the oracle suites.

## Why

SCALING.md asserts ~200 B/exposure and extrapolates to "~3 TB at 25k/sec × 7 d." For
a posting whose premise is "scaling problems are real, not theoretical," a fully-sized
scaling story is the soft spot. You don't need 500k/sec — you need **one measured
point** where the in-memory hot-window state starts to hurt, so the extrapolation is
anchored to a measured per-exposure cost, not a guess. The Phase-7
`engine_join_state_current` gauge ("rises AND falls") is used here for its real purpose.

## The central constraint

**`tracemalloc` peak is allocation-nondeterministic** (Python allocator + GC timing
vary run to run), so it cannot be the asserted number without breaking the determinism
policy. The reported `bytes_per_exposure` comes from a **structural** measure —
window-state entry count × measured per-entry size — which is deterministic on re-run
under a fixed seed and single thread. `tracemalloc` peak is reported as a **cross-check
only**, never asserted. Same discipline as the Phase-7 `FINAL read_rows` fix: don't
build a claim on a number that drifts with runtime timing.

## DONE command

```
make scale-curve && make test && make lint
```

- `make scale-curve` generates the tier profiles, drains the engine over each,
  and prints the curve: `exposures_in_window`, `peak_join_state_structural_bytes`,
  `bytes_per_exposure`, `engine_join_state_current` occupancy per tier — then writes
  it into `docs/SCALING.md`, replacing the asserted ~200 B constant.
- `make test` + `make lint` green; gate-0 tiny golden byte-identical.
- Offline: no `make up` — the engine drains generated profiles in-process.

## Done-when

1. **Curve produced.** `make scale-curve` runs the engine drain over tiers (e.g.
   1k / 10k / 100k exposures in the 7-day window, same event-time span, event *count*
   dialed up) and reports peak structural window-state bytes, `bytes_per_exposure`,
   and `engine_join_state_current` occupancy per tier.
2. **SCALING.md anchored to measurement.** The "~200 B/exposure" line is replaced by
   the measured constant + the curve table; the "3 TB at 25k/sec × 7 d" extrapolation
   is re-derived from the *measured* `bytes_per_exposure`, shown as extrapolation.
3. **Deterministic.** Same seed → identical curve (structural measure; `tracemalloc`
   is a labeled cross-check only). A re-run asserts the structural numbers equal.
4. Gate-0 tiny golden byte-identical (scale profiles are additive; the probe is a
   read-only observation of engine state).

## Pinned decisions (do not re-litigate)

- **Structural measure is the asserted number; `tracemalloc` is a cross-check** (see
  central constraint).
- **The curve measures event COUNT in-window (state occupancy), not msgs/sec
  throughput.** SCALING's first wall is the *memory* budget (`exposure_rate ×
  window`), which occupancy models directly; wall-clock throughput needs continuous
  follow, which is deferred (no phase owns it). Say so — don't imply a throughput
  benchmark.
- **Reuse `engine_join_state_current`** (Phase 7) rather than adding a metric.
- **Offline, no compose.** The engine drain is in-process over generated profiles,
  same idiom as the oracle suites — keeps the phase cheap and re-run-deterministic.

## Scope (files)

- `producer/profiles/scale_curve.py` (tiered event counts, fixed span/seed),
  `streaming/scale_probe.py` (drive the drain, structural + `tracemalloc` measures),
  `Makefile` `scale-curve`.
- `docs/SCALING.md` (measured curve table replaces the asserted constant; the TB
  extrapolation re-derived and labeled as such).
- Tests: structural-measure determinism (same seed → same `bytes_per_exposure`),
  occupancy monotonic in tier, probe unit test.
- Records: this spec, DECISIONS (structural-vs-tracemalloc choice), BACKLOG,
  CLAUDE.md status + commands.

## Review & stack risk

- **code-reviewer** at the finish line (mandatory): determinism of the reported number
  (structural, not tracemalloc), profile additive (existing profiles untouched).
- **functionality-tester** after code-reviewer.
- **security-reviewer NOT triggered** — no CI / `.env` / compose / ClickHouse-user /
  agent-context change.
- **coherence-auditor** at phase exit.

## Out of scope (deferred, recorded)

- A live throughput (msgs/sec) benchmark — needs continuous follow (deferred).
- Spill-to-disk / RocksDB state — the SCALING tier change, not built here.
- Any change to the engine's state representation (the probe observes, never alters).
