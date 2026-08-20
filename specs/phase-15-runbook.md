# Phase 15 — Runbook and incident log (PROPOSED)

Contract for the `phase-15-runbook` branch. Source: JD-alignment follow-on — **not**
in the original `docs/PHASES.md` plan (Phases 0–11). Covers sketches 5 (the two real
incident stories) + 6 (the batch-drain operational boundary), intertwined as one
operational-narrative deliverable. Answers the posting's "alerts and runbooks you
actually wrote" bullet with a real artifact, not talking points.

**Status: PROPOSED — do not open a branch until approved.** No new dependencies.
Docs-only — no pipeline code changes.

## Why

Two of the strongest pieces of evidence in this repo are buried in ARCHITECTURE §8
"Gotchas": the `FINAL read_rows`-counts-unmerged-parts non-determinism (caught because
it made the *benchmark* lie in CI) and the clickhouse-connect timezone round-trip that
quadrupled the report snapshots. In post-incident form they read as production war
stories — exactly the on-call credibility the posting screens for. The batch-drain
scope call (windowing semantics proven on a bounded drain, continuous follow not
operated) belongs in the same runbook as a documented known-limitation, so it's owned
out loud rather than discovered.

## The central constraint

**Elevate existing facts; invent nothing.** Every incident, number, and fix in the
runbook must trace to an existing ARCHITECTURE §8 gotcha, a DECISIONS entry, or a
RESULTS number. This is the truth-isolation / no-fabrication discipline applied to
docs: if a runbook claim cannot be traced to a real recorded event, **STOP and report**
(CLAUDE.md workflow) — do not write a plausible-sounding incident.

## DONE command

```
make test && make lint
```

- Docs-only; the code suite is unchanged (gate-0 tiny golden trivially byte-identical).
- A link/trace check (mirroring the Phase-11 README link check) verifies every runbook
  cross-reference resolves: each incident links its ARCHITECTURE §8 gotcha and the
  guard that now prevents regression; each named alert links `observability/rules/`.

## Done-when

1. **`docs/RUNBOOK.md` exists**, with two incidents in post-incident format —
   **symptom → detection → root cause → fix → generalization → would-catch-it-next-time**:
   - **The benchmark that lied in CI** — `FINAL` physically reads un-merged
     version-parts, so `read_rows` counted transient part-bloat; the rollup headline
     reversed (0.8×) in CI's run-state. Fix: `OPTIMIZE ... FINAL` to merged steady
     state before measuring + a magnitude-free direction assert. Generalization: never
     trust a `FINAL` scan's `read_rows` as a structural number.
   - **The timezone round-trip that quadrupled the snapshots** — clickhouse-connect
     renders `DateTime` in the client's local tz; a Python round-trip stamped
     `reported_at` 6 h apart across processes. Fix: compute `reported_at` server-side
     in the INSERT; read the version as tz-free epoch-millis. Generalization:
     cross-process timestamp comparison must be tz-free at the wire, not the display.
2. **Batch-drain operational boundary** documented as a known-limitation runbook
   entry: what is proven (arrival-ordered, watermark-gated, evicting windowing on a
   bounded drain) vs what is **not operated** (continuous unbounded follow, spill-to-
   disk state, TTL'd dedup), with the SCALING.md Flink-port pointer.
3. **Would-catch-it-next-time honesty.** For each incident, name the alert that would
   catch a recurrence — and where none of the four existing alerts covers it (the
   `FINAL read_rows` drift is not alerted), say so rather than imply coverage.
4. **Every claim traces.** The trace check passes; no invented numbers.

## Pinned decisions (do not re-litigate)

- **Elevate, never invent** (see central constraint). The two incidents already
  happened and are recorded in ARCHITECTURE §8; this phase reformats them, adds no new
  facts, and STOPs if a claim can't be traced.
- **Runbook, not a résumé bullet.** Written for the next on-call engineer (detection +
  guard + generalization), which is also why it reads well in an interview.
- **Docs-only, no code.** No `streaming/`, `queries/`, or `clickhouse/` change — the
  two guards already exist (bench direction-assert; server-side `reported_at`); the
  runbook references them, it does not re-implement them.
- **Honest coverage gaps stay visible** — an un-alerted failure mode is documented as
  un-alerted, not smoothed over.

## Scope (files)

- `docs/RUNBOOK.md` (the two incidents + the batch-drain boundary).
- A small trace/link check (script or test) so the DONE command can assert
  cross-references resolve, mirroring the Phase-11 README link check.
- Records: this spec, a `docs/README.md` / README pointer to the runbook, CLAUDE.md
  status. (No DECISIONS reversal — the runbook restates existing decisions.)

## Review & stack risk

- **code-reviewer** at the finish line (mandatory): docs accuracy — every claim traces
  to an existing gotcha / DECISIONS / RESULTS fact (truth-isolation applied to docs).
- **functionality-tester** after code-reviewer: runs the trace check, confirms every
  cross-reference resolves.
- **security-reviewer NOT triggered** — no CI / `.env` / compose / ClickHouse-user /
  agent-context change.
- **coherence-auditor** at phase exit: the runbook must not drift from ARCHITECTURE §8
  / DECISIONS / RESULTS (it restates them; catch any divergence).

## Out of scope (deferred, recorded)

- New incident scenarios beyond the two recorded ones (inventing them violates the
  central constraint).
- A live push path so an alert actually fires an incident (still the deferred
  Alertmanager-push BACKLOG row).
- Automating post-incident capture — this phase writes two by hand.
