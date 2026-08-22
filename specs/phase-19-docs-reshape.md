# Phase 19 — Docs reshape (PROPOSED)

Contract for the `phase-19-docs-reshape` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), finding 8: the process scaffolding outweighs the pipeline in the reader's
first five minutes — a 314-line README whose first screen is a problem statement, a
1,263-line DECISIONS.md organised by phase, and several guards that exist only to police
docs. Depends on Phases 16–17 merged — NOT 18: reordered 2026-08-22 to run BEFORE
18a/18b (DECISIONS "Process" entry — the consolidation removes the drift tax every
later phase would otherwise pay again). Docs-only — no pipeline code.

**Status: PROPOSED — do not start until Phases 16–17 have merged and this is approved.**
No new dependencies.

## Why

The audience is a Data-Platform engineering manager with five minutes. Today they meet
the phase history before the constraint equation. The strongest facts in the repo —
the measured 571 B/exposure and its 8.6 TB consequence, the two-path design it forces,
the accuracy/restatement table, the cost-lever tables — should be the first screen.
The audit trail stays (it is real evidence of how the work was done) but moves below
the fold. Phase 15's "elevate, never invent" constraint applies verbatim.

## The central constraint

**Move, merge, delete — never invent.** Every number in the reshaped docs traces to
`tests/pins.py`, a `make`-regenerated block (`scale-curve`, `cost-levers`,
`rollup-bench`, `cost-report`), or a recorded DECISIONS entry. No new measurements are
taken in this phase; if a claim needs one, it is cut. The docs-vs-pins guard
(`tests/test_docs_accuracy_pins.py`) and the regenerated-block markers are the proof.

## DONE command

```
make test && make lint && make check-docs
```

- `make check-docs` (new, replaces `make check-runbook` + the README link check): one
  offline script that (a) resolves every intra-repo link and anchor across `README.md`
  and `docs/`, (b) asserts every `<!-- generated -->` block matches its generator's
  current output marker, (c) asserts every named guard/alert/target in the docs exists
  in source by an exact-token match (fixing the substring-blind BACKLOG row on
  `check_runbook.py`). Not a pytest file (same reason as `check-runbook`: avoid the
  run-tests-hook full-suite re-trigger).

## Done-when

1. **README first screen (≤ 60 lines before the first `##`).** In order: one-sentence
   what-it-is; the constraint equation with the measured constant and the 8.6 TB
   consequence; the two-path answer in two sentences; the accuracy/restatement table
   (tiny / medium / long_delay, from `pins.py`); the cost-lever table (projection,
   PREWHERE, incremental rollup, async inserts — direction + measured delta, from the
   generated blocks); the 30/30 agent eval line; one "run the headline demo" command.
   Then `## Architecture` (diagram as today), `## How it's proven`, `## Scaling`, `##
   Run it`, `## Repo map`, `## History` (the phase table, moved here from CLAUDE.md
   "Current status", which keeps a one-line pointer + the current phase only), `## Next
   steps`.
2. **DECISIONS.md split into binding + appendix.** A new top section "Decisions still
   in force" — ≤ 20 entries, one paragraph each, grouped by component (events & topics,
   resolve, engine, serving, reconciliation, lake, agent, ops), each linking to its
   original phase entry. Every entry that a later phase reversed or superseded (e.g. the
   Phase-12 `--lake-land` off-by-default carve-out after Phase 17; the fan-out/reduce
   after Phase 16) is marked **superseded by** in place — entries are never deleted.
   The per-phase log moves under `## Appendix — by phase`, unchanged in content and
   re-ordered chronologically (today 15 precedes 12).
3. **ARCHITECTURE.md is the end-state spec.** §3.2 diagram, §3.3 components and §3.4
   table describe the post-18 system; every "Phase N added…" parenthetical is reduced to
   a DECISIONS link. §8 Gotchas stays as-is (it is the raw incident record the runbook
   elevates). §7 "Build order" points to README `## History`.
4. **One docs guard, not three.** `make check-docs` subsumes `check_runbook.py` and the
   Phase-11 README link check; `tests/test_docs_accuracy_pins.py` stays (it guards
   numbers, not prose). The BACKLOG "prose-citation guard" row is closed by the exact-
   token check in (c) or explicitly re-deferred with a reason — never silently dropped.
5. **CLAUDE.md shrinks to what the next session needs.** Architecture block, repo map,
   commands, policies, conventions, workflow rules stay. The status table moves to
   README `## History`; "Current status" becomes: current phase, last merged PR, open
   BACKLOG count, and a pointer. No rule changes.
6. **Honesty boundary stays on the first screen.** The README keeps a two-sentence
   "what this does NOT show" line (batch drain, single node, laptop-scale numbers) above
   the fold — the review praised the honesty boundaries; burying them would undo that.

## Pinned decisions (do not re-litigate)

- **Elevate, never invent** (Phase 15 precedent).
- **Nothing is deleted from DECISIONS; it is re-organised and annotated.**
- **The phase table moves, it does not shrink.** Every row's result, gate and spec link
  survive the move to README `## History`.
- **Numbers come from pins and generated blocks only.** A docs number with no source
  is a bug, not a style choice.
- **Docs-only, no code** except `scripts/check_docs.py` (or the existing
  `check_runbook.py` renamed and extended) and its Makefile target.

## Scope (files)

- `README.md`, `DECISIONS.md`, `docs/ARCHITECTURE.md`, `CLAUDE.md` (status section,
  commands: `check-runbook` → `check-docs`), `docs/RESULTS.md` (first-screen tables are
  sourced from here; RESULTS keeps the long-form), `docs/PHASES.md` (rows for 16–19),
  `scripts/check_docs.py` + Makefile, `.github/workflows` (run `make check-docs` in the
  lint job), BACKLOG (close or re-defer the two docs-guard rows), DECISIONS Phase 19.

## Review & stack risk

- **code-reviewer** (mandatory): every number traces; every superseded decision is
  annotated, none deleted; CLAUDE.md rules unchanged.
- **security-reviewer NOT triggered** unless the CI lint job change is judged in scope
  (it adds one offline command; no secrets, no service).
- **functionality-tester**: `make check-docs` catches a deliberately broken anchor, a
  stale generated block, and a renamed guard (three negative tests run by hand and
  recorded in the PR body).
- **coherence-auditor** at exit — this phase IS a coherence pass; the auditor's
  findings are the acceptance test.

## Out of scope (deferred, recorded)

- Any new measurement, benchmark, or profile.
- A rendered site / diagrams-as-images — Markdown in-repo only.
- Re-running `make agent-eval` for fresher verdict numbers (API tokens; the Phase-10
  numbers stand, labelled with their date).

## Pre-branch reconciliation required (2026-08-22)

This spec was written assuming Phase 18 had landed. The 2026-08-22 reorder runs it
BEFORE 18a/18b, so the branch's commit 1 (CLAUDE.md Workflow rules: the
spec-reconciliation amendment, stop for approval) must resolve the clauses that
still reference unbuilt Phase-18 deliverables — listed, not resolved, here:

- Central constraint: the `make`-regenerated blocks named include `rollup-bench` and
  `cost-report` (18a / 18b targets that do not exist yet).
- Done-when 1: the first-screen cost-lever table lists "incremental rollup, async
  inserts" (18a / 18b levers) beside the Phase-13 projection / PREWHERE rows.
- Done-when 3: ARCHITECTURE §3.2–3.4 "describe the post-18 system".
- `docs/PHASES.md` scope: "rows for 16–19" now means 16, 17, 18a, 18b, 19 in the
  reordered sequence.

The amendment also adds the three `specs/TEMPLATE.md` sections this spec predates:
Evidence, Record updates, Threat model (`make check-docs` takes no variable and
deletes nothing — expected to be "None", stated).
