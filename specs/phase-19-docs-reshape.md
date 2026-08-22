# Phase 19 — Docs reshape (RECONCILED)

Contract for the `phase-19-docs-reshape` branch. Source: post-plan extension — **not**
in the original `docs/PHASES.md` plan. Origin: the Phase-15 architecture review
(2026-08-20), finding 8: the process scaffolding outweighs the pipeline in the reader's
first five minutes — a 314-line README whose first screen is a problem statement, a
1,263-line DECISIONS.md organised by phase, and several guards that exist only to police
docs. Depends on Phases 16–17 merged — NOT 18: reordered 2026-08-22 to run BEFORE
18a/18b (DECISIONS "Process" entry — the consolidation removes the drift tax every
later phase would otherwise pay again). Docs-only — no pipeline code.

**Status: RECONCILED 2026-08-22 against main @ fd0e28f.** No new dependencies.

## Why

The audience is a Data-Platform engineering manager with five minutes. Today they meet
the phase history before the constraint equation. The strongest facts in the repo —
the measured 571 B/exposure and its 8.6 TB consequence, the two-path design it forces,
the accuracy/restatement table, the cost-lever tables — should be the first screen.
The audit trail stays (it is real evidence of how the work was done) but moves below
the fold. Phase 15's "elevate, never invent" constraint applies verbatim.

## The central constraint

**Move, merge, delete — never invent.** Every number in the reshaped docs traces to
`tests/pins.py`, a `make`-regenerated block (`scale-curve` and `cost-levers` only —
the two generators that exist on main), or a recorded DECISIONS entry. No new measurements are
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
   (tiny / medium / long_delay, from `pins.py`); the cost-lever table (the Phase-13 rows
   only: projection WIN, FINAL-avoidance DOCUMENTED NEGATIVE, PREWHERE WIN — direction
   + measured delta, from the `cost-levers` generated block); the 30/30 agent eval line; one "run the headline demo" command.
   *Evidence: row 1 of the Evidence table.*
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
   table describe the post-17 system (lake of record) as it runs on main today — 18a/18b
   edit §3.4 when they land; every "Phase N added…" parenthetical is reduced to
   a DECISIONS link. §8 Gotchas stays as-is (it is the raw incident record the runbook
   elevates). §7 "Build order" points to README `## History`.
4. **One docs guard, not three.** `make check-docs` subsumes `check_runbook.py` and the
   Phase-11 README link check; `tests/test_docs_accuracy_pins.py` stays (it guards
   numbers, not prose). BACKLOG row 37 (substring-blind trace check) closes ONLY when a
   test pins a partial-rename failure (`_canonicalize` vs `_canonicalize_tables`)
   against the exact-token check. BACKLOG row 47 (prose accuracy citations) closes ONLY
   if the exact-token check also covers every accuracy number in README/RESULTS prose
   against `tests/pins.py`; otherwise it is re-deferred with trigger "next change to
   `tests/pins.py`". Neither row is silently dropped.
5. **CLAUDE.md shrinks to what the next session needs.** Architecture block, repo map,
   commands, policies, conventions, workflow rules stay. The status table moves to
   README `## History`; "Current status" becomes: current phase, last merged PR, open
   BACKLOG count, and a pointer. No rule changes.
6. **Honesty boundary stays on the first screen.** The README keeps a two-sentence
   "what this does NOT show" line (batch drain, single node, laptop-scale numbers) above
   the fold — the review praised the honesty boundaries; burying them would undo that.

## Evidence (REQUIRED)

Every Done-when item names the test or command output that proves it.

| Done-when | Proof (test file / `make` target / command output) |
|---|---|
| 1 | `awk '/^## /{exit} {n++} END{print n}' README.md` (line count up to the first `##`) ≤ 60, pasted; `make check-docs` output (link/anchor + exact-token); `tests/test_docs_accuracy_pins.py` green |
| 2 | `make check-docs` output (link/anchor + exact-token); `tests/test_docs_accuracy_pins.py` green |
| 3 | `make check-docs` output (link/anchor + exact-token); `tests/test_docs_accuracy_pins.py` green |
| 4 | Three hand-run negative tests — a deliberately broken anchor, a stale generated block, a renamed guard — each showing `make check-docs` FAIL, output pasted in the PR body; plus the row-37 partial-rename test and the row-47 prose-coverage check (or its re-deferral) named in Done-when 4 |
| 5 | `make check-docs` output (link/anchor + exact-token); `tests/test_docs_accuracy_pins.py` green |
| 6 | `make check-docs` output (link/anchor + exact-token); `tests/test_docs_accuracy_pins.py` green |

The same table, filled with the actual run's output, is item 2 of the "Before
reporting DONE" checklist (CLAUDE.md Workflow rules).

## Pinned decisions (do not re-litigate)

- **Elevate, never invent** (Phase 15 precedent).
- **Nothing is deleted from DECISIONS; it is re-organised and annotated.**
- **The phase table moves, it does not shrink.** Every row's result, gate and spec link
  survive the move to README `## History`.
- **Numbers come from pins and generated blocks only.** A docs number with no source
  is a bug, not a style choice.
- **Docs-only, no code** except `scripts/check_docs.py` and its Makefile target.
- **`scripts/check_docs.py` is `docs/check_runbook.py` moved with `git mv` and
  extended** — one file, history preserved, never a second script.

## Scope (files)

- `README.md`, `DECISIONS.md`, `docs/ARCHITECTURE.md`, `CLAUDE.md` (status section,
  commands: `check-runbook` → `check-docs`), `docs/RESULTS.md` (first-screen tables are
  sourced from here; RESULTS keeps the long-form), `docs/PHASES.md` (rows 16, 17, 18a, 18b, 19 — already present; verify they match
  README `## History`),
  `scripts/check_docs.py` + Makefile, `.github/workflows` (run `make check-docs` in the
  lint job), `tests/test_check_docs.py` (the BACKLOG-37 partial-rename pin Done-when 4
  requires — the one test file the "docs-only" pinned decision admits), BACKLOG (close
  or re-defer the two docs-guard rows), DECISIONS Phase 19.

## Record updates (REQUIRED)

The explicit list of record files this phase must change; checked off in the report
(checklist item 6), diffed by the coherence auditor against the actual diff.

- [ ] `README.md` — first screen, `## History` (phase table moved from CLAUDE.md)
- [ ] `DECISIONS.md` — binding/appendix split + Phase-19 entry
- [ ] `docs/ARCHITECTURE.md` — §3 (end-state, post-17) + §7 (points to README `## History`)
- [ ] `CLAUDE.md` — status section; `check-runbook` → `check-docs` in Commands and
      Project tooling
- [ ] `docs/PHASES.md` — rows 16, 17, 18a, 18b, 19 verified against README `## History`
- [ ] `docs/RESULTS.md` — long-form source of the first-screen tables
- [ ] `BACKLOG.md` — rows 37 + 47: close or re-defer (conditions in Done-when 4), never
      silent
- [ ] `Makefile` — `check-runbook` → `check-docs`
- [ ] `.github/workflows` — lint job runs `make check-docs`
- [ ] every `.claude/agents/*.md` and `.claude/commands/*.md` that names
      `check-runbook` or `check_runbook.py`

## Threat model (REQUIRED when the phase adds a Makefile target that takes a variable, deletes anything, or takes user input)

None — `make check-docs` takes no variable, deletes nothing, takes no input.

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
- 18a and 18b each append one row to the README cost-lever table (recorded in their
  own Record-updates lists).

## Pre-branch reconciliation required (2026-08-22)

This spec was written assuming Phase 18 had landed. The 2026-08-22 reorder runs it
BEFORE 18a/18b. Resolved by this branch's commit 1 (the spec-reconciliation
amendment, CLAUDE.md Workflow rules) against main @ fd0e28f:

- Central constraint: generated blocks are `scale-curve` and `cost-levers` only
  (`rollup-bench` / `cost-report` struck — 18a / 18b targets that do not exist).
- Done-when 1: the first-screen cost-lever table holds the Phase-13 rows only
  ("incremental rollup, async inserts" struck).
- Done-when 3: ARCHITECTURE §3.2–3.4 describe the post-17 system as it runs on main
  today; 18a/18b edit §3.4 when they land.
- `docs/PHASES.md` scope: rows 16, 17, 18a, 18b, 19 (already present; verified
  against README `## History`).

The amendment also added the three `specs/TEMPLATE.md` sections this spec predated
(Evidence, Record updates, Threat model), the `git mv` pinned decision for
`scripts/check_docs.py`, the BACKLOG 37/47 close conditions in Done-when 4, and the
18a/18b cost-lever-row note under Out of scope.
