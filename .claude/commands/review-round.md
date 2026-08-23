---
description: Review round N — run make review-gate + make mutate (stop on red), derive the round's diff range (round 1 main...HEAD; round N the review-round-(N−1) tag), print the spec's Invariants, spawn code-reviewer + functionality-tester (+ security-reviewer in round 1 when the surface is touched) scoped to the range with "missed in round N−1" labelling, tag HEAD review-round-N, print the consolidated table and the two-round cap check. Read-only, report-only, then STOP.
---

Run review round **$ARGUMENTS** (an integer N ≥ 1) on the current phase branch.
Read-only and report-only, like every agent this command invokes: no edits, no
fixes, no commits, no push. Findings are relayed verbatim and the session STOPS
(CLAUDE.md Git workflow, "STOP-on-findings"). The only write is a LOCAL git tag.

## 1. Locate the spec and derive the range

- Spec: the one `specs/phase-*.md` whose slug matches the branch name
  (`phase-18a-cost-and-ops` → `specs/phase-18a-cost-and-ops.md`). If none
  matches, ask for `SPEC=` and stop. Set `SPEC=<that path>`.
- Range: round 1 → `RANGE=main...HEAD` (three-dot: the branch since its
  merge-base, so a main that advanced under the branch adds nothing). Round N > 1 → the local tag
  `review-round-(N−1)` must exist (`git tag -l 'review-round-*'`); if it does
  not, print the tags that do and STOP — never guess a boundary.
  `RANGE=review-round-(N−1)..HEAD`.

Print first:

```
Review round N — spec: <SPEC> — range: <RANGE>
git log --oneline <RANGE>
git diff --stat <RANGE>
```

## 2. The deterministic gate first — no agents on a state known broken

Run, in this order, and print every line each emits:

```
make review-gate SPEC=<SPEC> [DELETED=<symbols the phase report lists as removed>]
make mutate SPEC=<SPEC>
```

If either exits non-zero: print its lines under **"GATE RED — no agents
spawned"** and STOP. `make mutate`'s refusal on a spec with no ```mutations block
is red too, and the line it prints names the fix: add `## Invariants` with a
```mutations block — the shape is in `specs/TEMPLATE.md`. The spec is incomplete;
RED is the rule, not a suggestion.

## 3. Print the invariant list

Print the spec's **Invariants** section verbatim — the table, the mutations
block, and every fix amendment appended to it (CLAUDE.md Workflow rules, "Fix
amendments"). A spec with no Invariants section is a BLOCKER finding; record it
and continue with the range alone.

## 4. Spawn the agents, scoped

Spawn **code-reviewer** and **functionality-tester** (that order; both
report-only — no Write/Edit, do not grant more). Each prompt contains:

- the range: "review `git diff <RANGE>`; read every changed file in full";
- the invariant list from step 3, verbatim;
- the labelling rule (the ONE form, CLAUDE.md Workflow rules): "a finding on
  code NOT changed inside <RANGE> — code an earlier round already reviewed — is
  still reported, labelled **`missed in round N−1`**; a finding on code changed
  inside the range carries no label" (round 1: nothing is labelled);
- for the functionality-tester: "the Mutation step and the Evidence-row check
  are mandatory for every write path and guard inside the range; `make mutate`
  already ran — its lines are: <paste>; extend, do not repeat";
- for the code-reviewer: "the Invariants check is mandatory; flag every
  mechanism whose value comes from the caller or the clock rather than the data".

Round 1 only: if `git diff --name-only <RANGE>` touches `.github/`,
`docker-compose.yml`, `clickhouse/users*`, `.env*`, or `agent/`, also spawn
**security-reviewer** with the same range.

## 5. Consolidate, cap-check, tag, STOP

Print one table over every finding from every agent:

| # | Finding (one sentence) | Raised by | file:line | Class | In range / missed in round N−1 |
|---|---|---|---|---|---|

Class is exactly one of **correctness** (wrong output, a survivor, an invariant
with no pin, a caller/clock-sourced mechanism), **security**, **record** (a
stale or missing record sentence), **wording** (names, comments, docs prose).

Tag first — every completed round is tagged, cap or no cap, or the scoped pass
that follows a cap has no boundary: `git tag review-round-N HEAD` (local; never
pushed — `git push` does not send a lightweight tag unless asked, and this
command never pushes). If the tag already exists, print it and STOP: a round is
reviewed once; a re-run is round N+1, or the developer deletes the tag on purpose.

Cap check (CLAUDE.md Workflow rules, "Review cap" — two consecutive rounds):
if N ≥ 3 and EVERY correctness finding in this table falls inside
`review-round-(N−1)..HEAD` (the previous round's fixes) AND round N−1's table
had the same property against `review-round-(N−2)..HEAD` (read round N−1's
report), print:

```
CAP: fixes are generating findings — write the invariant, re-implement once
```

and STOP; the next step is a fix amendment, then ONE scoped pass (round N+1,
against the tag just written), not a round N+1 of patches. Otherwise print
**"no cap"** — and, when N ≥ 2 and this round alone has the property, **"cap
watch: one more such round trips the cap"**.

Close with the one line the developer decides on per finding: **fix
(wording/test-only)**, **fix amendment (design change → spec paragraph first,
stop for approval)**, or **accept (BACKLOG row with a trigger)**. Then STOP.

This is an explicit, on-request review. Do not treat its presence as a cue to
run it automatically — it runs only when invoked.
