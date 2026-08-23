---
description: Scoped review round N — print round N−1's diff range and the spec's invariant list, then run code-reviewer + functionality-tester over that range with "missed in round N−1" labelling. Read-only, report-only, then STOP.
---

Run review round **$ARGUMENTS** (an integer N ≥ 1) on the current phase branch.
Read-only and report-only, like every agent this command invokes: no edits, no
fixes, no commits, no push. Findings are reported verbatim and the session STOPS
(CLAUDE.md Git workflow, "STOP-on-findings").

## 1. Establish the range

- Round 1 reviews the whole branch: `RANGE=main...HEAD`.
- Round N > 1 reviews **round N−1's fixes only**: the commits since the previous
  round's gate. Find the boundary from the commit log — the last commit whose
  message names the previous round (e.g. `round-N−1`, `review round N−1`,
  `gate`) or, failing that, the last commit BEFORE the first fix commit that
  followed the previous verdicts; if it is ambiguous, print the candidates and
  ask the developer for the boundary SHA instead of guessing. Then
  `RANGE=<boundary>..HEAD`.

Print, before anything else:

```
Review round N — range: <RANGE>
git log --oneline <RANGE>
git diff --stat <RANGE>
```

## 2. Print the invariant list

Locate the active spec (`specs/phase-<slug>.md` for this branch) and print its
**Invariants** section verbatim — the table AND any fix amendments appended to it
(CLAUDE.md Workflow rules, "Fix amendments"). If the spec has no Invariants
section, print that as the first finding (BLOCKER: a spec without invariants
cannot be reviewed against them) and continue with the range alone.

## 3. Review cap check (before spending a round)

If N ≥ 3, read the previous two rounds' verdicts (the session transcript or
the phase report). If BOTH reported correctness findings only in the previous
round's fixes, print **"Review cap reached (CLAUDE.md Workflow rules)"** and
STOP: the next step is not another round — it is writing the invariant and
re-implementing against it once. Do not run the agents.

## 4. Run the agents, scoped

Run **code-reviewer** then **functionality-tester** (that order), each with a
prompt that contains:

- the range: "review `git diff <RANGE>`; read changed files in full";
- the invariant list from step 2, verbatim;
- the labelling rule: "a finding on code UNCHANGED since round N−1's range is
  still reported, but labelled **`missed in round N−1`** so the review's own
  drift is visible; a finding inside the range carries no label";
- for the functionality-tester: "the Mutation step and the Evidence-row check
  are mandatory for every write path and guard inside the range".

Both agents are report-only by contract (no Write/Edit); do not grant more.

## 5. Report and STOP

Relay both verdicts verbatim, findings ordered BLOCKER / correctness /
should-fix / suggestion, each tagged in-range or `missed in round N−1`. Close
with the one line the developer decides on: **fix (wording/test-only)**, **fix
amendment (design change → spec paragraph first, stop for approval)**, or
**review cap**. Then STOP — no fixes, no push.

This is an explicit, on-request review. Do not treat its presence as a cue to
run it automatically — it runs only when invoked.
