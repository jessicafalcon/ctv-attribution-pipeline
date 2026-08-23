---
description: Review round N on a phase branch (tooling/fix/docs branches get one line — run the agents directly). Step 1 derives the range (round 1 main...HEAD; round N the review-round-(N−1) tag) and refuses an existing review-round-N tag via scripts/round_tag.py; step 2 runs make review-gate + make mutate (red → no agents); step 3 prints the spec's Invariants; step 4 spawns code-reviewer + functionality-tester (+ security-reviewer in round 1 when the surface is touched) scoped to the range with "missed in round N−1" labelling; step 5 prints the consolidated table, writes the round tag with `round_tag.py write` (six anchored key=value fields, local, never pushed), then runs `round_tag.py cap` — the two-round rule as code — printing CAP / cap watch / no cap. Read-only, report-only, then STOP.
---

Run review round **$ARGUMENTS** (an integer N ≥ 1) on the current phase branch.
Read-only and report-only, like every agent this command invokes: no edits, no
fixes, no commits, no push. Findings are relayed verbatim and the session STOPS
(CLAUDE.md Git workflow, "STOP-on-findings"). The working tree is never written;
the repo's only writes are a LOCAL annotated tag per round and the throwaway
worktrees `make mutate` registers and removes under `.git/worktrees/`.

## 1. Locate the spec and derive the range

- Scope: phase branches only. If the branch is not `phase-*` (a `tooling/*`,
  `fix/*`, `docs/*` branch) print ONE line and STOP:
  `no phase spec for <branch> — tooling/fix branches run the agents directly
  (CLAUDE.md Project tooling)`. Never die on a missing `--spec`; never invent a spec
  so the sweep has something to mutate.
- Spec: the one `specs/phase-*.md` whose slug matches the branch name
  (`phase-18a-cost-and-ops` → `specs/phase-18a-cost-and-ops.md`). If none
  matches, ask for `SPEC=` and stop. Set `SPEC=<that path>`.
- Tag collision, checked HERE before anything runs: `uv run python
  scripts/round_tag.py read N` must fail with "missing" — if it prints a record,
  round N already ran; STOP (a round is reviewed once; a re-run is round N+1, or
  the developer deletes the tag on purpose). For N ≥ 2, `read N−1` must print a
  record; a parse error or "missing" STOPS the command — the previous round's
  tag is not a round record, and nothing is inferred from it.
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

## 5. Consolidate, tag, cap-check, STOP

Print one table over every finding from every agent:

| # | Finding (one sentence) | Raised by | file:line | Class | In range / missed in round N−1 |
|---|---|---|---|---|---|

Class is exactly one of **correctness** (wrong output, a survivor, an invariant
with no pin, a caller/clock-sourced mechanism), **security**, **record** (a
stale or missing record sentence), **wording** (names, comments, docs prose).

Tag — every completed round is tagged BEFORE the cap check, or the scoped pass
that follows a cap has no boundary. The tag is written by CODE, never composed
by hand (DECISIONS "Process": model-written text reaches a control decision only
through fixed fields a script parses):

```
uv run python scripts/round_tag.py write N --range <RANGE> \
  --agents code-reviewer,functionality-tester[,security-reviewer] \
  --correctness <count of correctness rows in the table> \
  --cap <yes|no|n/a> --gate "review-gate:OK mutate:<killed>/<survived>/<errors>"
```

`--cap` is `n/a` in round 1; otherwise `yes` when the table has ≥ 1 correctness
row and EVERY one falls inside `review-round-(N−1)..HEAD` (the previous round's
fixes), else `no` — zero correctness rows is `no` (no findings is no evidence),
and the script refuses any other combination. The script refuses an existing
tag, validates the six fields against their patterns, reads the tag back, and
never pushes (a local annotated tag; `git push` sends none unless asked).

Cap check (CLAUDE.md Workflow rules, "Review cap" — two consecutive rounds), as
code: `uv run python scripts/round_tag.py cap N --this <the --cap value above>`
prints exactly one of

```
CAP: fixes are generating findings — write the invariant, re-implement once
cap watch: one more such round trips the cap
no cap
```

(round 1 reads no tag; N ≥ 2 reads `review-round-(N−1)` with the anchored
parser — a bad tag is a parse error that stops the command, never a default;
CAP needs N ≥ 3, this round `yes` AND the previous round `yes`). On CAP, STOP:
the next step is a fix amendment, then ONE scoped pass (the round after this
one, against the tag just written), not another round of patches.

Close with the one line the developer decides on per finding: **fix
(wording/test-only)**, **fix amendment (design change → spec paragraph first,
stop for approval)**, or **accept (BACKLOG row with a trigger)**. Then STOP.

This is an explicit, on-request review. Do not treat its presence as a cue to
run it automatically — it runs only when invoked.
