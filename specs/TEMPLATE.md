# Phase N — <name> (PROPOSED)

Contract for the `phase-N-<slug>` branch. Source: <PHASES.md entry, or "post-plan
extension — not in the original plan" + the review finding that originated it>.
Depends on <predecessor> merged.

**Status: PROPOSED — do not start until approved.** <Dependency note: "no new
dependencies", or the package and why; any pinned-version feature the phase relies
on is a STOP-and-ask if it turns out unsupported.>

The section order below is the existing spec shape (`specs/phase-15-runbook.md`,
`specs/phase-17-lake-of-record.md`). Three of the sections marked REQUIRED were added
after the Phase-16/17 retrospective: ~60 % of review-gate findings were record files
lagging code, ~15 % were spec clauses written before the predecessor landed. The
fourth, **Invariants**, was added after the Phase-18a review gate (DECISIONS
"Process", 2026-08-22): three rounds each found correctness bugs in the previous
round's fixes because the spec pinned a mechanism instead of the property the
mechanism had to keep. Every spec carries all four; a spec without them is not
approvable. A spec carries at
most ~6 pinned decisions / Done-when items (CLAUDE.md Workflow rules) — split larger
scope into sub-phases (18a/18b), each with its own spec from this template.

## Why

<The problem in the reviewer's words, then why this phase and not a fix PR.>

## The central constraint

**<One bolded sentence.>** <What must not move while the phase moves everything
else — byte-identical pins, elevate-never-invent, etc.>

## DONE command

```
make test && make lint && <the phase's own live gate>
```

- <One bullet per command segment: what it proves and which pin / golden it
  reproduces.>

## Done-when

1. **<Item.>** <Behavioural clause the code can falsify.> *Evidence: <see the
   Evidence section — every item has a row there.>*
2. …

(≤ ~6 items. An item is a contract, not a narrative: a "Done when" claim the code
can falsify. PHASES.md carries the same clauses; if the landing diverges, PHASES.md
is corrected at exit — the spec and DECISIONS are authoritative.)

## Evidence (REQUIRED)

Every Done-when item names the test or command output that proves it. **An item
without evidence is not a Done-when item** — either find its proof or cut it.
The functionality-tester confirms every named test exists and exercises the
claim; a named-but-missing test is a blocker.

| Done-when | Proof (test file / `make` target / command output) |
|---|---|
| 1 | `tests/test_<x>.py::test_<y>` / `make <target>` output line "<…>" |
| 2 | … |

The same table, filled with the actual run's output, is item 2 of the "Before
reporting DONE" checklist (CLAUDE.md Workflow rules).

## Invariants (REQUIRED)

Properties, not mechanisms — written BEFORE any pinned decision names how the
code works. Each is a universally quantified sentence ("for all X, Y holds")
paired with the scenario test that would falsify it. The test is named here first;
the mechanism comes later and must satisfy the invariant, never the reverse.
Reviewers read this list (code-reviewer "Invariants" check; `/review-round`),
the functionality-tester mutates the code that upholds each one, and the
coherence auditor greps the records against it.

| Invariant ("for all …, … holds") | Falsified by (scenario test) |
|---|---|
| For all <X>, <Y>. | `tests/test_<x>.py::test_<scenario>` — <the scenario in one clause> |
| … | … |

Rules:

- **A mechanism is not an invariant.** "A one-row watermark records the last
  refresh" is a mechanism; the invariant it was standing in for is "for every
  versioned row, the version is a function of the row's content, never of the
  caller or the clock". State the second; let the Pinned decisions name the first,
  and only by reference ("satisfies invariant 2").
- **Every invariant names its falsifying test before any code exists.** A test
  that reproduces the scenario under the invariant's quantifier (reverse-order
  loads, a replay, equal sort keys, a non-UTC machine) — not a test of the
  mechanism's happy path.
- **A fix that changes a data structure, a write path, or who-writes-what is a
  design change**: it gets a one-paragraph amendment to this section naming the
  invariant it restores (CLAUDE.md Workflow rules, "Fix amendments").
- **Every invariant's upholding code gets a mutation line.** `make mutate SPEC=…`
  reads ONE fenced block in this section, `path.py::function  operator`, one
  mutation per line; operators are exactly `delete-call`, `constant-return:<v>`,
  `invert-guard`, `swap-sort-key` (CLAUDE.md Commands). Each line is applied to
  HEAD in a throwaway worktree and the offline suite must go red; a `SURVIVED`
  line is a correctness finding (`/review-round` runs it before any agent).

  ```mutations
  lake/load_serving.py::record_dirty_exposure_keys   delete-call
  reconcile/rollup.py::refresh_campaign_hourly        constant-return:0
  clickhouse/apply.py::migrate_report_snapshots       invert-guard
  reconcile/reconcile.py::pick_household              swap-sort-key
  ```

Worked example — Phase 18a's rollup versions (`specs/phase-18a-cost-and-ops.md`;
DECISIONS Phase 18a). The first spec pinned the mechanism: a one-row watermark
table records the last refresh, and the refresh recomputes the keys dirtied since
it. Three review rounds each found a correctness bug in the previous round's fix
(the watermark advanced on a no-op; a replay left it ahead of the data; a
re-computation could lose to an older row under the ReplacingMergeTree). The
invariant that should have been written first:

| Invariant | Falsified by |
|---|---|
| For every versioned row (`campaign_hourly`, `rollup_dirty`, `report_snapshots`), its version = `max(processed_at)` over the rows it summarizes — a function of the data, never of the caller or the clock. | `tests/integration/test_rollup_dirty.py::test_every_rows_version_equals_the_max_stamp_of_what_it_summarizes`. Replay: `::test_a_replayed_refresh_cannot_lose_to_an_earlier_one`; offline `tests/test_rollup_dirty.py::test_the_rollup_row_version_is_data_derived_not_caller_supplied`, `::test_a_reload_of_the_same_day_records_the_same_versions`. Reverse-order load (day 2 then day 1 == day 1 then day 2): the 18a Evidence table names `::test_load_order_does_not_change_the_dirty_set`, which the branch does not contain — the named-but-missing case the functionality-tester now treats as a blocker. |
| For every key whose served rollup row changed, that key was in the set the refresh recomputed (`changed ⊆ dirty`); the served rollup equals a full rebuild. | `tests/integration/test_rollup_dirty.py::test_every_changed_key_was_refreshed`, `::test_the_served_rollup_equals_a_full_rebuild`; reported by `make rollup-bench`. |

The pinned decision then reads "rollup versions are data-derived (`max(stamp)`)
— satisfies invariant 1; the watermark table is gone because no mechanism that
stores a caller-supplied marker can satisfy it".

## Pinned decisions (do not re-litigate)

Each decision may name a mechanism only by reference to the invariant it
satisfies ("… — satisfies invariant N"). A decision that pins a mechanism no
invariant requires is a smell: either write the invariant or drop the pin.

- **<Decision.>** <Why; the alternative rejected in one clause; "satisfies
  invariant N".>
- … (≤ ~6)

## Scope (files)

- <Every file the phase touches, code and record alike.>

## Record updates (REQUIRED)

The explicit list of record files this phase must change. The session checks each
off in its report (checklist item 6); the coherence auditor diffs this list against
the actual diff — any file listed here and absent from the diff is a finding, and
so is the reverse.

- [ ] `DECISIONS.md` — Phase N entry (every non-obvious choice; supersede-pointers
      on any earlier entry this phase reverses)
- [ ] `docs/PHASES.md` — Phase N row: Done-when as landed; "Delivered" paragraph
- [ ] `CLAUDE.md` — Current status row; Commands (every new/changed `make` target);
      dependency allowlist (if a package was added); Event model facts (if a column
      changed); Repo map (if a package moved)
- [ ] `docs/ARCHITECTURE.md` §8 Gotchas — every stack surprise found live
      (+ §3.x components / diagram if the arrow moved)
- [ ] `BACKLOG.md` — rows closed (strike-through + "DONE Phase N") and rows opened
      (deferred findings with a trigger)
- [ ] Spec amendments — every LATER spec this phase invalidates gets a
      "Pre-branch reconciliation required" banner naming the clauses (Phase-17
      precedent: BACKLOG row 56 → `specs/phase-18*.md`)
- [ ] `docs/RESULTS.md` / `docs/SCALING.md` / `docs/RUNBOOK.md` — <only the
      blocks this phase regenerates or the incidents it adds; "none" is a valid entry>
- [ ] `README.md` — <demos / commands / Next steps touched, or "none">

## Threat model (REQUIRED when the phase adds a Makefile target that takes a variable, deletes anything, or takes user input)

For each such target, state the behaviour — and the test pinning it — for:

- an **empty value** (`make <target> PROFILE=`);
- a **path-escaping value** (`PROFILE=../x`);
- a **shell-metacharacter value** (a value containing `"; `);
- the **variable exported from the environment** instead of given on the command
  line (`export PROFILE=…; make <target>`), and for any confirmation knob,
  **`$(origin)` gating** — `CONFIRM=yes` counts only from the command line.

Worked example — the Phase-17 `lake-reset` findings (DECISIONS Phase 17, "Review
gate, round 3"; `lake/destructive.py`; `Makefile` `_YES`): three review rounds each
found a hole in the guard the previous round added — `$(origin CONFIRM)` was
defeated by `MAKEFLAGS='CONFIRM=yes'`; guard / prompt / `rm` on separate recipe
lines were stepped over by `make -i`; an env-origin `PROFILE='$(shell …)'` is
expanded on every `$(PROFILE)` reference. The settled shape: one Python process
validates the profile (`[a-z0-9_]+`), derives the root from it (no path argument
exists to escape with), prompts on a tty, then acts; every recipe is one line; the
residuals (`MAKEFLAGS`, env-origin `$(shell …)`) are STATED, with the threat model
("mistakes, not a user who controls the environment"). Write the table up front so
the gate reads it instead of discovering it.

| Target | empty | `../x` | `"; ` | env-exported | `$(origin)` on CONFIRM | Pinned by |
|---|---|---|---|---|---|---|
| `make <target>` | … | … | … | … | … | `tests/test_makefile.py::…` |

(If the phase adds no such target, keep the heading and write "None — no new
Makefile target takes a variable, deletes, or reads input.")

## Review & stack risk

- **code-reviewer** (mandatory): <what it checks here>.
- **security-reviewer** (<mandatory if CI / .env / compose exposure / ClickHouse
  users / agent context are touched, else "not triggered — reason">).
- **functionality-tester**: DONE command + <the phase's specific negative tests>.
- **coherence-auditor** at exit: <the stale sentences it must find gone>; diffs
  the Record-updates list above against the actual diff.
- Stack risk: <pinned-version features to verify in the first hour; STOP and
  report before any workaround; findings go under ARCHITECTURE §8>.

## Out of scope (deferred, recorded)

- <Each item with where it is recorded — BACKLOG row, SCALING tier note, a later
  phase's spec.>
