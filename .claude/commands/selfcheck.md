---
description: Post-commit self-check — verify the last commit against its spec and this repo's policies, then STOP.
---

Verify the current branch's most recent commit. Execute the checks, report,
then **STOP — no push, no agents, no fixes.**

Report each, concisely, with concrete evidence (counts, pass/fail output,
file:line):

- **(a) Suite** — run `uv run pytest -q`; report pass/fail counts.
- **(b) DONE command** — if the commit implements a spec in `specs/`, run that
  spec's DONE command and paste its real result. The DONE command is the only
  definition of done; "tests pass" alone does not substitute. Exception: if
  the DONE command is destructive (`make down`) or costs API tokens
  (`make agent-*`), report that it needs explicit user go-ahead instead of
  running it.
- **(c) Determinism** — name any step this commit adds that could give a
  different answer on a re-run (unseeded randomness, wall-clock on the data
  path, LLM calls, unordered output). Confirm each is justified in
  DECISIONS.md, or flag it.
- **(d) Fixtures** — no commit in this work touches `fixtures/tiny/` (after
  Phase 1): on a branch check `git diff main...HEAD --stat -- fixtures/`; on
  main check `git diff HEAD~1 --stat -- fixtures/` (skip if HEAD has no
  parent). Must be empty. Also confirm no test was weakened to get green.
- **(e) Divergence** — any spec-vs-ARCHITECTURE.md-vs-reality gap hit during
  the work: name it and confirm it was reported to the user, not silently
  adapted.
- **(f) Eyeball** — the ONE file you'd most want a human to read
  line-by-line, and why.

This is an explicit, on-request verification. Do not treat its presence as a
cue to run it automatically — it runs only when invoked.
