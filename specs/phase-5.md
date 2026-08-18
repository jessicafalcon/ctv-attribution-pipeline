# Phase 5 — Engine hardening

Contract for the `phase-5-engine-hardening` branch. Source: `docs/PHASES.md`
→ Phase 5, `docs/ARCHITECTURE.md` §3.3 "Attribution engine", DECISIONS.md
(Phase 3), and the four Phase-5-due BACKLOG rows (dedup/device-state, medium
baseline, and the two resolve-continuous-follow rows — reconciled below).

Add the hardening features **one at a time, each with its own
producer-knob-driven test** (CLAUDE.md: "Engine features are added one at a
time, each with a test that uses a producer knob to exercise it"):

1. **Dedup (full seen-set)** — a stream-level `conversion_id` / `exposure_id`
   seen-set that drops exact re-sends early (batch drain; no in-batch TTL — TTL
   is a continuous-mode scaling note, see that feature).
2. **Watermarks + allowed lateness** — an event-time watermark that retains
   exposure state long enough to admit conversions whose matching exposures are
   up to `allowed_lateness` late; conversions themselves are never dropped by a
   lateness gate (they are pure probes — see the invariant below).
3. **Hot-window eviction** — exposure state ages out once no in-tolerance
   conversion can still match it; an `engine_` gauge shows state rise and fall.
4. **Assists / `processed_at` / `path`** — already on `AttributedConversion`
   since Phase 3; this phase only proves they stay correct through the windowed
   path (assists survive eviction; `processed_at = ingest_time`; `path = hot`).

The pure core (`streaming/attribute.py`) still owns every attribution DECISION;
the new windowing is deterministic and event-time-driven (no wall clock), so
replay and the live engine cannot diverge (DECISIONS Phase 3).

## DONE command

```
uv run python -m streaming.replay --profile tiny --source fixtures && \
diff fixtures/tiny/expected/attributed.jsonl data/out/tiny/attributed.jsonl && \
make down && make up && \
make seed PROFILE=medium && make run PROFILE=medium && \
make eval PROFILE=medium && \
make test && make lint
```

Plus the opt-in live parity + eviction + dedup integration test
(`make test-int`, run by CI's integration job):
`tests/integration/test_engine_hardening.py` — which itself re-asserts the tiny
golden gate below against ClickHouse FINAL.

## Acceptance gate 0 — the frozen tiny golden is the regression oracle (HARD GATE)

The 55-row `fixtures/tiny/expected/attributed.jsonl` is frozen (Phase 1) and is
**the instrument that proves the rewritten engine did not silently change
attribution semantics**. This phase replaces an order-independent, event-time
core (`attribute.py` — `max`/`min` over total orders, no arrival dependence, no
eviction) with an arrival-ordered, watermarked, evicting operator. So:

- **Byte-identical tiny output is a Phase-5 acceptance gate, not an afterthought.**
  The offline replay must still emit the exact 55 golden rows, and `test-int`
  must still find `attributed_conversions` FINAL == the golden fixture.
- **If the evicting engine changes even one tiny row, STOP** — do not edit the
  frozen fixture. A changed row means either the eviction bound is wrong or a
  conversion was dropped that should not have been. Report it; do not "fix" it by
  touching the fixture.

Tiny survives **only** under the eviction rule pinned below: tiny's event-time
span is ~3.5d (exposures ~37h + conversion delays ≤48h), all **< 7d**, so the
watermark never reaches any exposure's eviction bound → **zero exposures
evicted** → every conversion still finds its state → identical output. `medium`
(span 10–14d) is what makes the gauge rise and fall. That is why the span split
exists and it is internally consistent with the `medium` design.

## Operational Done-when (robustness-oracle equality)

Generate the `medium` profile once (seed pinned **in the profile**). Let **E**
= the full generated event set with duplicates **and** late arrivals baked in.
Score **E two ways** and compare:

- **Oracle P/R.** `score(...)` over `attribute(dedup_by_id(E))` fed in
  **event-time** order through the existing 7-day last-touch core (reuse the
  offline-replay path — pure, no services). This is "attribution if every event
  had arrived exactly once, on time."
- **Engine P/R.** `score(...)` over `attributed_conversions` **FINAL** after the
  **live hardened engine** consumes E in **arrival order** (topic-offset / emit
  order as drained — see "Arrival order" below, NOT a re-sort by the field) with
  watermarks + `allowed_lateness ≥ profile.late.max_minutes`.

Both use the same truth side file (`data/truth/medium/truth_links.jsonl`, in the
harness only — N1) and the same `exposure_id → household_id` map.

**Done when ALL of:**

1. **Parity.** Engine precision AND recall equal Oracle precision AND recall,
   **exactly** (same floats, not "close"). This is the correctness proof that
   dedup + allowed-lateness + eviction drop no in-tolerance data.
2. **Eviction ran.** The `engine_` join-state gauge is **> 0** at some point
   over the run (state actually built up) **and** returns below its peak (state
   actually evicted). A vacuous run where nothing ages out fails this clause.
3. **Dedup, separate assertion.** P/R parity holds with or without a dedup
   mechanism (exact re-sends share a `conversion_id` and collapse under
   `reduce_conversion` + ReplacingMergeTree FINAL either way), so **P/R MUST NOT
   stand in for the dedup test**. Prove dedup directly:
   - `engine_dedup_suppressed_total > 0` with the duplicate knob on, AND
   - `attributed_conversions` FINAL row count is **identical** to a
     dedup-suppressed-off run over the same E.

`make test` (offline) and `make lint` green throughout.

## `medium` profile constraints (load-bearing — do not discover mid-phase)

`producer/profiles/medium.json`, seed pinned. The generator (`generate.py`
`_ingest_time` / `_with_duplicates`) already implements the late and duplicate
knobs; `medium` sets them so both mechanisms genuinely fire.

- **Span > hot window.** The exposure event-time span must exceed 7 days
  (target **10–14 days**) so exposure state genuinely ages out; otherwise
  Done-when clause 2 is vacuous. Size `n_exposures` / `events_per_hour`
  accordingly.
- **`late.max_minutes ≤ allowed_lateness`.** Keep every late arrival inside the
  hot-path tolerance, so the oracle stays a plain `dedup + replay` with **no
  lateness-drop bookkeeping** and clause-1 parity is exact. If any profile ever
  sets `late.max_minutes` beyond `allowed_lateness`, those late events are
  *designed misses* and the oracle must drop them too — but `medium` stays under
  the guard (this phase proves "no drops," not "correct drops").
- **Fractions high enough to fire.** `duplicate_fraction` and `late.fraction`
  sized so the dedup-suppressed counter is non-trivial and there exist late
  arrivals that would go unattributed without allowed-lateness (i.e. a
  no-watermark engine would score *worse* than the oracle — sanity-check this
  when tuning, so the features aren't dead weight on `medium`).

The clean-run baseline the developer asked BACKLOG to pin is the **Oracle P/R**
above (computed from the same E), not a second seed — recorded so a future reader
knows `medium`'s numbers won't be tiny's 0.673.

## What each feature does

### Dedup — full seen-set in batch mode; TTL is a scaling note, not code

Teaching note (first appearance): a **seen-set dedup** keeps the ids already
processed and suppresses a second arrival of the same id. The key is
`conversion_id` (and `exposure_id` on the exposure side); a re-send is dropped
before it reaches the window/join and before it is offered to `exposures_landed`,
and is counted (`engine_dedup_suppressed_total`).

**Why a full seen-set and not a TTL'd one (settled — this drove the design).**
The duplicate injector re-sends the **identical payload**: the re-send carries
the same `event_time` AND the same `ingest_time` as its original (the
`+uniform(10,300)` in `generate.py:56-59` is a sort key for emit order, then
discarded — never a field). So original and re-send are **field-indistinguishable
in time**; nothing an event-time TTL could measure against differs between them.
An event-time TTL sized to the 300s re-send delay would also sit on a
seed-dependent knife-edge (on a denser stream the watermark can advance ~300s of
event-time between a pair, evicting the id from a 300s TTL before its re-send →
`dedup_suppressed_total` silently undercounts, brittle across seeds; RMT collapse
still makes clause-1 parity pass and *hides* it). And "TTL boundary" cannot be a
producer-knob test — the knob can't push a duplicate past an event-time TTL when
the pair has identical timestamps — which would contradict CLAUDE.md's
knob-driven rule.

So: the engine is a **bounded batch drain that already holds the whole topic in
memory**; keep a **full `conversion_id` / `exposure_id` seen-set for the drain,
no in-batch TTL eviction**. It is O(n), the same order as the grouping that
already exists, and deterministic on the single partition. **The TTL story is
real only for continuous mode (explicitly out of scope — Phase 5 stays
batch-drain), so it is a `SCALING.md` / `DECISIONS.md` note** — "seen-set TTL'd by
`event_time + max_resend_delay` once the engine follows continuously" — not
speculative code now. This is the project's "simplest standard solution now;
scaling path is a note, not code" contract applied cleanly.

- **ARCHITECTURE §3.3 / §8 reconciliation — DONE this phase (Option A).** §3.3
  previously said dedup uses "TTL'd state sized to the max plausible duplicate
  delay"; that sizing provably can't hold in batch, so it was corrected (not
  DECISIONS-footnoted), per the repo's convention that ARCHITECTURE tracks reality
  and §8 parks batch-vs-continuous deviations. Landed in the spec commit:
  §3.3 prose + diagram qualify dedup as a batch seen-set (TTL under continuous
  follow); §8 gains a timestamp-identical-re-send gotcha and its stale
  "continuous follow lands in Phase 5" line is fixed to "windowing on the batch
  drain; continuous follow deferred"; `DECISIONS.md` Phase 5 carries the why;
  `SCALING.md` (seeded) carries the continuous-mode TTL. So the coherence-auditor
  meets one consistent story, not a half-updated §3.3.
- Dedup is a **stream-level** mechanism (keep join-state and insert volume down,
  make re-sends observable). It is **distinct** from the pure core's set-semantics
  assist dedup and from ReplacingMergeTree's read-time collapse — the spec must
  not conflate the three. Because RMT collapses re-sends anyway, dedup is
  *semantically transparent*: the Done-when clause-3 "FINAL count == dedup-off
  run" invariance is exactly that transparency check, and the counter is what
  proves the mechanism actually fired.
- **Do not conflate "unknown device" with "duplicate"** (BACKLOG, Phase 1
  note). Unknown `u-` device ids never repeat, so they exercise IP fallback, not
  dedup. The dedup test drives re-sends via `duplicate_fraction` **only**, and
  asserts an unknown-device conversion is not treated as a duplicate.

### Watermarks + allowed lateness (ARCHITECTURE §3.3 "Lateness")

Teaching note (first appearance): a **watermark** is the pipeline's estimate of
how far event-time has advanced, held back by a grace period so late events
still land. **Allowed lateness** is that grace.

**Pinned definitions (load-bearing — do not paraphrase in code):**

- `watermark = max(event_time seen so far) − allowed_lateness`. Derived from
  **event_time only**, never wall clock (determinism policy). The `−
  allowed_lateness` lag is where the grace lives.
- `allowed_lateness` is an engine config (env-overridable, default documented),
  and MUST be `≥ profile.late.max_minutes` for `medium` — enforced/asserted, not
  assumed.

### The two "lates" — do not conflate them (this is the trap that breaks tiny)

Two distinct quantities both get called "late"; only the second is bounded by
`allowed_lateness`:

- **Conversion delay** = the *event-time* gap between an exposure and the
  conversion it causes (tiny: `conversion_delay_minutes [10, 2880]` → up to
  **48h**). This is **normal, in-window (7d)** behavior — the current engine
  attributes all of it. It is NOT lateness.
- **Arrival lateness** = `ingest_time − event_time`, the `late` knob (tiny
  ≤180min). This is what `allowed_lateness` bounds.

**Invariant (state it in the code):** a conversion is matched against **any
exposure still in state under the 7d bound, regardless of how far its
`event_time` trails the watermark.** Matching keys on `event_time` eligibility
(`conv.event_time − 7d ≤ exp.event_time ≤ conv.event_time`), never on the
conversion's distance behind the watermark. **Conversions are pure probes: a
conversion is never dropped by a conversion-side lateness gate.** It becomes
unattributed for exactly one reason — its matching exposure was already evicted
(a *state-miss*) — never because the conversion itself is "too late." Dropping a
conversion because its `event_time` lags the running watermark would discard
conversions the current engine attributes → **tiny golden breaks** (tiny's 48h
conversion delays would trip a naive gate), and the wrong "fix" would be editing
a frozen fixture. Do not do this. `allowed_lateness` governs only whether the
*exposure* is still retained (via the watermark lag in the eviction bound below),
never whether a conversion is admitted for matching.

### Hot-window eviction (ARCHITECTURE §3.3 "Hot window state")

Teaching note (first appearance): **eviction** drops window state that can no
longer affect any future output, which is what bounds memory — the "central
scaling constraint" (ARCHITECTURE §3.3).

**Pinned eviction rule.** An exposure at event-time `T` is evicted once
`watermark > T + HOT_WINDOW` (7d). Because `watermark = max(event_time) −
allowed_lateness`, this is equivalently `max(event_time) > T + 7d +
allowed_lateness` — the `allowed_lateness` grace is folded into the watermark,
so the bound itself is the clean `T + 7d`. An in-tolerance late conversion at the
far window edge (`event_time = T + 7d`, arriving up to `allowed_lateness` late)
still finds exposure `T` in state, because the watermark lags by exactly that
grace. This is what makes clause-1 parity hold.

- **Correctness invariant (say it in the code):** eviction MUST NEVER drop an
  exposure that an in-tolerance conversion could still match. The bound above is
  that guarantee.
- **Why tiny is unaffected:** tiny's max `event_time` is ~3.5d after
  `sim_start`; the watermark (max event_time − ≤180min) never reaches any
  exposure's `T + 7d` bound → zero evictions → the golden holds (gate 0).
- Expose an `engine_` gauge of live window-state size (per-key or total,
  implementer's call) so Done-when clause 2 can observe rise and fall. On `medium`
  it rises as early exposures accumulate and falls as they cross `T + 7d`.

### Assists / `processed_at` / `path` (delivered Phase 3; Phase 5 regression-guards them through eviction)

Disposition on record (so the coherence-auditor sees it, not rediscovers it):
PHASES lists these as Phase-5 items, but Phase 3 already delivered all three —
`_attributed` sets them, the model carries them, and the golden fixture rows have
them. This is a latent PHASES-vs-reality drift; Phase 5's job is to **prove they
survive the evicting window**, not re-add them.

`AttributedConversion` already carries `assists`, `processed_at` (= conversion
`ingest_time`, event-derived RMT version), and `path` (`hot`). This phase does
**not** re-add them; it proves they remain correct once matching runs against an
**evicting** window: assists are the in-window non-credited exposures *still
retained* at match time (identical to the all-in-memory core because eviction
only removes exposures no conversion can still match), and `path` stays `hot`.
A test asserts assist parity between engine and oracle on `medium`.

## Where the windowing lives (engine shape)

The engine stays a **bounded batch drain** (ARCHITECTURE §8 gotcha; no
continuous Kafka follow, no wall clock — determinism policy). The change from
Phase 3: instead of `fold_final` collecting an entire household's rows
order-independently, the hardened engine processes the drained stream **in
arrival order** through a stateful, event-time-watermarked, evicting window
operator, so watermark/lateness/eviction are meaningful and deterministic.

**Arrival order = topic-offset / emit order as drained — do NOT re-sort (this is
load-bearing for the watermark, not for dedup).** Consume in **offset order** as
`common.kafka.drain` returns it:

- The producer already emits in `(ingest_time, id)` order (`generate.py:60`), so
  offset order **is** arrival order — re-sorting by `ingest_time` buys nothing and
  only risks divergence. Re-sorting by **`event_time`** would be **wrong**: it
  would place every late event at its event-time position and erase the lateness
  the watermark exists to handle, so eviction/allowed-lateness would never be
  exercised and clause-1 parity would be meaningless.
- Dedup does **not** depend on order here — the full seen-set suppresses a re-send
  at any offset distance (unlike a TTL, which is why the TTL was dropped). Order
  matters only so the **watermark advances deterministically** as events arrive,
  which is what makes eviction and the state-miss the real, reproducible thing
  clause 1 checks.
- Offset-order determinism requires **single-partition topics**. Confirmed:
  `exposures` / `conversions` / `device_graph` are `num_partitions=1`
  (`producer/seed.py:28-31`) and `conversions_resolved` is `num_partitions=1`
  (created by the resolve stage, `resolve/stage.py:43`). If any topic ever goes
  multi-partition, offset order is per-partition only and this determinism
  argument must be revisited.

- Every decision still lives in `streaming/attribute.py` leaves; the new
  windowed operator is plumbing that calls the same leaves, so the offline
  replay (oracle) and the live engine agree by construction.
- **What the oracle and engine share, and what they must NOT share.** The shared
  code is the **decision leaves** — `attribute_household`, `reduce_conversion`,
  and the new `dedup_by_id` — in `streaming/attribute.py`. The **engine** adds
  arrival-order consumption + watermark + eviction; the **oracle deliberately
  does NOT evict** (it is `attribute(dedup_by_id(E))`, all-in-memory,
  non-evicting — the Done-when "Oracle P/R" definition). Their exact agreement is
  precisely the proof that eviction drops nothing. **The oracle must never import
  or call the eviction/watermark path** — if it did, a bug in eviction would
  corrupt both sides equally and clause-1 parity would pass green while both are
  wrong. Parity is a check *because* the two paths differ in exactly one thing:
  eviction.
- **Pre-committed default: the eviction/watermark window lives in the pure core;
  Bytewax carries keyed state only.** Keeping the window as pure functions (used
  by the engine, not the oracle) is the existing "Bytewax owns plumbing, the pure
  core owns decisions" decision (DECISIONS Phase 3), not a new one, and it
  sidesteps whether Bytewax's windowing can express the exact event-time eviction
  bound deterministically. Verify the Bytewax stateful-operator docs, but only
  move window logic into Bytewax if its windowing buys something concrete — and
  the **tiny-golden identity (gate 0) is the arbiter either way**. Record the
  final split in DECISIONS.md.

## BACKLOG reconciliation (Phase-5-due rows)

- **Unknown-device vs duplicate** (Phase 1 note) — **addressed here**: dedup
  keys on `conversion_id`/`exposure_id`; a test asserts a non-repeating `u-`
  device conversion is not counted as a duplicate. Row closed.
- **`medium` clean baseline** (Phase 4 coherence audit) — **addressed here**:
  the Oracle P/R over the same E is the pinned baseline; late knob kept
  `≤ allowed_lateness` per the constraint above. Row closed.
- **Resolve continuous-follow: graph refresh** and **conversions-offset
  reprocessing** (Phase 2 rows, trigger "when resolve moves to continuous
  follow") — **NOT triggered**: Phase 5 hardens the *engine*, and the engine
  (and resolve) stay batch-drain. Re-defer both with the trigger unchanged
  ("when resolve moves to continuous follow"); note in the phase-exit BACKLOG
  review that Phase 5 did not move resolve.

## Determinism

- The windowed engine is a pure function of (events in arrival order, window,
  allowed_lateness): no wall clock, no entropy. Same `medium` seed → identical
  `attributed_conversions` FINAL and identical `engine_dedup_suppressed_total`.
- `processed_at = ingest_time` unchanged; RMT keyed `conversion_id` version
  `processed_at`. Replaying either topic from offset 0 converges to the same
  FINAL state (idempotency contract).
- Oracle and engine share the pure core, so clause-1 parity is deterministic,
  not sampled.

## Scope (files)

- `producer/profiles/medium.json` — new profile, seed pinned, span 10–14d,
  `late.max_minutes ≤ allowed_lateness`, non-trivial `duplicate_fraction` /
  `late.fraction`. **`fixtures/tiny/` and `tiny.json` untouched, frozen.**
- `streaming/attribute.py` — add pure helpers reused by oracle + engine:
  `dedup_by_id(...)` and the window/watermark/eviction decision leaf(s). Keep
  existing leaves (`attribute_household`, `reduce_conversion`) working; no
  behavior change on tiny.
- `streaming/dataflow.py` — replace the order-independent `fold_final` grouping
  with the in-arrival-order, watermarked, evicting stateful operator calling the
  pure leaves. `allowed_lateness` config from env (default documented).
- `streaming/metrics.py` — add `engine_dedup_suppressed_total` (Counter) and the
  join-state-size gauge (`engine_join_state_size` or similar). `engine_` prefix.
- `streaming/replay.py` / an oracle entrypoint — expose the "dedup + event-time
  replay" scoring path so `make eval PROFILE=medium` and the parity test can
  compute Oracle P/R without services.
- `accuracy/run.py` — already reads credited + truth for `--profile`; ensure it
  works for `medium` (truth side file path parametrized). No truth into the DB.
- `Makefile` — `make run` / `make seed` / `make eval` already take `PROFILE`;
  confirm `PROFILE=medium` flows through. No new destructive targets.
- Unit tests (no services) — one per feature, each knob-driven:
  - **`tests/test_tiny_golden_regression.py` (gate 0)** — the refactored engine's
    offline replay over `fixtures/tiny/` still emits the exact 55 golden rows
    (byte-identical to `expected/attributed.jsonl`). Fails loud if any tiny row
    changes; never edit the fixture to make it pass.
  - `tests/test_dedup.py` — re-sends suppressed + `dedup_suppressed_total`
    counter fires; a full-seen-set (no in-batch TTL eviction, so a re-send at any
    offset distance is still suppressed); unknown-device (`u-`) NOT treated as a
    duplicate. No "TTL boundary" case — the pair is timestamp-identical, so no
    producer knob can push a duplicate past an event-time TTL (that's why dedup is
    a full seen-set here).
  - `tests/test_lateness.py` — a late conversion is unattributed **only when its
    last-touch exposure has been evicted (a state-miss)**, constructed by
    advancing the watermark past `T + 7d + allowed_lateness` before the
    conversion arrives — **never via a conversion-side lateness check**. Also
    assert a conversion whose exposure is still retained is attributed no matter
    how far its `event_time` trails the watermark (the invariant).
  - `tests/test_eviction.py` — exposure evicted only past
    `T + 7d + allowed_lateness`; gauge rises then falls; an in-tolerance match
    just under the bound still succeeds (the correctness invariant).
  - `tests/test_assists_windowed.py` — assists from the evicting window equal the
    all-in-memory core on `medium`.
  - `tests/test_oracle_parity.py` — Oracle P/R over `dedup_by_id(E)` (pure, no
    services) equals engine P/R **when the engine core is run offline over the
    same E** (services-free half of clause 1).
- `tests/integration/test_engine_hardening.py` (opt-in `make test-int`, CI
  integration job) — full clause 1/2/3 against `make up`: seed medium → resolve
  → hardened engine → ClickHouse; engine P/R == oracle P/R exactly; join-state
  gauge rose and fell; `dedup_suppressed_total > 0` and FINAL row count ==
  dedup-off run.

## Determinism / test hygiene

- **Do not weaken any existing test** and **do not touch `fixtures/tiny/`**
  (frozen after Phase 1). Tiny must still produce the Phase-3 golden
  `attributed.jsonl` (55 rows) and the Phase-4 pinned 0.673/1.000 through the
  refactored engine — a regression test that survives the rewrite is the guard
  that hardening didn't change tiny's semantics.
- `tests/test_truth_isolation.py` stays green; truth stays in `accuracy/` /
  `tests/` only.

## Review & stack risk

- **code-reviewer + functionality-tester** at the finish line (mandatory review
  gate). **security-reviewer NOT triggered** unless the work touches CI, `.env`,
  compose exposure, ClickHouse users, or agent/LLM context — it does not (config
  is an `allowed_lateness` env knob mirroring existing `CLICKHOUSE_*` handling);
  note it explicitly rather than running it for nothing.
- **coherence-auditor** at phase exit (mandatory, before the PR merges) + the
  BACKLOG review above.
- **Bytewax stack risk.** A stateful, ordered, evicting window operator is more
  than `fold_final`. Per the pre-commit above, default to window-in-pure-core /
  Bytewax-carries-keyed-state-only; check the official Bytewax stateful-operator
  / windowing docs before working around anything; log any surprise under
  ARCHITECTURE §8 (stack-surprise rule) and the final split in DECISIONS.md. The
  tiny golden (gate 0) is the arbiter of any implementation choice.

## Out of scope

- Reconciliation, `campaign_hourly` rollups, `report_snapshots`, restatements
  (Phase 6). Late arrivals **beyond** `allowed_lateness` are emitted unattributed
  here and recovered in Phase 6 — `medium` deliberately keeps late within
  tolerance so Phase 5 proves "no in-tolerance drops," not reconciliation.
- Naive-vs-optimized benchmark, Grafana/Alertmanager wiring (Phase 7).
- Fault profiles, collectors, agent (Phase 8+). `medium` is a hardening profile,
  not a fault profile.
- Moving resolve (or the engine) to continuous Kafka follow — stays batch-drain
  (determinism policy); the two resolve BACKLOG rows re-defer unchanged.
- Co-view read-time factor (BACKLOG; Phase 7 or agent-narrative trigger).
