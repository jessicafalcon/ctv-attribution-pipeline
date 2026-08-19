# Phase 6 — Reconciliation and restatements

Contract for the `phase-6-reconciliation` branch. Source: `docs/PHASES.md`
→ Phase 6, `docs/ARCHITECTURE.md` §3.3 "Reconciliation job" / "ClickHouse" /
"Reporting", DECISIONS.md (Phase 3 forward-note: reconciled `processed_at` >
hot), and the Phase-5 BACKLOG row "Phase 6 needs a long-delay profile".

The second attribution path: a periodic job recovers conversions the hot path
missed, closing the long-window tail without keeping 90 days of processor state.

## The spine — what actually leaves a reconciliation candidate

The Phase-5 engine treats a conversion as a **pure probe**: it is buffered until
the watermark releases it, and the EOF flush attributes it against complete
state. So **arrival lateness alone never leaves a reconciliation candidate** — a
conversion that arrives days late (high `ingest_time`) is still matched by the
flush. PHASES.md says "days-late arrivals recovers those conversions"; read that
as **long event-time delay**, not arrival lateness (a source imprecision to
correct in PHASES, same as Phase 5 did).

The one genuine hot-path miss: a conversion whose causal exposure is **>7 days
before it in event-time**. The hot window evicts an exposure once
`watermark > exposure.event_time + 7d`; a conversion whose `event_time` is more
than 7d after its exposure releases only after that bound, so the exposure is
already gone → the conversion is emitted **unattributed** (a state-miss). Its
exposure still lives in `exposures_landed` (landed regardless of hot eviction),
so reconciliation can recover it by matching over the long (≤90d) window.

`medium` maxes `conversion_delay` at 3d (< 7d), so it produces **zero**
reconciliation candidates — this is why Phase 6 needs a new `long_delay` profile
whose delay straddles the 7d boundary (BACKLOG, Phase 5).

## DONE command

```
uv run python -m streaming.replay --profile tiny --source fixtures && \
diff fixtures/tiny/expected/attributed.jsonl data/out/tiny/attributed.jsonl && \
make test && make lint
```

Plus the live reconciliation proof on a clean `long_delay`-only stack:
`make test-int-long-delay` (make down && up && seed long_delay && run long_delay
→ `tests/integration/test_reconcile.py`), which asserts the recovery delta and
the restatement. Shared `make test-int` stays tiny-only (tiny/medium/long_delay
share conversion_id space; DECISIONS Phase 5). Gate 0 (tiny golden byte-identical)
still holds — reconciliation is additive, the hot path is untouched.

## Done-when

On a clean `long_delay` stack (seed → resolve → hot engine → reconciliation):

1. **Recovery delta.** After the hot run, the long-delay caused conversions are
   **unattributed** (state-miss). After one reconciliation pass, they are
   **attributed** with `path=reconciled`, and household-grain **recall rises** by
   the recovered **caused, correctly-household-resolved** count — not by every
   recovered row (an organic recovered conversion raises precision-FP, not recall;
   a shared-IP-misresolved one doesn't raise recall either). Pinned numbers (from
   the generated profile, like Phase 5's medium — not invented here) live in the
   tests.
2. **Idempotent recovery.** A second reconciliation pass over the same state is a
   no-op on FINAL (same corrected rows, same `reconciled_at` version → RMT keeps
   one). Replaying converges.
3. **Restatement.** `report_snapshots` holds a pre-reconciliation snapshot and a
   post-reconciliation snapshot with distinct, ordered `reported_at`; the
   restatement query shows the per-campaign **serving-metric** change between them
   (ROAS and/or the truth-free match-rate move as recovered conversions land).
   **Recall is not a snapshot metric** — it is truth-derived, so its rise is the
   accuracy-harness delta (clause 1), scored out-of-band by `make eval`, never
   written to a table (truth-isolation, N1).
4. Gate 0, `make test`, `make lint` green.

## Reconciliation job (`reconcile/`)

Reuses the **pure `attribute_household` leaf** at a 90d window — no second
attribution implementation, so hot and reconciled decisions cannot diverge
(BACKLOG "split attribute.py" row stays deferred). Steps:

1. **Read candidates — hot-unattributed rows ONLY.** `attributed_conversions`
   FINAL where **`attributed = 0 and path = 'hot'`** (pin this exact WHERE).
   Reconstruct a `ResolvedConversion` per row (the table carries every resolved
   field). **Truth-free**: reads pipeline state only, never `data/truth/` — so
   `reconcile/` stays inside the isolation guard (`tests/test_truth_isolation.py`).

   **Why `path = 'hot'` (not all `attributed = 0`) is load-bearing** (correctness
   constraint, pin + test it): reconciliation must never re-open a
   **hot-*attributed*** row. Re-attributing an already-credited conversion over a
   90d window yields the *same* last-touch exposure (the 90d last-touch equals the
   7d last-touch whenever an in-7d exposure exists), but writing it back with a
   higher `processed_at` would flip its `path` from `hot` to `reconciled` for **no
   change in attribution** — corrupting the `path` label and forcing needless
   rewrites. Scoping to `attributed = 0 and path = 'hot'` keeps reconciliation's
   job "recovery only" and matches the `path = hot|reconciled` contract. It also
   makes the second-pass no-op fall out for free: still-unmatched candidates write
   nothing (step 4), so a second pass re-selects them, re-fails, and changes
   nothing. A test asserts a hot-**attributed** row is untouched by a
   reconciliation pass.
2. **Read candidate exposures** — from `exposures_landed` FINAL, for the
   candidates' households, with `event_time` in
   `[conv.event_time − 90d, conv.event_time]`. Reconstruct `Exposure` models.
   Faithful by construction: `exposures_landed` carries **every `Exposure` field
   the leaf reads** (`event_time`, `exposure_id`, `household_id` — ddl.sql:35-48),
   and all timestamps are `DateTime64(3)` with the producer rounding to
   `round(..., 3)` (ms), so the ClickHouse→pydantic round-trip is **lossless** and
   the leaf gets byte-identical inputs whether hot or reconciled.
   - **Bulk-load once, group in memory — not per-candidate (avoid N+1).** Fetch the
     candidate households' in-window exposures in **one** query and build
     `exposures_by_household` in Python (the pure `reconcile()` already takes that
     shape), rather than a query per candidate conversion. Fine either way at
     `long_delay` scale, but make it a conscious choice — a one-line SCALING note
     (per-candidate reads are an N+1 pattern that bites at volume).
3. **Attribute** — `attribute_household(exposures, [conv], window=LONG_WINDOW)`
   (`LONG_WINDOW = timedelta(days=90)`). A conversion with an in-90d exposure
   becomes attributed; one still without stays unattributed (permanent, or a
   future pass).
4. **Write corrected rows** — only the newly-attributed ones, `path="reconciled"`,
   `processed_at = reconciled_at`. Insert into `attributed_conversions`; RMT
   supersedes the hot unattributed row (higher version, same `conversion_id`).
5. **Refresh `campaign_hourly`**, **write a `report_snapshot`** (below).

`reconciled_at = max(ingest_time over the fixed input set) + Δ`
(`Δ = RECONCILE_DELTA`, a **documented constant** ≥ 1 ms — `DateTime64(3)`
resolution). Two things pinned so the idempotency contract holds:

- **The max's input set is fixed and defined explicitly** (record in DECISIONS):
  `max(ingest_time)` is taken over **all landed serving state** — the union of
  `exposures_landed` FINAL and `attributed_conversions` FINAL `ingest_time` — a
  set that does **not** change based on which conversions get recovered. So a
  re-run computes the *same* `reconciled_at`, and reconciliation converges
  (replay-safe). Taking the max over only the *candidate* rows would be wrong: the
  candidate set shrinks as rows are recovered, so the max could move between
  passes.
- `max(ingest_time)` ≥ every hot `processed_at` (= a conversion's `ingest_time`),
  and `+Δ` makes it **strictly** greater even for the latest-arriving conversion
  (which may itself be a candidate) — satisfying the DECISIONS Phase-3 rule that a
  reconciled version must exceed the hot version. Data-derived, **no wall clock**.

Deterministic: same state → same `reconciled_at` → idempotent RMT replacement.

## `long_delay` profile (`producer/profiles/long_delay.json`)

Seed pinned. `conversion_delay_minutes` = a **single uniform range straddling the
7d hot window and the (7d, 90d] long window** (start ~`[10, 43200]` = 10 min–30 d),
so caused conversions split into hot-attributable (delay ≤7d) and reconciliation
candidates (delay >7d) — recovery is then a measurable delta, not all-or-nothing.
Constraints:

- **Delay max ≤ 90d** (the long window) so every long-delay conversion is
  *recoverable*. A delay >90d is permanently unattributed — a **different** test
  (defer to a fault profile if wanted); keep this profile's max ≤ 90d so recovery
  is total and the Done-when reads cleanly.
- **Check the hot/reconciled split after generating; keep a healthy hot
  baseline.** A flat `[10min, 90d]` is ~92% reconciled / ~8% hot (P(delay>7d) is
  most of the range) — too lopsided: the restatement delta needs a **non-trivial
  pre-reconciliation baseline** to read against. After generating, inspect the
  split; if the hot share is trivial, pull the max **down** (~20–30d gives a
  healthier mix), then pin. This is why the range starts at ~30d, not 60d.
- Not a fault profile: keep the noise knobs medium-like — `shared_ip_fraction`
  `0.2`, `unknown_device_fraction` `0.1` (matching `medium.json`) — so the
  recovered conversions are mostly clean household matches, not a shared-IP
  wrong-household spike (that is Phase 8). (Note: `shared_ip 0.2` still yields the
  2 residual wrong-household hot attributions that cap `long_delay` recall at
  73/75 — DECISIONS Phase 6.)
- Event-time span and other knobs sized so both paths are non-trivially
  populated (enough hot-attributed AND enough recovered). Late-injector may stay
  small — arrival lateness is not what creates candidates here.

Exact counts (hot-miss, recovered, split ratio) pinned in tests **after
generating** (Phase-5 medium precedent), never invented in this spec.

## ClickHouse (`clickhouse/`)

- **`campaign_hourly`** — rollup, **versioned-replace ReplacingMergeTree**
  (`order by (campaign_id, hour)`, version a refresh column), refreshed by
  recomputing from `attributed_conversions` FINAL + `exposures_landed` FINAL and
  inserting a new version per `(campaign_id, hour)`; readers use FINAL. NOT a
  `TRUNCATE` (destructive-command rule) and NOT an insert-triggered summing MV
  (corrections would double-count — ARCHITECTURE §3.3). This is not just *allowed*
  but **mandated**: CLAUDE.md's determinism policy already forbids insert-triggered
  summing MVs for exactly this reason.
  - **Each refresh recomputes and rewrites ALL `(campaign_id, hour)` keys** with
    the new version — not a delta — so no key holds a stale value under FINAL. A
    key that *disappears* between refreshes (no rows any more) would **linger** at
    its last version (RMT can't emit a tombstone here); a non-issue at this grain
    (keys don't vanish — spend/exposures are append-only), noted as a SCALING
    concern, not solved now.
  - A background refreshable MV is the SCALING alternative (note it; don't build
    it — determinism/testability favor the explicit versioned refresh now).
- **`report_snapshots`** — `(reported_at, campaign_id, period, <metrics>)`,
  **campaign-total grain** this phase: the `period` column is present but **fixed
  to a documented sentinel value** (e.g. the literal `'all'`, or the profile's
  sim-window label) — write it down so day-grain periods slot in later **without a
  schema change** (day-grain is deferred to the agent phase). One snapshot per
  refresh; `reported_at` data-derived (pre-reconcile `= max_hot_ingest`,
  post-reconcile `= reconciled_at`), so the two are distinct and ordered. RMT keyed
  `(reported_at, campaign_id, period)` so re-running a pass converges.
  - **Every metric column is serving-derived and truth-free** (truth-isolation
    invariant, N1): the four advertiser metrics (ROAS, CPA, CVR, site-visit rate —
    DECISIONS Phase 4) and their raw spend/revenue/conversions/purchases/exposures,
    plus an optional **match-rate = attributed conversions / total conversions**
    (computed from `attributed_conversions` FINAL alone — no truth). **No snapshot
    column is ever truth-derived** — recall/precision live only in the eval harness
    against `data/truth/`. A snapshot metric that needed recall would force truth
    into the DB and break `tests/test_truth_isolation.py`; do not add one.
- DDL added idempotently in `clickhouse/ddl.sql`; `clickhouse/apply.py` unchanged
  in shape.

## Reporting (`queries/`)

- **Restatement query** — per `(campaign_id)`, a **serving-layer** metric as of
  each `reported_at` from `report_snapshots`, so "as reported pre-reconcile vs now"
  is a two-row diff. This is the ARCHITECTURE "metric for period P as of time T"
  query at campaign-total grain. The metric is **ROAS** and/or **match-rate**
  (defined below) — **never recall** (truth-derived; stays in the eval harness).
- Report v1 (`queries/report.py`) is unchanged; the four metrics now read a state
  that includes reconciled rows (FINAL already collapses hot→reconciled).

## Open risks (called out, not fixed)

- **Reconciliation extends last-touch over 90d, so precision won't be 1.0
  post-reconcile.** An organic conversion that was hot-unattributed can now
  spuriously match a long-window exposure — the same last-touch over-credit
  property as the hot path, and it may dip precision **more** than the hot path,
  because 90d offers more exposures for an organic conversion to match. This is
  *consistent* behavior (not a bug) — the same truth-blind over-credit, at longer
  range.
  - **Disposition (do NOT gate the Done-when on precision).** Gate on (a) the
    caused long-delay conversions recovered — the recall/candidate **delta** — and
    (b) the restatement query showing the metric change between snapshots. **Have
    `make eval` report precision at both the hot-only state and post-reconcile**
    (the test flow scores accuracy after the hot pass and again after
    reconciliation), so the recall-for-precision trade is **visible**. That trade
    is the portfolio story — recovery buys recall at some precision cost — not a
    defect to hide. RESULTS/README states it honestly (BACKLOG honesty-boundary
    row).

## Determinism

- Reconciliation is a pure function of ClickHouse FINAL state + the fixed
  `LONG_WINDOW` / `RECONCILE_DELTA`: no wall clock, no entropy. Same state → same
  corrected rows and same `reconciled_at`.
- Rollup refresh and snapshots are deterministic recomputes over FINAL.
- Idempotency: replaying topics from offset 0 → same hot state → same
  reconciliation → same FINAL (versioned RMT keeps one row per key).

## Scope (files)

- `reconcile/__init__.py`, `reconcile/reconcile.py` — the pure matcher
  (`reconcile(candidates, exposures_by_household, window, reconciled_at) ->
  list[AttributedConversion]`, reusing `attribute_household`) + a `run` entry that
  reads ClickHouse FINAL, computes `reconciled_at`, writes corrected rows,
  refreshes the rollup, writes a snapshot. `python -m reconcile.reconcile`. No
  truth. `reconcile_` Prometheus metrics (candidates, recovered, still-missing).
- `clickhouse/ddl.sql` — add `campaign_hourly` + `report_snapshots`.
- `clickhouse/client.py` — read helpers: unattributed candidates (full resolved
  row), exposures-in-window-by-household, and a rollup/snapshot reader for tests.
- `queries/restatement.sql` + wiring in `queries/` — the two-snapshot diff.
- `Makefile` — `run` gains the reconciliation step **after** the engine (or a
  `reconcile` target invoked by `run`); `test-int-long-delay` target (isolated
  clean stack); `.PHONY` updated. `make report` unchanged. (Minor, non-blocking:
  there are now three per-profile int targets — `test-int`, `test-int-medium`,
  `test-int-long-delay`; a parameterized `test-int-profile PROFILE=x` would DRY
  them. Optional cleanup, not required for Phase 6.)
- `producer/profiles/long_delay.json` — new profile. `fixtures/tiny/` frozen,
  untouched.
- Unit tests (no services):
  - `tests/test_reconcile.py` — pure `reconcile()`: a >7d-delay conversion with an
    in-90d exposure is recovered (attributed, `path=reconciled`,
    `processed_at=reconciled_at`); one with no in-90d exposure stays unattributed
    and **no row is written for it** (so a second pass is a no-op); a
    **hot-attributed row (`attributed=1, path=hot`) is untouched** by a
    reconciliation pass (the WHERE-clause correctness constraint — assert its row
    is byte-identical after the pass); `reconciled_at > hot processed_at` and is
    stable across re-runs (fixed input set); idempotence (re-running yields
    identical rows).
  - `tests/test_rollup_snapshot.py` — versioned-replace refresh keeps one row per
    `(campaign, hour)` on FINAL; snapshot rows carry distinct ordered
    `reported_at`; restatement diff formatting.
  - A `long_delay` offline parity/count test (Phase-5 medium precedent): generate
    → resolve → hot engine (build_flow) → reconcile (pure) and pin the hot-miss
    count, recovered count, and post-reconcile recall.
- `tests/integration/test_reconcile.py` — live on the `long_delay` stack: hot run
  leaves the long-delay conversions unattributed; a reconciliation pass recovers
  them (FINAL `path=reconciled`, recall rises by the pinned count); a second pass
  is a no-op; the restatement query shows the pre/post metric change. **Capture
  precision both after the hot pass and after reconciliation** (via the eval
  harness) so the recall-up/precision-trade is asserted-visible, not gated. Opt-in
  (`make test-int-long-delay`).

## Review & stack risk

- **code-reviewer + functionality-tester** at the finish line (mandatory).
  **security-reviewer NOT triggered** — no CI-workflow, `.env`/credential,
  compose-exposure, ClickHouse-user, or agent/LLM-context changes (new DDL tables
  and a matcher module only). Noted explicitly rather than run for nothing.
- **coherence-auditor** at phase exit (mandatory) + BACKLOG review — the Phase-6
  long-delay-profile prerequisite row (now satisfied), the honesty-boundary row,
  and the co-view row are the ones to touch.
- **ClickHouse stack risk.** Versioned-replace rollup refresh and FINAL-on-rollup
  reads: verify RMT version semantics against the ClickHouse docs before working
  around anything; log surprises under ARCHITECTURE §8.

## Out of scope

- Naive-vs-optimized benchmark, Grafana/Alertmanager (Phase 7). `campaign_hourly`
  exists here but the benchmark that reads it vs a full scan is Phase 7.
- Day-grain restatement periods (agent phase); the `period` column is present but
  fixed at campaign-total now.
- Fault profiles, collectors, agent (Phase 8+). `long_delay` is a reconciliation
  profile, not a fault profile.
- Co-view read-time factor (BACKLOG). Continuous Kafka follow (still deferred).
