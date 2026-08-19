# Phase 8 — Fault harness and signal collectors

Contract for the `phase-8-fault-harness` branch. Source: `docs/PHASES.md` →
Phase 8, `docs/ARCHITECTURE.md` §4.1 (what the agent watches) / §4.2 (Observe:
the typed `AttributionContext`) / §4.3 (near-miss pair), and the BACKLOG rows due
here (15 co-view clamp, 20 shared-IP misattribution, 28 bench-on-new-profile).

Two deliverables, both gated: **five named fault profiles** as producer profiles,
and **deterministic collectors** that build a typed `AttributionContext` from
ClickHouse with **zero LLM calls**. No agent loop, no probes, no LLM — those are
Phase 9. This phase produces the reproducible faults and the observation object the
agent will later reason over.

## The two rulings this phase is built on (from the design review — do not re-litigate)

**Ruling A — `duplicate_flood` is a negative control, not a diagnosable fault.**
The duplicate injector re-appends a timestamp-identical payload (DECISIONS Phase 5),
the engine dedups it (full seen-set), and ReplacingMergeTree collapses any survivor —
so `attributed_conversions`/`exposures_landed` FINAL are **byte-identical whether the
flood happened or not**. A correctly-absorbed flood produces a *correct* number, and
the agent (§4) exists to catch numbers that are *probably wrong*. So duplicate_flood's
correct future agent output is **no-fault**, and Phase 10 scores it as a
**false-positive-rate control**, not a fault-recall case. Consequences pinned here:
- Collectors are **strictly ClickHouse-derived**. The dedup counter
  (`engine_dedup_suppressed_total`) stays a Prometheus/alert-plane concern (Phase 7),
  **never** a context field — no `.prom` side-channel into the collector (it would
  break "from ClickHouse", couple the collector to gitignored live-run artifacts, and
  give Phase 9 a field with no probe SQL behind it).
- The fault taxonomy is **labeled diagnosable vs control up front** (below), so Phase
  10 never tries to score "did the agent name duplicate_flood?".
- Escape hatch (recorded, not built): a diagnosable duplicate *bug* is
  **dedup-disabled + flood**, which inflates the ClickHouse tables and stays inside
  the from-ClickHouse model. The `.prom` side-channel is never that path.

**Ruling B — full §4.2 context now, shape frozen at Phase-8 exit.** Every §4.2 field
maps to a named §4.1 hypothesis the Phase-9 agent must rank; there are no speculative
fields. The `AttributionContext` pydantic shape is the **contract Phase 9 consumes**:
Phase 9 adds probe SQL + ranking over the frozen model and must NOT add or rename
fields — a genuinely-needed new field in Phase 9 is a STOP-and-report back-edit to
this contract, not routine churn. Two guards on over-reach:
- **Co-view stays a raw genre-reach stat.** Include raw, ClickHouse-derived
  exposures/attributed-conversions per genre — but NOT the co-view-*adjusted* factor
  (BACKLOG 26, deferred to the Phase-10 near-miss; "reporting never reads generation
  params"). Full co-view-inflation *detection* lands with the factor, not here.
- **Restatement volume comes from `report_snapshots`** (PRE vs FINAL, DB-derived),
  never the Prometheus `reconcile_restatement_roas_abs_delta` (alert-plane).

## Fault taxonomy (labeled up front — Ruling A coherence flag)

| Profile | Primary knob(s) | Class | §4.1 hypothesis it feeds |
|---|---|---|---|
| `shared_ip_spike` | `shared_ip_fraction`↑, `unknown_device_fraction`↑ | **diagnosable** | wrong-household / shared-IP matches |
| `late_burst` | `late.fraction`↑, `late.max_minutes`↑ (past 7d) | **diagnosable** | late-arrival distortion; window-edge |
| `co_view_bug` | `co_view_multiplier[sports]`↑ | **diagnosable @ Phase 10 (needs adjusted factor)** | co-view inflation — NOT discriminable from raw genre_reach (FG1); see below |
| `real_lift` | `caused_conversion_rate`↑, shared-IP low | **diagnosable** | real performance change |
| `duplicate_flood` | `duplicate_fraction`↑ | **control (benign)** | none — correct output is no-fault |
| (`medium` / no-fault) | — | **control** | none — the Phase-10 baseline (not built here) |

`co_view_bug` is a real fault (not a control like duplicate_flood), but its signal is
NOT discriminable from Phase-8/9 ClickHouse data alone (review-gate FG1, verified
live): raw `genre_reach` reads sports 0.561 vs comedy 0.522 — ~7% margin, comedy
un-boosted — because the organic baseline dilutes the caused-only 2.5× skew. It becomes
diagnosable only once the co-view-*adjusted* factor lands (Phase 10, BACKLOG 26), which
supplies the per-genre expected baseline. So the truth-side skew is asserted here (the
knob fires), NO raw-reach skew assertion is added (it would be flaky/false), and
DECISIONS Phase 8 flags the predictable Phase-9/10 back-edit to the frozen §4.2 shape.

The no-fault baseline is Phase 10's concern (`medium` is the interim clean reference);
this phase ships the five profiles above. `real_lift` and `shared_ip_spike` are the
**near-miss pair** (§4.3): both raise reported ROAS, only IP-cluster stats tell them
apart — so the collector must populate that discriminator, proven here, ranked in P9.

## BACKLOG rows due this phase

- **Row 20 (load-bearing).** `shared_ip_spike` MUST *engineer and observe* at least one
  caused, ambiguously-resolved conversion whose wrong-household candidate has a
  more-recent in-window exposure than its true household, so the most-recent-exposure
  reduction actually misattributes it. Proof: `caused_wrong_household ≥ 1`
  (equivalently `recall(household) < 1.0`) on this profile, **asserted** — the fault is
  observed, not assumed. Found by an offline seed search over the pure oracle, then
  pinned.
- **Row 15 (co-view clamp).** `generate.py`'s `min(1.0, rate)` clamp saturates
  silently. `co_view_bug` keeps `caused_conversion_rate × multiplier ≤ 1.0`
  (`0.2 × 4.0 = 0.8`) so the genre skew is **observable, not clamped** — disposition
  the clamp as left-as-is with that reason (no code change), don't just note it.
- **Row 28 (bench on a new profile).** `make bench` equality is empirically-true on
  long_delay, not a structural guarantee, and wrong-household rows are where the rollup
  refresh and `report.sql` could diverge. Before running bench on `shared_ip_spike`,
  verify the refresh semantics structurally OR add a bench run on it and confirm
  equality. (This phase does not add a bench gate; the row is dispositioned — verify
  or re-defer with the structural note.)

## DONE command

```
make down && make up && make seed PROFILE=shared_ip_spike && make run && \
make eval PROFILE=shared_ip_spike && make context PROFILE=shared_ip_spike && \
make test && make lint
```
(`make eval` / `make context` default to `PROFILE=tiny`, so both take the profile
explicitly — like the long_delay demo.)

- `make seed PROFILE=shared_ip_spike && make run`: the full pipeline (resolve → engine
  → reconcile) on the load-bearing profile. shared_ip_spike keeps delays in the hot
  window, so `make run` and `make run-hot` agree on the caused rows; `make run` is used
  so `report_snapshots` exists for the context's restatement field.
- `make eval`: prints the accuracy table; `caused_wrong_household ≥ 1` (Row 20 proof,
  asserted in the live integration test).
- `make context PROFILE=shared_ip_spike`: builds the typed `AttributionContext` from
  ClickHouse FINAL + `report_snapshots` and prints it — populated, pydantic-validated,
  zero LLM calls.
- `make test` (offline): pure collector unit tests (synthetic rows), the five
  fault-profile reproducibility + structural tests, `AttributionContext` schema tests.
- `make lint`; gate-0 tiny golden byte-identical (collectors + new profiles are
  read-only observers of a disjoint stack).

Live isolation (shared conversion_id space, DECISIONS Phase 5): `make test-int-shared-ip`
= clean `shared_ip_spike`-only stack (down → up → seed → run) proving the live context
build + the Row-20 misattribution assertion, mirroring `test-int-medium` /
`test-int-long-delay`.

## Done-when

1. **Five fault profiles run reproducibly.** Each `producer/profiles/<fault>.json`
   validates against the `Profile` schema and produces **byte-identical** output on two
   seeded runs (the Phase-1 determinism guarantee, extended per profile). Each is
   pinned by a structural offline test that asserts the fault is *present*
   (shared_ip_spike: `caused_wrong_household ≥ 1`; late_burst: hot-misses > 0;
   co_view_bug: sports conversions-per-exposure materially above the flat genres and
   below saturation; real_lift: caused-conversion count above the medium baseline with
   `caused_wrong_household == 0`; duplicate_flood: dedup fires AND FINAL-equivalent row
   set is unchanged vs its dedup-off self — the control invariant).
2. **`AttributionContext` populated, typed, no LLM.** The collector builds the full
   §4.2 object from ClickHouse (match rate overall + over time, per-campaign metrics,
   per-campaign restatement deltas, window-edge lag distribution, shared-IP/ambiguous
   cluster stats, raw genre reach) with zero LLM calls; pure aggregation functions are
   unit-tested with synthetic rows; a live integration test proves the ClickHouse read
   path on shared_ip_spike.
3. Gate 0, `make test`, `make lint` green.

## `AttributionContext` shape (frozen at phase exit — Ruling B)

All fields ClickHouse-derived (N1: truth is never read, never in the DB). Nested
pydantic models in `agent/context.py`:

- `profile: str`, `processed: int`, `attributed: int`, `match_rate: float | None`
- `match_rate_by_day: list[MatchRatePoint]` — (`day`, `processed`, `attributed`,
  `match_rate`) → real-lift-vs-inflation, upstream-data-change
- `campaigns: list[CampaignMetrics]` — reuse `report.sql` columns (spend, exposures,
  conversions, purchases, revenue, roas/cpa/cvr/site_visit_rate, nullable) → real change
- `restatements: list[CampaignRestatement]` — reuse `restatement.sql` over
  `report_snapshots` (roas_as_reported/roas_now/roas_delta, conversions delta,
  revenue_delta) → late-arrival distortion
- `window_edge: WindowEdgeStats` — attribution lag (`conv.event_time − exp.event_time`
  via `exposure_id` join to `exposures_landed`) bucketed over the 7d window + a
  near-boundary count → window-edge effects
- `ip_clusters: IpClusterStats` — `resolution='ip'` / `ambiguous=1` attributed counts,
  max `candidate_count`, top shared IPs by attributed-conversion count → wrong-household
- `genre_reach: list[GenreReach]` — raw exposures / attributed-conversions /
  conversions-per-exposure per `program_genre` (NOT co-view-adjusted) → co-view inflation

Structure mirrors `accuracy/`: pure `agent/collect.py` (aggregation functions over
already-fetched rows, no I/O, no clock) + readers in `agent/readers.py` (parameterized
SQL) + `agent/run_context.py` (`make context` entrypoint: readers → pure funcs → print).

## Scope (files)

- `producer/profiles/{shared_ip_spike,late_burst,co_view_bug,real_lift,duplicate_flood}.json`
  (5 new). No producer *code* change (Row 15 clamp left as-is, dispositioned).
- `agent/__init__.py`, `agent/context.py` (models), `agent/collect.py` (pure aggregators),
  `agent/readers.py` (ClickHouse SQL readers), `agent/run_context.py` (`make context`).
- `Makefile`: `context` target, `test-int-shared-ip` target.
- Tests: `tests/test_collect.py` (pure, synthetic rows), `tests/test_context_schema.py`,
  `tests/test_fault_profiles.py` (5-profile reproducibility + structural),
  `tests/integration/test_context.py` (live shared_ip_spike).
- Records: this spec, `DECISIONS.md` (Phase 8 block: Ruling A control framing, Ruling B
  freeze + co-view-raw + restatement-source, the seed-search pins, the fault knob
  choices), `BACKLOG.md` (15 dispositioned, 20 done, 28 dispositioned), `CLAUDE.md`
  status + `make context` / `make test-int-shared-ip` in Commands.

## Review & stack risk

- **code-reviewer + functionality-tester** at the finish line (mandatory).
- **security-reviewer TRIGGERED?** — No CI/.env/credential/compose-exposure/ClickHouse-
  user/agent-LLM-context change (the collector is deterministic SQL, no LLM boundary
  yet — that's Phase 9). Run it anyway only if the collector readers touch connection
  config; default is code-reviewer + functionality-tester.
- **coherence-auditor** at phase exit (mandatory) + BACKLOG review (15, 20, 28 due).

## Out of scope (deferred, recorded)

- The agent loop, hypothesis catalog, probe registry, `AttributionFinding`, webhook →
  Phase 9. This phase stops at the populated context.
- No-fault baseline profile + the near-miss *demo* + false-positive table → Phase 10.
- Co-view-adjusted factor → BACKLOG 26 (Phase 10). Only raw genre reach here.
- A `make bench` gate on shared_ip_spike → Row 28 dispositioned (verify-or-re-defer),
  not added as a phase gate.

## Phase-9 forward-notes (review-gate deferrals, no Phase-8 action)

- **SN1 (prompt-injection surface).** Context string fields (`ip`, `genre`,
  `campaign_id`, top-cluster IPs) are attacker-influenceable data. When Phase 9 builds
  the LLM prompt they MUST reach the model as delimited/structured data, never spliced
  into instruction text. BACKLOG row added; trigger = Phase-9 prompt authoring.
- **SN2 + CA-Q4 (SELECT-only must cover the collector read path).** The Phase-9
  SELECT-only ClickHouse user + write-denied test must cover `run_context.py` /
  `agent/readers.py` (`clickhouse.client.connect()`), not only the probe registry —
  else the read-only guarantee holds on probes but not on the collectors. Fold into the
  Phase-9 DB-user work; no new BACKLOG row.
- **CA-minor (`profile` is a label, not a filter).** `AttributionContext.profile` is a
  caller-supplied label; the serving tables have no `profile` column (mitigated by
  clean single-profile stacks, consistent with `make eval`/`report`). The Phase-9 agent
  must never treat `profile` as ground truth about which rows it read.
