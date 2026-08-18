# Phase 4 — Accuracy eval and reporting v1 · CHECKPOINT

Contract for the `phase-4-eval-reporting` branch. Source: `docs/PHASES.md`
→ Phase 4, `docs/ARCHITECTURE.md` §3.3 "Reporting" / §4.3 "How it's proven",
DECISIONS.md (Phase 4). Pre-spec record corrections (household grain, N1
side-file join, tiny-is-organic-over-credit, co-view deferral) are already
merged into those docs.

Two deliverables, independent: an **accuracy eval** (`make eval`) and a
**reporting v1** (`make report`). Both read ClickHouse FINAL state; the eval
additionally joins the truth side file **in the harness** (truth never enters
the DB — N1, DECISIONS Phase 4).

## DONE command

```
make down && make up && \
make seed PROFILE=tiny && make run && \
make eval PROFILE=tiny && make report && \
make test && make lint
```

Passes when, on a clean compose cycle over the frozen tiny fixture:

- `make eval` prints an accuracy table with **household-grain precision 0.673
  (35/52) and recall 1.000 (35/35)**, plus the **labeled exact-`exposure_id`
  diagnostic 0.058 (3/52)** marked "last-touch → causal coincidence; not an
  accuracy measure."
- `make report` prints the four metrics (ROAS, CPA, CVR, site-visit rate) per
  campaign for the 3 tiny campaigns, with **NULL** (not a crash, not 0/0) on any
  zero denominator.
- `make test` and `make lint` are green.

The headline numbers are additionally pinned by an **offline** unit test
(scoring the frozen fixtures with no services) and re-verified live by an
opt-in integration test (`make test-int`), which CI's integration job runs.

## What the eval does (ARCHITECTURE §4.3, DECISIONS Phase 4)

Scores the engine's attribution against ground truth at **household grain** —
the engine is last-touch, so it credits the most-recent in-window exposure, not
necessarily the causal one; exact-`exposure_id` equality would measure
coincidence, not attribution quality (DECISIONS Phase 4).

Three inputs, joined in the harness:

1. **Credited rows** — `attributed_conversions` FINAL where `attributed = 1`:
   `conversion_id → (household_id, exposure_id)`. This is the engine's decision.
2. **Exposure→household** — `exposures_landed` FINAL: `exposure_id →
   household_id`. Used to map a truth exposure to its household.
3. **Truth links** — `data/truth/<profile>/truth_links.jsonl` **side file**
   (never loaded into ClickHouse): `conversion_id → truth_exposure_id`. Present
   only for caused conversions; organic conversions have no entry.

Definitions (DECISIONS Phase 4):

- A credited conversion is **correct** when it is caused (has a truth link) AND
  the engine's `household_id` equals the household of its `truth_exposure_id`.
- **precision** = correct / all credited (`attributed=true`) conversions.
- **recall** = correct / all truth links.
- **exact-id diagnostic** (labeled, never the headline) = credited rows whose
  `exposure_id` equals `truth_exposure_id` / all credited.

The scoring is a pure function `score(credited, truth, exposure_household)`; no
I/O, no clock, unit-testable without services. The harness (`accuracy/run.py`)
does the reads and printing.

## What the report does (ARCHITECTURE §3.3 "Reporting")

Four advertiser metrics per campaign, read from the raw serving tables (the
`campaign_hourly` rollup and the naive-vs-optimized benchmark are Phase 6/7).
Campaign is obtained by joining each credited conversion's `exposure_id` to
`exposures_landed.campaign_id`.

Per campaign (metric definitions — DECISIONS Phase 4, option 1):

- **ROAS** = attributed revenue / spend.
- **CPA**  = spend / attributed **purchases** (acquisition = purchase; keeps CPA
  an independent signal from CVR — DECISIONS Phase 4).
- **CVR**  = attributed conversions / exposures.
- **site-visit rate** = attributed **site_visit** conversions / exposures.

Load-bearing rules (DECISIONS Phase 4):

- **Read FINAL on BOTH tables.** `exposures_landed` and `attributed_conversions`
  are ReplacingMergeTree; a plain `sum`/`count` over unmerged parts counts
  duplicate landings and pre-reduction rows and silently inflates the spend and
  exposure denominators. Spend/exposures from `exposures_landed FINAL`;
  conversions/revenue from `attributed_conversions FINAL WHERE attributed = 1`.
  This is what makes the Phase-3 RMT choice for `exposures_landed` pay off.
- **NULL on zero denominators**, uniformly via `nullIf(denominator, 0)` — a
  campaign with no purchases (CPA), no spend (ROAS), or no exposures (CVR)
  yields NULL, never a divide-by-zero or a crash.
- **Do NOT filter wrong-household attributions.** An ambiguous shared-IP
  conversion credited to a campaign counts toward that campaign's metrics even
  when truth disagrees — that is the advertiser's reported number, and the
  divergence is measured separately by `make eval`. A subtly-inflated ROAS is
  exactly the "plausible-but-wrong number" the Phase-9 agent will diagnose; it
  is a feature to preserve, not a filter to add.

## Truth isolation (N1)

`make eval` reads truth ONLY from the side file, ONLY in `accuracy/` (outside
the pipeline dirs the isolation guard `tests/test_truth_isolation.py` checks:
`resolve`, `streaming`, `reconcile`, `clickhouse`, `queries`). The report
(`queries/`, a pipeline dir) never touches truth. No truth is loaded into
ClickHouse. The isolation test must stay green; extend `PIPELINE_DIRS` only if
a new pipeline dir is added (it is not).

## Determinism

- Eval scoring is a pure function of (credited, truth, exposure_household); same
  fixture → identical numbers. No wall clock, no entropy.
- Report is deterministic SQL over FINAL state; same ClickHouse state → same
  table.
- Both read FINAL, so replays/duplicates/corrections read as one row
  (idempotency contract).

## Scope

- `accuracy/__init__.py`, `accuracy/score.py` — pure `score(...)` returning a
  pydantic `AccuracyReport` (profile, credited, truth_links, household_correct,
  precision, recall, f1, exact_exposure_correct, exact_exposure_match_rate,
  organic_credited, caused_missed, caused_wrong_household), plus a
  `format_report(report) -> str` for the printed table. No I/O.
- `accuracy/run.py` — `main(argv)`: `--profile` (default `tiny`); read credited
  + exposure→household from ClickHouse FINAL, read truth side file
  `data/truth/<profile>/truth_links.jsonl`, `score`, print `format_report`.
  `python -m accuracy.run`. The ONLY module that reads truth outside `producer/`
  / `agent/eval/` / `tests/`.
- `queries/__init__.py`, `queries/report.sql` — one query returning per-campaign
  spend, exposures, conversions, purchases, revenue, roas, cpa, cvr,
  site_visit_rate; FINAL on both tables; `nullIf` on every denominator; SQL
  keywords lowercase, one column per line.
- `queries/report.py` — load `report.sql`, execute via
  `clickhouse.client.connect`, format + print a per-campaign table. No truth
  (pipeline dir). `python -m queries.report`.
- `clickhouse/client.py` — add read helpers `read_credited(client)` →
  `{conversion_id: (household_id, exposure_id)}` and
  `read_exposure_households(client)` → `{exposure_id: household_id}`, both over
  FINAL. No truth (pipeline dir).
- `Makefile` — `eval` target (`python -m accuracy.run --profile $(PROFILE)`);
  `report` target (`python -m queries.report`). Add both to `.PHONY`. `make run`
  unchanged.
- Unit tests (no services):
  - `tests/test_accuracy.py` — pure `score()`: correct-household, wrong-household
    (caused misattribution), organic-credited FP, caused-but-unattributed
    (missed); and a fixture-pinned case reading
    `fixtures/tiny/expected/attributed.jsonl` +
    `fixtures/tiny/exposures.jsonl` + `fixtures/tiny/truth_links.jsonl` →
    asserts precision 0.673 (35/52), recall 1.000 (35/35), exact-id diagnostic
    3/52, organic_credited 17, caused_wrong_household 0.
  - `tests/test_report.py` — `format_report`/report formatting over synthetic
    rows incl. NULL denominators; assert `report.sql` reads FINAL on both tables
    and uses `nullIf` on each ratio (guards the load-bearing rules as text).
- `tests/integration/test_eval_report.py` — against `make up`: seed → resolve →
  engine, then (a) score via ClickHouse reads == the pinned fixture numbers, and
  (b) run `report.sql`, assert the 3 tiny campaigns are present, the four
  metrics computed, and NULL appears where a denominator is zero (if any).
  Opt-in (`make test-int`); the CI integration job already runs `make test-int`.

## Expected tiny numbers (pinned)

- Credited (attributed=true) conversions: **52**. Truth links: **35**.
- Household-correct: **35** → precision **0.673** (35/52), recall **1.000**
  (35/35). caused_missed **0**, caused_wrong_household **0**.
- Organic credited (last-touch over-credit, no truth link): **17** — the sole
  driver of tiny's sub-1.0 precision (DECISIONS Phase 4; shared-IP
  wrong-household is a Phase-8 fault-profile story, not tiny).
- Exact-`exposure_id` diagnostic: **3/52 = 0.058** (labeled, not the headline).

Report — 3 campaigns `camp-00/01/02` (producer generates `camp-{i:02d}`,
zero-indexed, `n_campaigns=3`). The integration test **pins these values** (a
wrong-ratio SQL bug that still used FINAL + `nullIf` would otherwise pass a
shape-only check; a silently-wrong ROAS is the exact deliverable the Phase-9
agent diagnoses). Report is SQL-only — no Python metric core to duplicate the
SQL and risk divergence — so the pin lives in the integration test:

| campaign | spend | exposures | attr conv | purchases | revenue | ROAS     | CPA    | CVR    | site-visit rate |
|----------|-------|-----------|-----------|-----------|---------|----------|--------|--------|-----------------|
| camp-00  | 4.48  | 47        | 16        | 7         | 431.99  | 96.4263  | 0.6400 | 0.3404 | 0.1915          |
| camp-01  | 4.86  | 56        | 24        | 9         | 626.71  | 128.9527 | 0.5400 | 0.4286 | 0.2679          |
| camp-02  | 4.36  | 47        | 12        | 5         | 324.14  | 74.3440  | 0.8720 | 0.2553 | 0.1489          |

Totals check: 47+56+47 = 150 exposures, 16+24+12 = 52 credited — consistent with
the pinned eval fixture. Ratios asserted with tolerance (float round-trip); the
test compares to these targets, not just shape. tiny does **not** exercise the
zero-denominator NULL path — every campaign has purchases — so the synthetic
`tests/test_report.py` case is the sole guard of NULL-on-zero-denominator.

## Review & stack risk

- **code-reviewer + functionality-tester** at the finish line (mandatory,
  CLAUDE.md review gate). **security-reviewer** is NOT triggered: no CI-workflow,
  `.env`/credential, compose-exposure, ClickHouse-user, or agent/LLM-context
  changes this phase (the CI integration job already exists from Phase 3). Note
  it explicitly in the report rather than running it for nothing.
- **coherence-auditor** at phase exit (mandatory, before the PR merges), plus a
  BACKLOG review — the Phase 8 shared-IP-fault row and the co-view-deferral row
  added this phase are the new items; neither is due now.
- **Truth-isolation risk.** The eval reads truth. Keep every truth reference in
  `accuracy/` (and `tests/`); `tests/test_truth_isolation.py` must stay green.

## Out of scope

- `campaign_hourly` rollup + scheduled refresh, `report_snapshots`, restatement
  view (Phase 6); naive-vs-optimized benchmark harness (Phase 7).
- Co-view read-time genre factor — **deferred** (BACKLOG; semantic trigger =
  when reporting adds genre-adjusted metrics for the agent/demo narrative, or
  Phase 7, whichever first). Stays a read-time factor, never storage/rollups;
  when it lands it must be a reporting-side config input decoupled from the
  producer profile.
- Agent, fault profiles, `medium` profile. Engine/resolve/producer code
  untouched. Fixtures stay frozen read-only.
