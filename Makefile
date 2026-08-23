# Later phases add: bench, agent-run, agent-eval (see CLAUDE.md → Commands).

.PHONY: setup up down seed resolve run run-hot lake-reset replay-serving lake-maintain reconcile-dagster dagster-ui scale-curve eval report restate bench cost-levers rollup-bench cost-report context agent-run agent-eval metrics-capture test-alerts test test-int test-int-medium test-int-long-delay test-int-shared-ip test-int-agent test-int-lakehouse check-docs lint review-gate mutate

PROFILE ?= tiny
# resolve replay input: fixtures/<profile> or out (data/out/<profile>). Keep the
# comment on its own line: make keeps the whitespace before an inline `#` as part
# of the value, which turned `--source "fixtures  "` into an argparse error.
SOURCE ?= fixtures

# make exports a variable to every recipe's environment and expands it to do so —
# a command-line origin always, and an environment origin too on GNU Make ≥ 4 — so
# `PROFILE='$(shell touch x)' make lake-reset` runs the `$(shell …)` before any
# recipe or Python guard. The $(value)/_Q quoting below cannot stop that (it guards
# the recipe TEXT, not the export), and `make -n` never shows it (it sets up no
# recipe environment). `unexport` removes make's reason to expand these: no recipe
# reads any of them as a shell variable ($$VAR), only as a make value via
# `$(call _Q,$(value VAR))` (or, for CONFIRM, `$(value CONFIRM)` in _YES), so
# unexporting is behaviour-preserving and closes the startup vector on both origins.
# Command-line values still reach recipes and sub-makes (MAKEFLAGS is unaffected).
# (fix/make-quote-profile; tests/test_makefile.py.) MAKEFLAGS/MAKEOVERRIDES from the
# environment stay a stated residual — mistakes, not adversaries (DECISIONS Phase 17).
unexport PROFILE SOURCE PARTITION SPEC BASE DELETED CONFIRM

# Load ANTHROPIC_API_KEY from .env for the token targets ONLY (agent-run,
# agent-eval). `uv run --env-file` injects into the child process it spawns, not
# the interactive shell, and uv's precedence keeps an already-exported key (CI, a
# future secret) authoritative over the file. `$(wildcard .env)` → no .env, no
# flag, so a fresh clone that exports the key still runs. Scoped to these two
# recipes; NOT a global `include .env` (which would leak every var into every
# recipe). .env is gitignored. Security-reviewed (BACKLOG 34).
ENV_FILE  := $(wildcard .env)
AGENT_ENV := $(if $(ENV_FILE),--env-file .env,)

# Digest-pinned prometheus image (must match docker-compose.yml) — promtool ships
# inside it, so the alert-rule tests need no new dependency and no floating tag.
PROM_IMAGE = prom/prometheus:v3.1.0@sha256:6559acbd5d770b15bb3c954629ce190ac3cbbdb2b7f1c30f0385c4e05104e218

setup:
	uv sync
	uv run python -m lake.install_extension  # duckdb iceberg ext (network); tests load-only
	uv run pre-commit install

up:
	docker compose up -d --wait

# The first sanctioned destructive path: removes containers AND volumes. Does
# NOT touch data/lake/ (the lake is the record; see lake-reset).
down:
	docker compose down -v

# The three destructive paths — lake-reset, replay-serving, lake-maintain — are
# ONE Python process each (lake/destructive.py): validate the profile, derive the
# root from it (no path argument exists to escape with), prompt on a tty, then
# act. Make never interpolates a user value into a guard and never splits guard
# and action across shells (rounds 2 and 3 of the Phase-17 review each found a
# hole in a Make-level guard; `make -i` cannot step inside a process). The profile
# reaches that process single-quoted and UNEXPANDED (`$(call _Q,$(value PROFILE))`,
# below), and PROFILE is `unexport`ed (top), so a `PROFILE='$(shell …)'` from the
# command line or environment never runs the shell — at recipe time OR make
# startup. Both were Phase-17 residuals, closed in fix/make-quote-profile.
#
# CONFIRM (the confirm knob) carries the SAME vector and is closed the same way:
# `--yes` only when CONFIRM is EXACTLY `yes` from the command line — `$(origin)`
# ignores an exported CONFIRM; `$(value CONFIRM)` (not `$(CONFIRM)`) so a
# command-line `CONFIRM='yes $(shell …)'` reaches the filter as TEXT (no
# expansion), and the `$(words …) = 1` guard rejects it (it is `yes` PLUS
# something), so it neither runs the shell nor auto-confirms; and CONFIRM is
# `unexport`ed (top) so the same `$(shell …)` cannot run at make startup either.
# Surviving stated residual: MAKEFLAGS/MAKEOVERRIDES from the environment — these
# guards are for mistakes, not a user who controls the environment (DECISIONS
# Phase 17).
_YES = $(if $(filter command line,$(origin CONFIRM)),$(if $(filter yes,$(value CONFIRM)),$(if $(filter 1,$(words $(value CONFIRM))),--yes,),),)

# _Q: single-quote a value for sh — the ONLY character that needs escaping inside
# '…' is ' itself, so `'"; echo x; "'` reaches Python as one literal argument (a
# bare "$(VAR)" interpolation would run the echo). Callers pass `$(value VAR)`, the
# UNEXPANDED text: make expands a variable before _Q ever sees it, so a
# `VAR='$(shell …)'` from the environment would otherwise run at recipe-expansion
# time — even under `make -n`, which is NOT a dry run of a variable's value (the
# value's own functions still expand). PROFILE / SOURCE / PARTITION and SPEC /
# BASE / DELETED all go through it, so no user value reaches sh expanded
# (fix/make-quote-profile; security review, PR #35 round 1).
_Q = '$(subst ','\'',$(1))'

# Delete this PROFILE's lake of record, data/lake/<profile> (spec D9). The
# clean-stack test-int-* targets pass CONFIRM=yes: a "clean stack" for a profile
# means a clean lake too, since the lake outlives `make down` and the serving
# tables are loaded from it.
lake-reset:
	uv run python -m lake.destructive reset --profile $(call _Q,$(value PROFILE)) $(_YES)

# Deterministic per PRODUCER_SEED (default: profile's seed).
seed:
	uv run python -m producer.seed --profile $(call _Q,$(value PROFILE))

# Offline resolve replay (service-free): device→household, IP fallback, fan-out.
# Writes data/out/<profile>/conversions_resolved.jsonl. The unit proof of the
# resolve step the engine runs in-process (Phase 16).
resolve:
	uv run python -m resolve.replay --profile $(call _Q,$(value PROFILE)) --source $(call _Q,$(value SOURCE))

# Phase 17: the lake is the record. One lake per PROFILE (profiles share
# conversion_id space — the same isolation `make down` gives ClickHouse, without a
# destructive step): engine and reconcile land under data/lake/<profile>/ (bound by
# each entry point's --profile) and the Dagster load reads it back. Each
# clean-stack test-int-* target pins `PROFILE` target-wide so its pytest line
# (run by the parent make) binds the same lake as the `$(MAKE) run PROFILE=<p>`
# child.
# No LAKE_ROOT here: every Python entry point takes `--profile` and binds its own
# lake (lake.iceberg_catalog.configure → data/lake/<profile>); LAKE_ROOT in the
# environment is test-only (tmp fixtures) and refused outside pytest.

# Live pipeline over the seeded stream: attribution engine (resolve in-process →
# hot join) → lake → Dagster load → ClickHouse, then the reconciliation pass
# (recovers long-window misses AND the deferred shared-IP conversions → lake →
# reload → rollup refresh + pre/post report snapshots). Run after `make up &&
# make seed`. Every row in ClickHouse arrived through the lake (Phase 17).
# LAKE_ASYNC_INSERT=1 (Phase 18b): the loader batches server-side into fewer,
# larger parts (async_insert=1, wait_for_async_insert=1). ON only here, off in the
# golden/oracle/capture paths so their pins never move for a batching reason. Both
# load-bearing lines opt in (dataflow's load and the reconcile reload).
run:
	LAKE_ASYNC_INSERT=1 uv run python -m streaming.dataflow --profile $(call _Q,$(value PROFILE))
	LAKE_ASYNC_INSERT=1 uv run python -m reconcile.reconcile --profile $(call _Q,$(value PROFILE))
	uv run python -m observability.ch_scrape

# Hot path only (engine, NO reconciliation). Used by the hot-path
# oracle suites — the frozen tiny golden and pinned tiny accuracy (Phase 3/4),
# and the medium hardening proof — which assert the engine's hot output; a
# reconciliation pass would over-credit their long-tail organics and shift those
# pins. Reconciliation is proven on its own profile (`make test-int-long-delay`).
run-hot:
	uv run python -m streaming.dataflow --profile $(call _Q,$(value PROFILE))
	uv run python -m observability.ch_scrape

# Phase 12/17: orchestrated reconciliation. Materialize the day-partitioned
# reconciled_conversions asset (exposures sourced from Iceberg via DuckDB,
# corrections appended to the lake) over the candidate days, reload the touched
# days, then the finalize asset — headless (no webserver, ephemeral Dagster
# instance). PROFILE selects the lake; PARTITION=<YYYY-MM-DD> materializes a
# single day. Run after make run-hot.
reconcile-dagster:
	uv run python -m orchestration.run reconcile --profile $(call _Q,$(value PROFILE)) $(if $(value PARTITION),--partition $(call _Q,$(value PARTITION)),)

# Phase 17: replay the serving layer FROM THE LAKE — no Kafka involvement. Drops
# the rows of exposures_landed + attributed_conversions + eval_meta (TRUNCATE — destructive,
# so CONFIRM=yes or the tty prompt), reloads every day the lake holds (hot AND
# reconciled current rows), stamps eval_meta in the SAME process (one recipe
# line — `make -i` cannot re-stamp after a refusal); `make eval` then reproduces the
# pins. The backfill story: Kafka retention is hours, the lake is forever.
replay-serving:
	uv run python -m lake.destructive replay --profile $(call _Q,$(value PROFILE)) $(_YES)

# Phase 17 (spec D10): lake hygiene as a Dagster job — expire snapshots older
# than LAKE_SNAPSHOT_MAX_AGE_DAYS (default 7) and rewrite each day partition
# that has accumulated more than one file per bucket into one file per bucket.
# Not part of make run. Row content is unchanged, data files are REWRITTEN (a
# mutation of the record, so it prompts like the other two; asserted offline on
# both raw tables). Expiry is metadata-only on pyiceberg 0.11.1 (BACKLOG 45).
lake-maintain:
	uv run python -m lake.destructive maintain --profile $(call _Q,$(value PROFILE)) $(_YES)

# Phase 12/17 (optional, dev only): the Dagster asset-graph viewer; materialize
# works for the ONE profile bound by DAGSTER_PROFILE (= PROFILE — there is no
# default lake root, so an unbound code location only renders the graph). Bound
# to loopback (-h 127.0.0.1) — never published, never 0.0.0.0. DAGSTER_HOME under
# gitignored data/ so instance sqlite + run logs never touch the repo (carries no
# secrets). Not needed for make reconcile-dagster (headless, ephemeral instance).
# A containerized/published webserver is a deployment lever, not built.
dagster-ui:
	mkdir -p data/dagster_home
	DAGSTER_PROFILE=$(call _Q,$(value PROFILE)) DAGSTER_HOME=$(PWD)/data/dagster_home uv run dagster dev -m orchestration.definitions -h 127.0.0.1 -p 3000

# Measured scaling curve (offline, no compose): drain the engine over tiered event
# counts (1k/10k/100k exposures resident in the hot window), report the STRUCTURAL
# per-exposure state cost (deterministic — deep sys.getsizeof of retained state ÷
# entries), engine_join_state_current occupancy, and a tracemalloc cross-check, then
# rewrite the measured-constant block in docs/SCALING.md. Occupancy (state size),
# not throughput. In-process over the scale_curve profile, same idiom as the oracle
# suites — no `make up` (Phase 14).
scale-curve:
	uv run python -m streaming.scale_probe

# Naive-vs-optimized reporting benchmark: the same four-metric question run as a
# full FINAL scan of the raw serving tables (report.sql) vs the pre-aggregated
# campaign_hourly rollup (bench.sql). Prints latency, rows read, bytes read for
# each. Run after `make run` populated the rollup (e.g. seed long_delay && run).
bench:
	uv run python -m queries.bench

# Query cost levers (Phase 13): three before/after measurements on the report
# query over the bench_large serving tables — a projection ordered by event_time
# (WINS), a FINAL-avoidance / bloom-skip-index candidate (documented NEGATIVE
# result — the schema doesn't reward one), and PREWHERE (WINS). Reuses
# bench_common.py's canonicalization + summary reader; asserts direction (winners read fewer bytes;
# the negatives are asserted NOT to help) and identical result rows; rewrites the
# "Query cost levers" block in docs/RESULTS.md. Live-stack: run after
# `make lake-reset PROFILE=bench_large CONFIRM=yes && make up && make seed
# PROFILE=bench_large && make run PROFILE=bench_large` (a clean lake + the same
# PROFILE on every step — the engine binds its lake from --profile).
cost-levers:
	uv run python -m queries.measure_levers

# Rollup refresh: full rebuild vs dirty-set refresh (Phase 18a), on a populated
# stack. Asserts the two leave campaign_hourly FINAL identical (6dp), that the
# incremental refresh WRITES fewer rows (direction only — rows read are printed with
# the granule counts that explain why they do NOT fall at this size), and the
# dirty-set gate: every key whose rollup row changed is in the dirty set above the
# rollup recomputed. Rewrites the "Rollup refresh" block in docs/RESULTS.md. ONE
# python process: it validates PROFILE ([a-z0-9_]+) and refuses a DB populated from
# another profile (the eval_meta marker) BEFORE it touches anything. PROFILE is never
# a path. It is not read-only: it applies the DDL (including the report_snapshots
# migration on an unmigrated stack) and creates + drops two scratch tables of its own
# — the live rollup is never its write target, which is what makes its equality check
# an oracle rather than a self-comparison. Run after `make run PROFILE=<p>` on a profile
# whose reconcile pass restates something (long_delay).
rollup-bench:
	uv run python -m queries.rollup_bench --profile $(call _Q,$(value PROFILE))

# Phase 18b: per-query cost from system.query_log. Tags each report/restate/bench
# query with a distinct log_comment, reads its cost back (as cost_rw), writes
# query_cost_daily, and rewrites the "Cost per report query" block in docs/RESULTS.md.
# Refuses a profile/DB mismatch via the eval_meta marker (BACKLOG 43). Run after
# `make run PROFILE=<p>` populated the serving tables.
cost-report:
	uv run python -m queries.cost_report --profile $(call _Q,$(value PROFILE))

# Dump each stage's terminal Prometheus registry from a REAL run (the provenance
# of the promtool alert fixtures). A CLEAN-STACK capture: `make down && make
# lake-reset PROFILE=<p> CONFIRM=yes && make up && make seed PROFILE=<p> && make
# metrics-capture PROFILE=<p>` — over a populated lake the reconcile candidates
# are the lake's CURRENT rows, so a second capture sees zero and the numbers
# differ. The fixtures are recaptured in Phase 18 (alert rules).
metrics-capture:
	mkdir -p data/out/$(call _Q,$(value PROFILE))/metrics
	uv run python -m streaming.dataflow --profile $(call _Q,$(value PROFILE)) --metrics-out data/out/$(call _Q,$(value PROFILE))/metrics/engine.prom
	uv run python -m reconcile.reconcile --profile $(call _Q,$(value PROFILE)) --metrics-out data/out/$(call _Q,$(value PROFILE))/metrics/reconcile.prom
	uv run python -m observability.ch_scrape --metrics-out data/out/$(call _Q,$(value PROFILE))/metrics/clickhouse.prom

# Attribution accuracy (household grain) vs the truth side file, for the given
# PROFILE (default tiny). Reads attributed_conversions FINAL from ClickHouse;
# truth never enters the DB (N1, DECISIONS Phase 4). Refuses a profile/DB
# mismatch via the eval_meta marker the populate path writes (BACKLOG 43).
eval:
	uv run python -m accuracy.run --profile $(call _Q,$(value PROFILE))

# The four advertiser metrics per campaign, from the raw serving tables.
report:
	uv run python -m queries.report

# Restatement: each campaign's metric as reported pre-reconciliation vs now, and
# the change reconciliation caused (report_snapshots FINAL). Run after `make run`.
restate:
	uv run python -m queries.restatement

# Build and print the typed AttributionContext (ARCHITECTURE §4.2) from ClickHouse
# — the deterministic, LLM-free observe step (Phase 8). Reading it back proves it
# is populated and pydantic-valid. Serving layer only (N1); the causal side file is
# never read. Run after `make run`. The agent loop that reasons over it is Phase 9.
context:
	uv run python -m agent.run_context --profile $(call _Q,$(value PROFILE))

# Run the attribution-integrity agent once, end to end, against the live stack
# (Phase 9). This is the ONLY path that calls the LLM — it costs API tokens, so ask
# the developer before running (CLAUDE.md). Reads as the SELECT-only agent_ro user;
# emits a typed AttributionFinding + the turn-2 cache_read (Rulings B/E). Run after
# `make run` populated the serving tables. Pass PROFILE explicitly (like context/eval);
# the Phase-9 Done-when uses PROFILE=shared_ip_spike.
agent-run:
	uv run $(AGENT_ENV) python -m agent.run_agent --profile $(call _Q,$(value PROFILE))

# Phase-10 fault->diagnosis sweep: every fault profile + the no-fault baseline, run
# EVAL_REPS times, scored against the pure rubric, both tables written to
# docs/RESULTS.md. Drives its own clean stack + lake per scenario
# (down/lake-reset/up/seed/run — profiles
# share conversion_id space, DECISIONS Phase 5), so run it on a free machine, not over a
# stack you want to keep. This is the ONLY eval path that calls the LLM — it costs API
# tokens (30 invocations, well under $10), so ask the developer before running (CLAUDE.md).
agent-eval:
	uv run $(AGENT_ENV) python -m agent.eval.run_eval

# Offline: no broker/ClickHouse. --ignore keeps the integration suite (which
# would probe services before skipping) from making any network attempt.
test:
	uv run pytest --ignore=tests/integration

# Integration tests against the running compose stack (`make up` first). Tiny
# only — the medium hardening test, the long_delay reconciliation test, and the
# long_delay lakehouse test each need a clean single-profile stack (profiles share
# conversion_id space; DECISIONS Phase 5), so they are excluded here and run via
# test-int-medium / test-int-long-delay / test-int-lakehouse.
# CTV_INT=1: the integration suite runs ONLY under these targets (tests/conftest.py
# skips it otherwise — a bare pytest used to seed the live broker and re-stamp
# eval_meta over whatever stack was up; review gate, round 3).
test-int: export CTV_INT = 1
test-int:
	uv run pytest tests/integration \
		--ignore=tests/integration/test_engine_hardening.py \
		--ignore=tests/integration/test_reconcile.py \
		--ignore=tests/integration/test_rollup_dirty.py \
		--ignore=tests/integration/test_context.py \
		--ignore=tests/integration/test_lakehouse.py

# Feature-5 live medium hardening proof on a CLEAN medium-only stack, isolated by
# the sanctioned `make down` (not a per-test TRUNCATE). Asserts Done-when clauses
# 1/2/3 live against the pinned oracle baseline. Runs the HOT engine only (resolve
# → engine), NOT `make run`: since Phase 6 `make run` also reconciles, and medium
# has a couple of hot-unattributed organics that the 90d pass would over-credit,
# reconciling here would shift the pinned hot-only precision (92/130). The medium
# proof is a hot-engine proof by design.
test-int-medium: PROFILE = medium
test-int-medium: export CTV_INT = 1
test-int-medium:
	$(MAKE) down
	$(MAKE) lake-reset PROFILE=medium CONFIRM=yes
	$(MAKE) up
	$(MAKE) seed PROFILE=medium
	$(MAKE) run-hot PROFILE=medium
	uv run pytest tests/integration/test_engine_hardening.py

# Phase-6 live reconciliation proof on a CLEAN long_delay-only stack, isolated by
# the sanctioned `make down` (tiny/medium/long_delay share conversion_id space;
# DECISIONS Phase 5). Recovers the long-delay misses, then asserts the recovery
# delta + restatement against ClickHouse FINAL — and, since Phase 18a, the
# dirty-set gate (test_rollup_dirty.py): every key whose rollup row the reconcile
# pass changed was refreshed, and the served rollup equals a full rebuild. The gate
# lives HERE, not in `make rollup-bench`: a contract proven only by a target CI
# never runs is proven nowhere it matters (review gate).
test-int-long-delay: PROFILE = long_delay
test-int-long-delay: export CTV_INT = 1
test-int-long-delay:
	$(MAKE) down
	$(MAKE) lake-reset PROFILE=long_delay CONFIRM=yes
	$(MAKE) up
	$(MAKE) seed PROFILE=long_delay
	$(MAKE) run PROFILE=long_delay
	uv run pytest tests/integration/test_reconcile.py tests/integration/test_rollup_dirty.py

# Phase-8/16 live fault-harness proof on a CLEAN shared_ip_spike-only stack,
# isolated by the sanctioned `make down` (profiles share conversion_id space;
# DECISIONS Phase 5). `run-hot` here, NOT `run`: the test pins the hot side first
# (caused_wrong_household == 0 — ambiguous shared-IP conversions are deferred,
# Phase 16), then runs the reconcile pass itself and pins the post side (the
# shared-IP fault observed: 69/80 correct, 11 wrong-household, Row 20) and the
# populated context (report_snapshots exists once that pass has run).
test-int-shared-ip: PROFILE = shared_ip_spike
test-int-shared-ip: export CTV_INT = 1
test-int-shared-ip:
	$(MAKE) down
	$(MAKE) lake-reset PROFILE=shared_ip_spike CONFIRM=yes
	$(MAKE) up
	$(MAKE) seed PROFILE=shared_ip_spike
	$(MAKE) run-hot PROFILE=shared_ip_spike
	uv run pytest tests/integration/test_context.py

# Phase-9 live read-only proof on a CLEAN shared_ip_spike-only stack (same isolation
# reason as the others). Asserts the Done-when's write-denied half — agent_ro
# INSERT/ALTER/DROP/CREATE → ACCESS_DENIED — and that the whole collector read path
# runs under agent_ro (SN2). NO LLM call, so no API tokens: the loop is unit-tested
# with a mocked client (tests/test_loop.py); this proves the DB boundary live.
test-int-agent: PROFILE = shared_ip_spike
test-int-agent: export CTV_INT = 1
test-int-agent:
	$(MAKE) down
	$(MAKE) lake-reset PROFILE=shared_ip_spike CONFIRM=yes
	$(MAKE) up
	$(MAKE) seed PROFILE=shared_ip_spike
	$(MAKE) run PROFILE=shared_ip_spike
	uv run pytest tests/integration/test_agent_readonly.py

# Phase-12/17 live lakehouse proof on a CLEAN long_delay-only stack + lake (same
# shared-conversion_id isolation as the others; this target DOES `lake-reset` the
# long_delay lake — it is destructive, like every clean-stack target). The test
# module itself writes only to a tmp LAKE_ROOT it creates (module fixture), so it
# is safe to run standalone against a populated stack. Asserts: lake-loaded
# serving rows == the direct-write oracle's rows; reconcile equivalence
# (ClickHouse-read exposures == the bucket-aligned lake pass, byte-identical); the
# Dagster-orchestrated pass reproduces the recovery; and an ACCUMULATED lake (≥3
# appends) loads and reconciles byte-identically. No API tokens. Afterwards the
# stack's ClickHouse holds the tmp lake's reconciled rows while
# data/lake/long_delay holds hot rows only — divergent by design; run
# `make lake-reset PROFILE=long_delay CONFIRM=yes && make run PROFILE=long_delay`
# before a `make replay-serving PROFILE=long_delay`.
test-int-lakehouse: PROFILE = long_delay
test-int-lakehouse: export CTV_INT = 1
test-int-lakehouse:
	$(MAKE) down
	$(MAKE) lake-reset PROFILE=long_delay CONFIRM=yes
	$(MAKE) up
	$(MAKE) seed PROFILE=long_delay
	$(MAKE) run-hot PROFILE=long_delay
	uv run pytest tests/integration/test_lakehouse.py

# Prove the five alert rules behave on REAL captured metric values (plus the one
# labelled-synthetic input that fires PartCountHigh — see alerts.yml) (fix #4: promtool
# from the digest-pinned prometheus image, never a floating tag). `check rules`
# validates syntax; `test rules` asserts each alert fires on long_delay's captured
# numbers and that on tiny's only RestatementMagnitude fires (the Phase-16 deferral
# landing restates ROAS) while the other three stay silent
# (observability/rules/tests/alerts_test.yml, generated from make metrics-capture).
# Needs only the image, not the compose stack.
test-alerts:
	docker run --rm -v "$(PWD)/observability/rules:/rules:ro" --entrypoint promtool $(PROM_IMAGE) check rules /rules/alerts.yml
	docker run --rm -v "$(PWD)/observability/rules:/rules:ro" --entrypoint promtool $(PROM_IMAGE) test rules /rules/tests/alerts_test.yml
	docker run --rm -v "$(PWD)/observability/rules:/rules:ro" --entrypoint promtool $(PROM_IMAGE) test rules /rules/tests/alerts_synthetic_test.yml

# The one docs guard (Phase 19; was check-runbook, Phase 15): every link/anchor in
# README.md + docs/ resolves, every `make`-generated block is present under its
# generator's marker and the README copies of its numbers match, and every guard /
# alert / make target the docs name exists in source as an EXACT token
# (scripts/check_docs.py). Standalone script, not a pytest file, so it doesn't
# re-trigger the full suite on a docs-only change (tests/test_check_docs.py runs the
# trace/target half under pytest on purpose). No variable, no delete, no input. No
# services, no network.
check-docs:
	uv run python scripts/check_docs.py

lint:
	uv run pre-commit run --all-files

# The offline review gate (scripts/review_gate.py): make test + ruff check/format
# --check (read-only, never make lint) + check-docs,
# then — with SPEC — the spec's Evidence ids exist and its Record-updates files
# are in the diff; DELETED=a,b greps for removed symbols. ONE process validates
# SPEC (an existing file under specs/, nothing derived from it) before anything
# runs; the value is single-quoted for sh (`_Q`, defined beside `_YES`); nothing
# here edits, commits, or fixes. `/review-round N` runs it first.
review-gate:
	uv run python scripts/review_gate.py $(if $(value SPEC),--spec $(call _Q,$(value SPEC)),) --base $(call _Q,$(if $(value BASE),$(value BASE),main)) $(if $(value DELETED),--deleted $(call _Q,$(value DELETED)),)

# The mutation sweep (scripts/mutate.py): each line of the spec's ```mutations
# block is applied to HEAD in a temporary git worktree (never this tree), the
# offline suite runs there under a reduced env, the worktree is removed
# (try/finally, which also compares `git worktree list` before/after). One verdict per
# mutation, KILLED / SURVIVED / ERROR, summing to the count; a registry change is a
# separate latched REGISTRY line; exit 1 on any of them. SPEC validated in-process.
mutate:
	uv run python scripts/mutate.py --spec $(call _Q,$(value SPEC))
