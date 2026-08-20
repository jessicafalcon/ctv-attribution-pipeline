# Later phases add: bench, agent-run, agent-eval (see CLAUDE.md → Commands).

.PHONY: setup up down seed resolve run run-hot scale-curve eval report restate bench context agent-run agent-eval metrics-capture test-alerts test test-int test-int-medium test-int-long-delay test-int-shared-ip test-int-agent lint

PROFILE ?= tiny
SOURCE ?= fixtures  # resolve replay input: fixtures/<profile> or out (data/out/<profile>)

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
	uv run pre-commit install

up:
	docker compose up -d --wait

# The ONLY sanctioned destructive path: removes containers AND volumes.
down:
	docker compose down -v

# Deterministic per PRODUCER_SEED (default: profile's seed).
seed:
	uv run python -m producer.seed --profile "$(PROFILE)"

# Offline resolve replay (service-free): device→household, IP fallback, fan-out.
# Writes data/out/<profile>/conversions_resolved.jsonl.
resolve:
	uv run python -m resolve.replay --profile "$(PROFILE)" --source "$(SOURCE)"

# Live pipeline over the seeded stream: resolve stage → attribution engine →
# reconciliation pass (recovers long-window misses, refreshes the rollup, writes
# pre/post report snapshots). Run after `make up && make seed`.
run:
	uv run python -m resolve.stage
	uv run python -m streaming.dataflow
	uv run python -m reconcile.reconcile

# Hot path only (resolve → engine, NO reconciliation). Used by the hot-path
# oracle suites — the frozen tiny golden and pinned tiny accuracy (Phase 3/4),
# and the medium hardening proof — which assert the engine's hot output; a
# reconciliation pass would over-credit their long-tail organics and shift those
# pins. Reconciliation is proven on its own profile (`make test-int-long-delay`).
run-hot:
	uv run python -m resolve.stage
	uv run python -m streaming.dataflow

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

# Dump each stage's TERMINAL Prometheus registry from a REAL knobbed run to
# textfiles under data/out/<profile>/metrics/. This is the provenance of the
# promtool alert fixtures (observability/gen_alert_fixtures.py bakes these into
# observability/rules/tests/alerts_test.yml): the threshold-crossing numbers come
# from a real stage run, never hand-authored.
# Live-stack (run after `make up && make seed PROFILE=<p>`): resolve_input_backlog
# needs a real consumer and reconcile_restatement_roas_abs_delta needs ClickHouse
# FINAL, so these two are not producible service-free — like test-int-long-delay.
metrics-capture:
	mkdir -p data/out/$(PROFILE)/metrics
	uv run python -m resolve.stage --metrics-out data/out/$(PROFILE)/metrics/resolve.prom
	uv run python -m streaming.dataflow --metrics-out data/out/$(PROFILE)/metrics/engine.prom
	uv run python -m reconcile.reconcile --metrics-out data/out/$(PROFILE)/metrics/reconcile.prom

# Attribution accuracy (household grain) vs the truth side file, for the last
# seeded profile. Reads attributed_conversions FINAL from ClickHouse; truth
# never enters the DB (N1, DECISIONS Phase 4).
eval:
	uv run python -m accuracy.run --profile "$(PROFILE)"

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
	uv run python -m agent.run_context --profile "$(PROFILE)"

# Run the attribution-integrity agent once, end to end, against the live stack
# (Phase 9). This is the ONLY path that calls the LLM — it costs API tokens, so ask
# the developer before running (CLAUDE.md). Reads as the SELECT-only agent_ro user;
# emits a typed AttributionFinding + the turn-2 cache_read (Rulings B/E). Run after
# `make run` populated the serving tables. Pass PROFILE explicitly (like context/eval);
# the Phase-9 Done-when uses PROFILE=shared_ip_spike.
agent-run:
	uv run $(AGENT_ENV) python -m agent.run_agent --profile "$(PROFILE)"

# Phase-10 fault->diagnosis sweep: every fault profile + the no-fault baseline, run
# EVAL_REPS times, scored against the pure rubric, both tables written to
# docs/RESULTS.md. Drives its own clean stack per scenario (down/up/seed/run — profiles
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
# only — the medium hardening test and the long_delay reconciliation test each
# need a clean single-profile stack (profiles share conversion_id space; DECISIONS
# Phase 5), so they are excluded here and run via test-int-medium /
# test-int-long-delay.
test-int:
	uv run pytest tests/integration \
		--ignore=tests/integration/test_engine_hardening.py \
		--ignore=tests/integration/test_reconcile.py \
		--ignore=tests/integration/test_context.py

# Feature-5 live medium hardening proof on a CLEAN medium-only stack, isolated by
# the sanctioned `make down` (not a per-test TRUNCATE). Asserts Done-when clauses
# 1/2/3 live against the pinned oracle baseline. Runs the HOT engine only (resolve
# → engine), NOT `make run`: since Phase 6 `make run` also reconciles, and medium
# has a couple of hot-unattributed organics that the 90d pass would over-credit,
# reconciling here would shift the pinned hot-only precision (92/130). The medium
# proof is a hot-engine proof by design.
test-int-medium:
	$(MAKE) down
	$(MAKE) up
	$(MAKE) seed PROFILE=medium
	$(MAKE) run-hot
	uv run pytest tests/integration/test_engine_hardening.py

# Phase-6 live reconciliation proof on a CLEAN long_delay-only stack, isolated by
# the sanctioned `make down` (tiny/medium/long_delay share conversion_id space;
# DECISIONS Phase 5). Recovers the long-delay misses, then asserts the recovery
# delta + restatement against ClickHouse FINAL.
test-int-long-delay:
	$(MAKE) down
	$(MAKE) up
	$(MAKE) seed PROFILE=long_delay
	$(MAKE) run PROFILE=long_delay
	uv run pytest tests/integration/test_reconcile.py

# Phase-8 live fault-harness proof on a CLEAN shared_ip_spike-only stack, isolated
# by the sanctioned `make down` (profiles share conversion_id space; DECISIONS
# Phase 5). `make run` (resolve → engine → reconcile) so report_snapshots exists
# for the context's restatement field; shared_ip_spike keeps delays in the hot
# window, so reconciliation only touches organics and the caused-side pins hold.
# Asserts the shared-IP fault is observed live (Row 20) + the context is populated.
test-int-shared-ip:
	$(MAKE) down
	$(MAKE) up
	$(MAKE) seed PROFILE=shared_ip_spike
	$(MAKE) run PROFILE=shared_ip_spike
	uv run pytest tests/integration/test_context.py

# Phase-9 live read-only proof on a CLEAN shared_ip_spike-only stack (same isolation
# reason as the others). Asserts the Done-when's write-denied half — agent_ro
# INSERT/ALTER/DROP/CREATE → ACCESS_DENIED — and that the whole collector read path
# runs under agent_ro (SN2). NO LLM call, so no API tokens: the loop is unit-tested
# with a mocked client (tests/test_loop.py); this proves the DB boundary live.
test-int-agent:
	$(MAKE) down
	$(MAKE) up
	$(MAKE) seed PROFILE=shared_ip_spike
	$(MAKE) run PROFILE=shared_ip_spike
	uv run pytest tests/integration/test_agent_readonly.py

# Prove the four alert rules fire on REAL captured metric values (fix #4: promtool
# from the digest-pinned prometheus image, never a floating tag). `check rules`
# validates syntax; `test rules` asserts each alert fires on long_delay's captured
# numbers and stays silent on tiny's (observability/rules/tests/alerts_test.yml,
# generated from make metrics-capture). Needs only the image, not the compose stack.
test-alerts:
	docker run --rm -v "$(PWD)/observability/rules:/rules:ro" --entrypoint promtool $(PROM_IMAGE) check rules /rules/alerts.yml
	docker run --rm -v "$(PWD)/observability/rules:/rules:ro" --entrypoint promtool $(PROM_IMAGE) test rules /rules/tests/alerts_test.yml

# Phase-15 docs trace check: every RUNBOOK.md cross-reference resolves and every
# named guard/alert still exists in source (docs/check_runbook.py). Standalone
# script, not a pytest file, so it doesn't re-trigger the full suite on a docs-only
# change. No services, no network.
check-runbook:
	uv run python docs/check_runbook.py

lint:
	uv run pre-commit run --all-files
