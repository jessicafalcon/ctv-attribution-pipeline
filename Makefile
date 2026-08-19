# Later phases add: bench, agent-run, agent-eval (see CLAUDE.md → Commands).

.PHONY: setup up down seed resolve run run-hot eval report restate bench context metrics-capture test-alerts test test-int test-int-medium test-int-long-delay test-int-shared-ip lint

PROFILE ?= tiny
SOURCE ?= fixtures  # resolve replay input: fixtures/<profile> or out (data/out/<profile>)

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

# Prove the four alert rules fire on REAL captured metric values (fix #4: promtool
# from the digest-pinned prometheus image, never a floating tag). `check rules`
# validates syntax; `test rules` asserts each alert fires on long_delay's captured
# numbers and stays silent on tiny's (observability/rules/tests/alerts_test.yml,
# generated from make metrics-capture). Needs only the image, not the compose stack.
test-alerts:
	docker run --rm -v "$(PWD)/observability/rules:/rules:ro" --entrypoint promtool $(PROM_IMAGE) check rules /rules/alerts.yml
	docker run --rm -v "$(PWD)/observability/rules:/rules:ro" --entrypoint promtool $(PROM_IMAGE) test rules /rules/tests/alerts_test.yml

lint:
	uv run pre-commit run --all-files
