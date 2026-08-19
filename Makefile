# Later phases add: bench, agent-run, agent-eval (see CLAUDE.md → Commands).

.PHONY: setup up down seed resolve run run-hot eval report restate test test-int test-int-medium test-int-long-delay lint

PROFILE ?= tiny
SOURCE ?= fixtures  # resolve replay input: fixtures/<profile> or out (data/out/<profile>)

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
		--ignore=tests/integration/test_reconcile.py

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

lint:
	uv run pre-commit run --all-files
