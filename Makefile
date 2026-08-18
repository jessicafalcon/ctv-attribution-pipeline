# Later phases add: bench, agent-run, agent-eval (see CLAUDE.md → Commands).

.PHONY: setup up down seed resolve run eval report test test-int lint

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

# Live pipeline over the seeded stream: resolve stage → attribution engine
# (reconciliation scheduler added Phase 6). Run after `make up && make seed`.
run:
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

# Offline: no broker/ClickHouse. --ignore keeps the integration suite (which
# would probe services before skipping) from making any network attempt.
test:
	uv run pytest --ignore=tests/integration

# Integration tests against the running compose stack (`make up` first).
test-int:
	uv run pytest tests/integration

lint:
	uv run pre-commit run --all-files
