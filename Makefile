# Later phases add: run, eval, report, bench, agent-run, agent-eval,
# test-int (see CLAUDE.md → Commands).

.PHONY: setup up down seed resolve test lint

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

test:
	uv run pytest

lint:
	uv run pre-commit run --all-files
