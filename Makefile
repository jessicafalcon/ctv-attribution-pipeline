# Later phases add: run, eval, report, bench, agent-run, agent-eval,
# test-int (see CLAUDE.md → Commands).

.PHONY: setup up down seed test lint

PROFILE ?= tiny

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
	uv run python -m producer.seed --profile $(PROFILE)

test:
	uv run pytest

lint:
	uv run pre-commit run --all-files
