# Phase 0 targets. Later phases add: seed, run, eval, report, bench,
# agent-run, agent-eval, test-int (see CLAUDE.md → Commands).

.PHONY: setup up down test lint

setup:
	uv sync
	uv run pre-commit install

up:
	docker compose up -d --wait

# The ONLY sanctioned destructive path: removes containers AND volumes.
down:
	docker compose down -v

test:
	uv run pytest

lint:
	uv run pre-commit run --all-files
