# Phase 0 — Skeleton and infra

Contract for the `phase-0-skeleton` branch. Source: `docs/PHASES.md` → Phase 0.

## DONE command

```
make down && make up && make test && make lint
```

Passes when: `make up` exits 0 with every service healthy (compose `--wait`
on health checks), the pytest suite is green, and lint is clean.

Verified at PR time (not runnable locally): CI green on the Phase 0 PR, and
CLAUDE.md "Project tooling" lists what is actually wired.

## Scope

- Repo layout: `producer/ resolve/ streaming/ reconcile/ clickhouse/
  queries/ observability/ agent/ tests/ fixtures/tiny/ specs/ docs/`.
- uv project (Python 3.12), ruff + pytest configured, pre-commit with ruff.
- Docker Compose: Redpanda (built-in schema registry), ClickHouse,
  Prometheus, Grafana, Alertmanager — all with health checks, pinned image
  tags, named volumes.
- Makefile: `setup`, `up`, `down`, `test`, `lint`.
- GitHub Actions CI: `make lint` + `make test` on every PR (integration job
  comes in Phase 3). PR template with Done-when / files / decisions / risks.
- Tooling wired per approved review: `run-tests` hook (wiring local-only in
  gitignored `settings.local.json`), four adapted report-only agents,
  `/selfcheck` command. CLAUDE.md "Project tooling" replaced with the index.

## Out of scope

Event models, topics creation, DDL, metrics endpoints, dashboards content,
alert rules — later phases. No runtime Python dependencies yet.
