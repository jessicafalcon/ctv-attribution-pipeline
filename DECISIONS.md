# DECISIONS.md — why-not-X log

One entry per non-obvious choice. Newest last.

## Phase 0

- **Redpanda's built-in schema registry, not a separate container.** Redpanda
  ships the registry in the same binary on port 8081. A separate
  Confluent-style registry container would add a JVM (banned) and a second
  thing to health-check for zero benefit at this scale.
- **`docker compose up -d --wait` for `make up`.** `--wait` blocks on every
  service's healthcheck, which satisfies "health checks, not sleeps" with no
  wrapper script.
- **All image tags pinned.** `latest` would make `make up` behave differently
  across machines/dates — a determinism-policy violation at the infra layer.
- **No runtime Python dependencies in Phase 0.** The allowlist packages
  (bytewax, clickhouse-connect, …) are added in the phase that first imports
  them, so each PR's dependency delta is reviewable.
- **ClickHouse native port (9000) not published to the host.** Everything we
  run from the host uses clickhouse-connect over HTTP (8123); in-network
  services can still use 9000. Avoids a common host-port collision.
- **`run-tests` hook wiring is local-only** (gitignored
  `.claude/settings.local.json`), inherited ruling from trial-signal-assistant:
  a committed settings.json would auto-execute an inbound PR branch's hook +
  pytest + conftest.py for anyone opening it in Claude Code.
- **Compose ports bind 127.0.0.1** (security-review finding): the stack runs
  passwordless local-dev services (ClickHouse default user, Grafana
  admin/admin, Redpanda admin API); binding to localhost keeps them off the
  LAN. In-network services are unaffected (compose DNS).
- **GitHub Actions pinned by tag, not SHA** — accepted for now: this CI
  holds zero secrets. Revisit (SHA-pin) before the Phase 3 integration job.
- **`tests/test_smoke.py` instead of a truly empty suite.** Bare `pytest`
  exits 5 on "no tests collected", which would make `make test` red; one
  layout assertion keeps the suite meaningfully green.
