# BACKLOG.md — deferred findings and revisits

Items reviewers or the developer accepted "for now" with a concrete revisit
trigger. Reviewed at every phase exit (alongside the coherence audit); an
item whose trigger has arrived is either done in that phase or re-deferred
here with a new trigger — never silently dropped.

| Item | Source | Trigger |
|---|---|---|
| SHA-pin GitHub Actions (`actions/checkout`, `astral-sh/setup-uv`) instead of mutable tags | security review, Phase 0 | Before the Phase 3 CI integration job lands |
| Digest-pin compose images (`image@sha256:...`) — full-version tags are still mutable on registry re-push | security review, Phase 0 | Together with the Actions SHA-pinning, before Phase 3 |
| 127.0.0.1 binding still admits any local process to passwordless ClickHouse / Grafana admin | security review, Phase 0 | Only if the stack ever runs on a shared/multi-user host — fine for single-dev laptops |
