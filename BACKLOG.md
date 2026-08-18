# BACKLOG.md — deferred findings and revisits

Items reviewers or the developer accepted "for now" with a concrete revisit
trigger. Reviewed at every phase exit (alongside the coherence audit); an
item whose trigger has arrived is either done in that phase or re-deferred
here with a new trigger — never silently dropped.

| Item | Source | Trigger |
|---|---|---|
| ~~SHA-pin GitHub Actions (`actions/checkout`, `astral-sh/setup-uv`) instead of mutable tags~~ **DONE Phase 3** — both actions pinned to 40-char SHAs (checkout `11d5960`, setup-uv `d4b2f3b`) with the tag kept in a trailing comment, in both CI jobs | security review, Phase 0 | Before the Phase 3 CI integration job lands — resolved |
| ~~Digest-pin compose images (`image@sha256:...`) — full-version tags are still mutable on registry re-push~~ **DONE Phase 3** — all 5 images pinned `name:tag@sha256:...` (redpanda, clickhouse, prometheus, alertmanager, grafana); `make down && make up` verified health-green on the digests | security review, Phase 0 | Together with the Actions SHA-pinning, before Phase 3 — resolved |
| 127.0.0.1 binding still admits any local process to passwordless ClickHouse / Grafana admin | security review, Phase 0 | Only if the stack ever runs on a shared/multi-user host — fine for single-dev laptops |
| ~~Schema-registry subjects have no compatibility mode set; re-registering changed models under default BACKWARD can 409 and fail the seed~~ **DONE Phase 2** — `producer.schemas` sets every subject to `NONE` before posting (`set_compatibility`); verified against Redpanda | coherence audit, Phase 1 | Phase 2 start — resolved |
| Phase 3 integration test must assert against ReplacingMergeTree FINAL state, not raw emitted stream (tiny carries duplicates; see DECISIONS.md Phase 1) | coherence audit, Phase 1 | Phase 3 start |
| `min(1.0, rate)` co-view clamp in generate.py saturates silently; a fault profile needing the multiplier observable at high caused_rate needs it revisited | code review, Phase 1 | When authoring the co-view-multiplier-bug fault profile (Phase 8) |
| Unknown `u-` device ids never repeat, so they exercise IP fallback but can never exercise device-keyed dedup or state. Correct per ARCHITECTURE (dedup keys on conversion_id/exposure_id) — but no Phase 5 test may conflate "unknown device" with "duplicate" | developer note, Phase 1 | Phase 5 (dedup / device-keyed state design) |
| The fixture-coverage test pins exact tiny counts (38/12/5 resolve cases, fan-out shapes {2:4, 3:1}) — the pin is load-bearing: re-tuning tiny means changing that test AND the frozen fixtures (now incl. `fixtures/tiny/expected/conversions_resolved.jsonl` and `tests/test_resolve_fixture.py`, which also pins 68 raw rows) together as one deliberate change, never silently | developer note, Phase 1 (extended Phase 2) | If Phase 2+ ever needs to re-tune the tiny profile |
| Resolve stage loads the device graph once at startup (`load_graph_index`); a device added to the graph after startup resolves as unknown-device → IP fallback or unresolvable instead of a device hit. Fine for the Phase 2 batch pass (static graph); must refresh on `device_graph` updates when the stage moves to continuous follow | architect review, Phase 2 | When resolve moves to continuous follow (Phase 3+) |
| Resolve stage assigns `conversions` from `OFFSET_BEGINNING` every pass with `enable.auto.commit=False`, ignoring group offsets — correct for the one-shot batch pass, but reprocesses the whole topic on every pass under continuous `make run`. The graph-refresh row covers the graph side, not this conversions-offset side | coherence audit, Phase 2 | When resolve moves to continuous follow (Phase 3+) |
