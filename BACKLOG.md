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
| Schema-registry subjects have no compatibility mode set; re-registering changed models under default BACKWARD can 409 and fail the seed | coherence audit, Phase 1 | Phase 2 start — set dev subjects to compatibility NONE or handle 409 on identical re-register |
| Phase 3 integration test must assert against ReplacingMergeTree FINAL state, not raw emitted stream (tiny carries duplicates; see DECISIONS.md Phase 1) | coherence audit, Phase 1 | Phase 3 start |
| `min(1.0, rate)` co-view clamp in generate.py saturates silently; a fault profile needing the multiplier observable at high caused_rate needs it revisited | code review, Phase 1 | When authoring the co-view-multiplier-bug fault profile (Phase 8) |
| Unknown `u-` device ids never repeat, so they exercise IP fallback but can never exercise device-keyed dedup or state. Correct per ARCHITECTURE (dedup keys on conversion_id/exposure_id) — but no Phase 5 test may conflate "unknown device" with "duplicate" | developer note, Phase 1 | Phase 5 (dedup / device-keyed state design) |
| The fixture-coverage test pins exact tiny counts (38/12/5 resolve cases, fan-out shapes {2:4, 3:1}) — the pin is load-bearing: re-tuning tiny means changing that test and the frozen fixtures together as one deliberate change, never silently | developer note, Phase 1 | If Phase 2+ ever needs to re-tune the tiny profile |
