"""Agent config constants (Phase-9 Ruling A / B). One place so the model, effort,
rep count, and loop bound are deliberate pins a reviewer can find — not defaults
scattered across call sites.

`AGENT_MODEL`/`AGENT_EFFORT` are chosen on capability, not the §2 cost posture (all
tiers clear $10): the near-miss discriminator is a pre-computed labeled context field,
a moderate weigh-two-signals judgment Sonnet handles at medium effort (DECISIONS
Phase 9). `EVAL_REPS` is defined here and CONSUMED IN PHASE 10 (the fault→diagnosis
sweep) — it is the fixed N behind the false-positive-rate table, pinned now so the
budget is predictable. Temperature is intentionally never set: it is removed on the
Claude-5 family (400 on any value), so the agent is non-byte-reproducible by
construction — which is why Phase-10 measures residual stability over reps, consistent
with the determinism policy's AI-edge carve-out (DECISIONS Phase 9)."""

AGENT_MODEL = "claude-sonnet-5"
AGENT_EFFORT = "medium"

# Phase-10 sweep: 5 reps × 6 scenarios = 30 invocations. Defined here, used there.
EVAL_REPS = 5

# Hard stop on the tool-use loop: emit AMBIGUOUS_NEEDS_HUMAN rather than loop forever.
AGENT_MAX_PROBE_ROUNDS = 6
