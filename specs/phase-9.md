# Phase 9 — Agent loop

Contract for the `phase-9-agent-loop` branch. Source: `docs/PHASES.md` → Phase 9,
`docs/ARCHITECTURE.md` §4.1 (what it watches) / §4.2 (the bounded read-only loop:
Observe → Hypothesize → Test → Rank → Report) / §4.3 (near-miss pair), the Phase-8
`AttributionContext` frozen contract (`agent/context.py`), and the Phase-9
forward-notes carried in `specs/phase-8.md` (SN1 prompt-injection, SN2/CA-Q4
SELECT-only coverage of the collector read path, CA-minor `profile` is a label).

**Deliverables (all gated):** a **hypothesis catalog enum**, a **probe registry**
(named parameterized SQL as tools over a **SELECT-only ClickHouse user**, the model
never writes SQL), **ranking**, a typed **`AttributionFinding`**, and an
**Alertmanager webhook endpoint**. This phase adds the reasoning layer over the
Phase-8 observation object; it does NOT run the full fault→diagnosis eval sweep or
the near-miss demo — those are Phase 10.

## Design-review rulings this phase is built on (settled with the developer — do not re-litigate)

**Ruling A — model, effort, and rep count are pinned here as config constants, not
left to Phase 10.** `AGENT_MODEL = "claude-sonnet-5"`, `AGENT_EFFORT = "medium"`,
`EVAL_REPS = 5`, in one `agent/config.py`.
- **Model = Sonnet 5 on capability, not cost.** The near-miss discriminator
  (`ip_clusters.ip_resolved_fraction` — elevated on `shared_ip_spike`, flat on
  `real_lift`) is a *pre-computed, labeled* context field; the agent weighs a named
  number against the match-rate rise, a moderate inference Sonnet handles cleanly.
  Opus buys no discrimination headroom the deterministic context doesn't already
  supply; Haiku's risk is misranking under the enum on one rep of a repeated sweep,
  which would undermine the Phase-10 headline. All three models clear the §2 "$10"
  posture; cost does not pick the model.
- **Effort = medium.** The near-miss is a bounded weigh-two-signals judgment, not
  deep agentic work; `high` mostly buys output tokens (cost) without changing the
  ranking. Deliberate pin, recorded in DECISIONS.
- **Reps = 5** is pinned now so the Phase-10 false-positive-rate table has a fixed,
  stated N (5 reps × 6 scenarios = 30 invocations) and the budget is predictable.
  Phase 9 defines the constant and does NOT consume it (no sweep here).

**Ruling B — prefix caching is wired in Phase 9, and it doubles as a determinism
nudge.** `cache_control: {"type": "ephemeral"}` on the stable prefix (system prompt
+ hypothesis enum + probe-registry tool list). This is the ~10× lever on the
dominant input term (the stable prefix, ~3–8k tokens, vs the ~1–2k serialized
context). Caching *requires* a frozen, stably-ordered prompt (render order
`tools → system → messages`), which is exactly the discipline the determinism policy
wants at the AI edge. The `agent-run` entrypoint logs `cache_read_input_tokens`;
Phase 9 asserts it is `> 0` on the second turn of the one live run.

**Ruling C — the agent is NOT byte-reproducible, by construction, and that is
correct.** Temperature/top_p are removed on the entire Claude-5 family (400 on any
value), so `temperature=0` is impossible; the agent's output varies run to run. This
is not a determinism-policy violation — the policy explicitly carves the AI out of
the byte-identical guarantee (CLAUDE.md: "AI sits at the edge; the pipeline is
deterministic"). It is *why* "repeated" earns its place in Phase 10: the reps
measure residual FP-rate stability, not byte-identity. One-line DECISIONS entry so a
reviewer doesn't later flag "why isn't the agent deterministic".

**Ruling D — the SELECT-only user is a `users.d` config file, not SQL DDL. Both
paths run in this stack; Option 2 is chosen for identity-mechanism consistency, not
because the SQL path is blocked.** New `clickhouse/users.d/agent-ro.xml`, same
passwordless / `::/0` / 127.0.0.1-host-bound posture as `allow-network.xml`, one `:ro`
compose mount.

*Empirical correction (verified on `clickhouse-server:24.8`):* the stock `default`
user ships with `ACCESS MANAGEMENT ... WITH GRANT OPTION`, so `CREATE USER` / `GRANT`
via the `apply.py` path (host → HTTP 8123, passwordless `default`) returns 200. An
earlier draft claimed `default` lacked access management and the SQL path would fail
at `apply.py` / force widening `default` — that was **wrong**; it inferred a missing
privilege from a missing config file (absence of config ≠ absence of privilege). Both
provisioning paths run. The choice is a preference, not a blocker. (Note kept verbatim
so a future reviewer's "is this premise real?" is already answered — that is the
question that just failed.)

Option 2 is chosen because:
1. **Identity belongs to the compose-up config layer, by one mechanism (primary
   reason).** `allow-network.xml` already establishes that principals and their access
   (the `default` user's network posture, the passwordless stance) are declared in
   `users.d` and reconstructed from source at container start. `agent_ro` is the same
   kind of object — a principal plus its grant — so it belongs in the same place,
   reconstructed at the same lifecycle point. Provisioning it in `ddl.sql` splits
   identity across two mechanisms for no gain; schema (tables) is the make-run /
   `ddl.sql` concern, and a user is not schema.
2. **Reconstruct-from-source (supporting, not decisive).** Config users are rebuilt
   from `users.d` at every container start. A SQL user persists in the mutable
   `access/` store on the data volume; the naive `CREATE USER IF NOT EXISTS` then
   skips it, so a later grant edit in `ddl.sql` silently fails to apply on a volume
   that outlives it (verified: a probe user created over HTTP persisted in `show
   users` until explicitly dropped). A *correct* SQL path — `CREATE USER OR REPLACE`
   plus an unconditional `GRANT` each run — would also reconstruct-from-source, so
   this is a reason to prefer config, not a defect that blocks SQL.

**Grant form, not `<readonly>1</readonly>`.** `<grants><query>GRANT SELECT ON
default.*</query></grants>` is DB-enforced SELECT-only at the access-control layer
(write-denied test passes: INSERT/ALTER/DROP/CREATE → `ACCESS_DENIED`) AND does not
reject the benign session settings `clickhouse-connect` sends on connect;
`readonly=1` can 400 on driver-set settings (a stack-surprise the grant form
sidesteps). Inline `<grants>` is supported since CH 22.4; the image is 24.8.

```xml
<clickhouse>
  <users>
    <agent_ro>
      <password></password>
      <networks replace="replace"><ip>::/0</ip></networks>
      <grants><query>GRANT SELECT ON default.*</query></grants>
    </agent_ro>
  </users>
</clickhouse>
```

**Ruling E — the SELECT-only user covers the WHOLE agent read path (SN2/CA-Q4), not
just probes.** Both the probe executor AND the Phase-8 collectors (`run_context.py` /
`agent/readers.py`, plus `report.run` / `restatement.run` when called from the agent
context) read as `agent_ro`. A new `connect_agent()` in `clickhouse/client.py`
(reads `CLICKHOUSE_AGENT_USER`, default `agent_ro`, empty password) is the single
read handle for everything agent-side. `make context` is re-pointed to it — same
rows, SELECT-only, deliberate change recorded — so the read-only guarantee holds on
the collectors, not only the probes. The write-denied integration test connects via
`connect_agent()` and asserts writes fail.

## Hypothesis catalog (the enum — ARCHITECTURE §4.2 "Hypothesize")

`agent/hypotheses.py`, `Hypothesis(StrEnum)`, exactly the six typed causes in §4.2,
each mapped to the §4.1 signal and the context field that feeds it:

| Enum member | §4.1 signal | Context field(s) |
|---|---|---|
| `DEVICE_GRAPH_MISMATCH` | wrong-household / shared-IP matches | `ip_clusters` |
| `WINDOW_EDGE_EFFECT` | attribution-window edge effects | `window_edge` |
| `CO_VIEW_INFLATION` | co-viewing inflation | `genre_reach` (RAW only — see below) |
| `LATE_ARRIVAL_DISTORTION` | late-arrival restatement | `restatements` |
| `REAL_PERFORMANCE_CHANGE` | genuine ROAS/match-rate lift | `match_rate_by_day`, `campaigns`, `ip_clusters` (flat) |
| `UPSTREAM_DATA_CHANGE` | match-rate anomaly not explained above | `match_rate_by_day` |

**Co-view caveat (carried from Phase 8, Row 15/26):** `CO_VIEW_INFLATION` is in the
enum for completeness but is NOT reliably discriminable from RAW `genre_reach` alone
(the adjusted factor is BACKLOG 26 / Phase 10). The agent MUST NOT return it as a
CONFIDENT top hypothesis from raw reach; the prompt states this and the ranking
guidance routes an unexplained genre skew to `AMBIGUOUS_NEEDS_HUMAN`. No Phase-9 gate
asserts a co-view diagnosis.

## Probe registry (ARCHITECTURE §4.2 "Test"; the probe contract, CLAUDE.md)

`agent/probes.py`. Each entry is `(name, parameterized SQL, pydantic result type)`
exposed to the model as a **tool** with a JSON-schema-typed parameter set; the model
supplies parameters, never SQL. The dispatcher validates params (pydantic), runs the
FIXED SQL with bound query parameters (clickhouse-connect server-side params — no
string interpolation, no injection surface) as `agent_ro`, and returns the typed
rows. Bounded set (each a confirm/deny follow-up for one hypothesis; `LIMIT`-bounded):

- `ip_cluster_detail(ip: str)` — attributed conversions, candidate counts, households
  reached through one shared IP → confirm/deny `DEVICE_GRAPH_MISMATCH`.
- `campaign_attributed_by_day(campaign_id: str)` — daily attributed conversions +
  revenue for one campaign (the per-campaign attributed *trend*, not a rate — a
  per-campaign `processed` denominator is undefined; read alongside the context's
  global `match_rate_by_day`) → `REAL_PERFORMANCE_CHANGE` vs `UPSTREAM_DATA_CHANGE`.
- `campaign_restatement(campaign_id: str)` — PRE-vs-now ROAS/conversions/revenue delta
  for one campaign → `LATE_ARRIVAL_DISTORTION`.
- `window_edge_distribution()` — the 7d-hot attribution-lag histogram + near-boundary
  share → `WINDOW_EDGE_EFFECT`.
- `genre_reach_detail(genre: str)` — RAW exposures / attributed / ratio for one genre
  → the (bounded, caveated) `CO_VIEW_INFLATION` check.

The registry is a single source of truth: `{name: Probe}` where `Probe` carries the
SQL, the params model, and the result model. `probes.tool_schemas()` derives the
Anthropic tool list from it (stable order → cacheable prefix); `probes.run(client,
name, params)` executes it. A structural test asserts every probe's SQL is
parameter-bound (no f-string of a param) and returns its declared type.

## Typed `AttributionFinding` (ARCHITECTURE §4.2 "Report"; the output contract)

`agent/finding.py`. Pydantic, schema-constrained; validation failure → escalate
`AMBIGUOUS_NEEDS_HUMAN`, never silent retry (CLAUDE.md output contract).

- `RankedHypothesis`: `hypothesis: Hypothesis`, `evidence_for: list[str]`,
  `evidence_against: list[str]`, `weight: float` (0–1, evidence weight).
- `AttributionFinding`: `profile: str` (the caller label, CA-minor: never treated as
  ground truth about which rows were read), `top_hypothesis: Hypothesis`,
  `ranked: list[RankedHypothesis]` (ordered by weight desc — the **ranking**),
  `ruled_out: list[Hypothesis]`, `recommended_action: str` (e.g. "hold campaign
  cmp-002 ROAS pending review"), `verdict: Literal["CONFIDENT",
  "AMBIGUOUS_NEEDS_HUMAN"]`, `probes_run: list[str]`.

The finding is produced by a **terminal `submit_finding` tool** whose input schema IS
`AttributionFinding`. The loop ends when the model calls it; the dispatcher validates
the input into the pydantic model. This keeps the whole model→app boundary inside the
typed-tool idiom (no structured-output/tool-use mix) and gives one clean escalation
path: a pydantic `ValidationError` on `submit_finding` → return a synthesized
`AMBIGUOUS_NEEDS_HUMAN` finding with the raw payload in `evidence_for`, never a silent
retry.

**`strict: true` is load-bearing (resolved — FT-1 residual materialized).** The first
live run proved `strict` is NOT belt-and-suspenders here: with the NON-strict schema
the model reasoned correctly (device_graph_mismatch, CONFIDENT, exact shared-IP
evidence) but returned `ranked` as a **stringified JSON array** — the whole payload
collapsed into that one field — so `model_validate` failed and the app-side net
escalated a CORRECT finding to AMBIGUOUS_NEEDS_HUMAN. Fix: `submit_finding` uses a
STRICT schema (`loop._strict_schema` inlines every `$ref`, sets
`additionalProperties: false` + all-keys `required` on every object, drops `title`;
`AttributionFinding` has no Optional/None fields, so no `anyOf`-null branch trips
strict). The live API accepted it and the confirming run returned a native `ranked`
array (device_graph_mismatch / CONFIDENT). App-side `model_validate` stays as the net,
so the escalate-contract stays pure and fires only on a genuine semantic failure — it
no longer masks a syntactic mangling. Regression guard: the exact malformed payload is
committed (`tests/data/malformed_submit_finding_input.json`) and asserted through
`_finalize` (`tests/test_loop.py`), plus a strict/ref-free schema-shape test.

## The loop (ARCHITECTURE §4.2; `agent/loop.py`)

A small, explicit manual tool-use loop (chosen over the SDK Tool Runner for control
and testability: we need agent_ro probe execution, param validation, the terminal
`submit_finding` escalation, prefix caching, and a mock-client unit test — an explicit
`while stop_reason == "tool_use"` loop is the interview-explainable standard here).

1. **Observe** — `agent/run_context.collect(connect_agent(), profile)` builds the
   frozen §4.2 `AttributionContext` (Phase-8 collectors, now via `agent_ro`).
2. **Prompt** — stable system prefix (role + the loop's rules + the co-view caveat +
   the ranking/verdict guidance) and the probe tool list, both under one
   `cache_control` breakpoint; the context is serialized as a **fenced JSON block in
   a user message** (SN1: `ip` / `program_genre` / `campaign_id` / cluster IPs reach
   the model as delimited data, never spliced into instruction text).
3. **Test / Rank** — `client.messages.create` with the probe tools; execute each
   `tool_use` as `agent_ro`, return typed `tool_result`s; loop, bounded by
   `AGENT_MAX_PROBE_ROUNDS` (config; a hard stop that emits an
   `AMBIGUOUS_NEEDS_HUMAN` finding rather than looping forever). Each turn appends the
   FULL `response.content` (thinking + `tool_use` blocks) back to `messages`, not just
   text — Sonnet-5 adaptive thinking returns `thinking` blocks that must be echoed
   back unchanged within the same-model tool loop.
4. **Report** — the model calls `submit_finding`; the loop validates and returns the
   `AttributionFinding`. `AGENT_MODEL`, `AGENT_EFFORT`, adaptive thinking, and the
   cached prefix are set here.

**Loop contract — ≥1 confirming probe before `submit_finding` (§4.2 "Test" is not
skippable).** Because the observe-step context already carries the near-miss
discriminator (`ip_resolved_fraction`), a confident model could call `submit_finding`
on turn 1 with zero probes — skipping the §4.2 Test step and leaving nothing on turn 2
to cache-assert. The prompt rules REQUIRE at least one confirming probe for the
leading hypothesis before `submit_finding` is allowed; a `submit_finding` with an
empty `probes_run` is rejected back to the model once ("run a probe first"), then, if
still empty, escalated to `AMBIGUOUS_NEEDS_HUMAN`. This enforces
Observe→Hypothesize→**Test**→Rank→Report, makes every finding cite a probe result
(auditability), and guarantees a turn 2 for the `cache_read` assertion.

`make agent-run PROFILE=<fault>` (`agent/run_agent.py`) runs the loop once and prints
the finding. **API-token command — ask the developer before running** (CLAUDE.md).

## Webhook endpoint (`agent/webhook.py`)

FastAPI app, `POST /alerts`, parses the Alertmanager webhook JSON (the standard
`{status, alerts: [{labels, annotations, ...}]}` shape), and triggers one agent
sweep per firing alert. The LLM call is injected (a callable dependency) so the unit
test posts a **captured Alertmanager payload** and asserts the endpoint parses it and
dispatches a sweep **with the LLM mocked** — zero tokens. The **live** scrape →
Alertmanager → webhook push chain stays deferred (BACKLOG "Live Alertmanager firing
path": the batch stages exit before a scrape; Phase 9 builds the endpoint, not the
push path). The scheduled-sweep trigger is `make agent-run`.

**Boundary (security, designed not discovered).** The alert payload is a **trigger
only**. The sweep re-derives the `AttributionContext` deterministically from
ClickHouse and does NOT feed alert `labels`/`annotations` into the LLM prompt — alert
labels are attacker-influenceable, and re-observing from the DB is also the cleanest
determinism story. If any alert text ever must reach the model, it goes inside the
same SN1 fenced *data* block, never instruction text. A test asserts a crafted alert
label does not appear in the assembled prompt's instruction region.

## DONE command

```
make down && make up && make seed PROFILE=shared_ip_spike && make run && \
make test && make lint && \
make test-int-agent            # write-denied + live read path (no tokens)
# then, with developer approval (API tokens):
make agent-run PROFILE=shared_ip_spike
```

- `make test` (offline, no tokens): probe-registry structural tests, `AttributionFinding`
  schema + escalation tests, the loop against a **mocked Anthropic client** (canned
  tool-use turns → dispatches the right probes → valid finding; malformed
  `submit_finding` → `AMBIGUOUS_NEEDS_HUMAN`), the SN1 prompt-injection test (a crafted
  context value lands in the JSON block, cannot alter the instruction text), the
  webhook test (sample payload, mocked LLM).
- `make test-int-agent` (live stack, no tokens): connect as `agent_ro` and assert
  INSERT / ALTER / DROP / CREATE all raise `ACCESS_DENIED` (**Done-when: DB user
  cannot write**), and assert `collect(connect_agent(), ...)` reads the context
  through the SELECT-only user (SN2 coverage). Isolated `shared_ip_spike` stack, same
  shared-conversion_id reason as the other `test-int-*` targets.
- `make agent-run PROFILE=shared_ip_spike` (developer-approved, API tokens): the agent
  runs end to end and emits a valid `AttributionFinding` (**Done-when: end-to-end
  valid finding**) whose `top_hypothesis == DEVICE_GRAPH_MISMATCH` (the shared-IP
  fault) with a non-empty ranking, and the run logs `cache_read_input_tokens > 0`
  (Ruling B, guaranteed by the ≥1-probe loop contract). The `CONFIDENT` verdict is the
  **observed-expected** outcome, reported and eyeballed — NOT gated (Ruling C: the
  agent is non-reproducible; verdict stability is a Phase-10 measurement over reps,
  never a single-run assertion, and `agent-run` is not in CI).
- `make lint`; gate-0 tiny golden byte-identical (the agent is a read-only observer;
  it writes nothing to the pipeline tables — pipeline output with the agent disabled
  is byte-identical, ARCHITECTURE §3.1).

## Done-when

1. **Agent runs against one fault profile end to end and emits a valid finding.**
   `make agent-run PROFILE=shared_ip_spike` produces a pydantic-valid
   `AttributionFinding` with `top_hypothesis == DEVICE_GRAPH_MISMATCH`, a non-empty
   ranking, and `probes_run` non-empty; `cache_read_input_tokens > 0` on turn 2.
   (Gated: valid finding + correct top hypothesis + non-empty ranking + a probe was
   run. NOT gated: the `CONFIDENT` verdict — observed-expected, reported not asserted,
   per Ruling C.)
2. **A test asserts the agent's DB user cannot write.** `make test-int-agent`:
   `agent_ro` INSERT/ALTER/DROP/CREATE → `ACCESS_DENIED`; and the collector read path
   runs under `agent_ro` (SN2).
3. Offline `make test` (probes, finding, mocked loop, SN1 injection, webhook) + `make
   lint` + gate-0 tiny golden byte-identical, all green.

## Scope (files)

- `agent/config.py` (model/effort/reps/max-rounds constants), `agent/hypotheses.py`
  (enum), `agent/probes.py` (registry + tool schemas + dispatcher),
  `agent/finding.py` (models), `agent/loop.py` (the loop + prompt assembly),
  `agent/run_agent.py` (`make agent-run`), `agent/webhook.py` (FastAPI endpoint).
- `agent/run_context.py` — re-point reads to `connect_agent()` (SN2), no shape change.
- `clickhouse/client.py` — add `connect_agent()`.
- `clickhouse/users.d/agent-ro.xml` (new), `docker-compose.yml` (one `:ro` mount line).
- `Makefile` — `agent-run`, `test-int-agent` targets.
- Tests: `tests/test_probes.py`, `tests/test_finding.py`, `tests/test_loop.py`
  (mocked client), `tests/test_prompt_injection.py`, `tests/test_webhook.py`,
  `tests/integration/test_agent_readonly.py`.
- Records: this spec, `DECISIONS.md` (Phase 9 block: Rulings A–E, the non-determinism
  note, the terminal-tool + thinking-echo + strict-fallback gotchas), `BACKLOG.md`
  (SN1 resolved; live-Alertmanager path re-deferred; FG2 cross-profile live pin still
  Phase 10; **new**: sweep-amplification trigger — "one sweep per firing alert" over
  `alerts[]` is a token-amplification vector once the live push lands, so dedupe/bound
  sweeps per webhook by `alertname`/`fingerprint` before wiring it; **new**:
  consider promoting just the token-free `agent_ro` write-denied check to PR CI as its
  own job — it's security-load-bearing, but needs its own isolated stack, so it lands
  as a local proof now with a CI-promotion trigger recorded), `CLAUDE.md` status +
  `make agent-run` / `make test-int-agent` in Commands.

## Review & stack risk

- **code-reviewer + functionality-tester** at the finish line (mandatory).
- **security-reviewer TRIGGERED — mandatory this phase.** New ClickHouse user
  (`agent-ro.xml`), compose service-config change, and the FIRST LLM-boundary /
  agent-context-assembly code in the repo. Focus: SELECT-only is DB-enforced and
  covers the whole read path (SN2); no secret in the XML; `ANTHROPIC_API_KEY` stays
  `.env`-only, never logged, never CI; SN1 structured-context enforcement; the probe
  dispatcher binds params server-side (no SQL injection); the webhook doesn't execute
  untrusted alert text.
- **coherence-auditor** at phase exit (mandatory) + BACKLOG review.
- Stack surprise watch: `readonly=1` vs grant-form (Ruling D — grant chosen); confirm
  `agent_ro` write denial is `ACCESS_DENIED` not a silent no-op. (The `default`
  access-management premise is already settled live — Ruling D empirical correction.)

## Out of scope (deferred, recorded)

- The full fault→top-hypothesis→correct? eval sweep, the false-positive-rate table,
  the no-fault baseline profile, the near-miss *demo* → **Phase 10** (`make
  agent-eval`). `EVAL_REPS` is defined here, consumed there.
- Co-view-*adjusted* factor → BACKLOG 26 / Phase 10; `CO_VIEW_INFLATION` stays a
  caveated, non-CONFIDENT enum member here.
- Live Alertmanager scrape → webhook push path → BACKLOG (re-deferred; endpoint built,
  push path not wired).
- Per-profile live headline pins for the other four faults (FG2) → Phase 10.
