# Runbook — attribution pipeline

For the next on-call engineer. Two incidents that already happened, in
post-incident form, plus one known limitation you should not rediscover the hard
way. Every fact here traces to [`ARCHITECTURE.md` §8 "Gotchas"](ARCHITECTURE.md#8-gotchas),
a [`DECISIONS.md`](../DECISIONS.md) entry, or a [`RESULTS.md`](RESULTS.md) number —
nothing is invented. If a symptom you hit isn't below, add it in the same format;
don't guess at a cause.

The four alerts referenced here live in
[`observability/rules/alerts.yml`](../observability/rules/alerts.yml):
`ConsumerLag`, `WatermarkStall`, `MatchRateOutOfBand`, `RestatementMagnitude`.
Where an incident is **not** covered by any of them, this runbook says so — an
un-alerted failure mode is worse when you think it's alerted.

---

## Incident 1 — the benchmark that lied in CI

**Symptom.** `make bench` prints the optimized `campaign_hourly` rollup reading
**more** rows than the naive full scan — a 0.8× ratio — so the "rollup wins"
headline reverses. It reproduces in CI but not on a fresh local run, where the
rollup reads fewer rows and wins.

**Detection.** CI, running `make bench` right after a test that refreshed the
rollup two extra times, measured the rollup at **1020 physical rows** and it lost
to the naive scan (0.8×). The same command locally, after a single refresh, read
**340 rows** and won 2.5×. Same code, two different answers depending on run
history — the tell that a "structural" number wasn't structural.

**Root cause.** `read_rows`/`read_bytes` (from ClickHouse's
`X-ClickHouse-Summary`) count **physically read** rows, not logical ones. A
ReplacingMergeTree keeps every superseded version as physical rows in separate
parts until a background merge collapses them, and `SELECT ... FINAL` physically
reads all the un-merged versions to do the collapse. `campaign_hourly` is
rewritten wholesale each rollup refresh and `attributed_conversions` gains a
higher-`processed_at` version per reconciled conversion, so both version-stack.
The benchmark was therefore measuring transient part-bloat, drifting with
background-merge timing — not table size. (An even earlier capture reported the
naive side at 864 rows / 42 KB and a 1.6× byte win; same artifact, opposite
direction.) See [`ARCHITECTURE.md` §8](ARCHITECTURE.md#8-gotchas), gotcha "A
`FINAL` scan's `read_rows` counts un-merged version-parts", and
[`RESULTS.md` → "Why each change works"](RESULTS.md#benchmark--naive-full-scan-vs-optimized-rollup).

**Fix.** [`queries/bench.py`](../queries/bench.py) `_canonicalize` runs
`OPTIMIZE TABLE ... FINAL` on every read table before measuring, so `read_rows`
reflects merged steady state on both sides — deterministic and re-run-identical,
and the honest apples-to-apples form a scheduled rollup serves in production. A
magnitude-free **direction assert** (`optimized read_rows < naive`) then fails the
run if the rollup ever stops being the smaller read, without pinning to a fragile
absolute count. Steady-state numbers now: naive 835 rows / 25,686 bytes,
optimized 340 rows / 21,760 bytes (2.5× rows, 1.2× bytes) — see the
[`RESULTS.md` benchmark table](RESULTS.md#benchmark--naive-full-scan-vs-optimized-rollup).
The fix relies on two ClickHouse defaults it does **not** override:
`OPTIMIZE ... FINAL` is synchronous on single-node (`alter_sync=1`) and a no-op
when already merged (`optimize_throw_if_noop=0`). Overriding either would
reintroduce the non-determinism.

**Generalization.** Never treat a `FINAL` scan's `read_rows`/`read_bytes` as a
stable structural number without first forcing the merge. Any measurement over a
ReplacingMergeTree that hasn't been canonicalized is measuring merge timing, not
data.

**Would catch it next time.** The guard is code-level and deterministic: the
`_canonicalize` OPTIMIZE plus the direction assert in `queries/bench.py`, enforced
every time `make bench` runs (including in CI). **No alert covers this.** All four
rules in `alerts.yml` watch live pipeline metrics (`resolve_input_backlog`,
`engine_watermark_lag_seconds`, the hot match-rate, `reconcile_restatement_roas_abs_delta`);
none observes the benchmark harness's `read_rows`, which is an offline measurement,
not a scraped metric. If the OPTIMIZE step is ever removed, nothing in monitoring
would flag it — only the direction assert would, and only when `make bench` is run.

---

## Incident 2 — the timezone round-trip that quadrupled the snapshots

**Symptom.** A pipeline run that should write **two** `report_snapshots` per
campaign (pre- and post-reconciliation) writes **four**, and the restatement's
before/after delta collapses toward zero.

**Detection.** `report_snapshots.reported_at` came out stamped **6 hours apart**
(the local UTC offset) for what should have been the same snapshot instant —
different depending on whether the write came from the `make run` subprocess or an
in-process caller. Two writers × two offsets = four rows where two were expected,
and with the versions no longer lining up, the restatement view's paired
before/after rows stopped matching, so the delta collapsed.

**Root cause.** `clickhouse-connect` renders `DateTime` columns in the **client's
local timezone**. Reading `max(ingest_time)` into Python and re-inserting it as the
`reported_at` version lands at a different wall-clock across processes with
different local offsets — a display-layer timezone applied to a value being used
for cross-process identity. See [`ARCHITECTURE.md` §8](ARCHITECTURE.md#8-gotchas),
gotcha "clickhouse-connect renders DateTime columns in the client's local
timezone", and [`DECISIONS.md`](../DECISIONS.md) (Phase 6, "clickhouse-connect
applies the client's local timezone").

**Fix.** The reconciliation job never round-trips a timestamp through Python for
storage. In [`reconcile/rollup.py`](../reconcile/rollup.py), `reported_at` is
computed **server-side** as `max(ingest_time) + offset_ms` inside the INSERT, so it
is identical no matter which process writes it; and in
[`reconcile/reconcile.py`](../reconcile/reconcile.py), `_max_ingest` reads the
`reconciled_at` version as a timezone-free **epoch-millis integer**
(`toUnixTimestamp64Milli`). The
exposure was read-side only: the write path was always safe — `client.insert` of a
timezone-aware UTC datetime stores the correct instant regardless of client offset
(verified: `min(event_time)` in `exposures_landed` equals `sim_start`
`2026-08-01T00:00:00Z` exactly).

**Generalization.** Cross-process timestamp comparison must be timezone-free **at
the wire**, not at the display. When a stored timestamp must round-trip through a
client for storage or cross-process identity, read it as `toUnixTimestamp64Milli`
(or compute it server-side) — never as a rendered `DateTime`.

**Would catch it next time.** The guard is structural: server-side `reported_at`
(`reconcile/rollup.py`) plus the tz-free epoch-millis version read
(`reconcile/reconcile.py` `_max_ingest`) — the bug is unrepresentable now that no
timestamp round-trips through the client for storage.
**No alert covers a recurrence.** `RestatementMagnitude` fires on
`reconcile_restatement_roas_abs_delta > 1.0` — a delta that's too **large**. This
bug's signature is the opposite: a delta that has collapsed toward zero because the
before/after snapshots stopped pairing, which sits *below* the threshold and keeps
the rule silent. So the one alert that touches restatements would not fire on this
failure mode; the code-level guard is the only thing standing in front of it.

---

## Known limitation — the engine is a batch drain, not a continuous follow

Not a bug — a scope boundary, owned out loud so it isn't rediscovered as a
surprise.

**What is proven.** The attribution engine runs the real windowing semantics —
**arrival-ordered processing, watermarks + allowed-lateness release, and hot-window
eviction** (Phase 5) — over a **bounded drain** of the seeded stream. It drains
both topics to memory once (EOF-driven, the same idiom as the resolve stage), feeds
a bounded source, and exits at end-of-input. The windowing correctness is real and
tested; what it operates on is finite.

**Why it's batch.** `bytewax.connectors.kafka` is an **unbounded** source — it
never signals end-of-input — so a dataflow built directly on it would not terminate
on a finite seeded stream. Draining to a bounded source also guarantees every
candidate for a `conversion_id` is present when the reduction runs. See
[`ARCHITECTURE.md` §8](ARCHITECTURE.md#8-gotchas), gotcha "Bytewax's Kafka source
follows forever", and [`DECISIONS.md`](../DECISIONS.md) (Phase 3/5).

**What is not operated.** Three things the batch shape defers, none of them
exercised here:

- **Continuous unbounded Kafka follow** — the engine does not run as a daemon
  tailing the topics; no phase owns the move to continuous follow yet.
- **Spill-to-disk / checkpointed state** — the whole topic is held in memory on a
  single partition; there is no RocksDB-style state backend.
- **TTL'd dedup** — dedup is a full in-memory seen-set, not a windowed/TTL'd one.
  The seeded duplicate is timestamp-identical to its original, so a TTL has nothing
  to measure against here; TTL'd eviction is the continuous-follow story only.

Two of the four alerts carry the same batch-mode honesty in their own comments:
`ConsumerLag` is a backlog **proxy**, not live consumer-group lag, and
`WatermarkStall` is a peak event→ingest lateness **proxy**, not a true
watermark-advance stall — because a batch drain has no advancing watermark to
stall against. Read them as batch-mode signals, not continuous-mode ones.

**Where the continuous version is spec'd.** The 500k/sec port maps every Bytewax
construct to its Flink equivalent — RocksDB state backend, incremental
checkpointing, watermarks + `allowedLateness`, late events to a side output — in
[`SCALING.md` — "Flink mapping"](SCALING.md#flink-mapping-500ksec-port). That's the
operational path to lift this limitation; it is documented, not built.
