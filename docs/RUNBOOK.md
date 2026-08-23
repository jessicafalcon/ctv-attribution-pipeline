# Runbook — attribution pipeline

For the next on-call engineer. Four incidents that already happened, in
post-incident form, plus one known limitation you should not rediscover the hard
way. Every fact here traces to [`ARCHITECTURE.md` §8 "Gotchas"](ARCHITECTURE.md#8-gotchas),
a [`DECISIONS.md`](../DECISIONS.md) entry, or a [`RESULTS.md`](RESULTS.md) number —
nothing is invented. If a symptom you hit isn't below, add it in the same format;
don't guess at a cause.

The five alerts referenced here live in
[`observability/rules/alerts.yml`](../observability/rules/alerts.yml):
`ConsumerLag`, `WatermarkStall`, `MatchRateOutOfBand`, `RestatementMagnitude` and
`PartCountHigh` (Phase 18a — the storage one).
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

**Fix.** [`queries/bench_common.py`](../queries/bench_common.py) `canonicalize` runs
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
`canonicalize` OPTIMIZE (`queries/bench_common.py` since Phase 18a) plus the
direction assert in `queries/bench.py`, enforced
every time `make bench` runs (including in CI).

Since Phase 18a the *condition* behind this incident is measured:
`clickhouse_active_parts{table}` and `clickhouse_unmerged_parts{table}`
(`observability/ch_scrape.py`, scraped at the end of every `make run` / `run-hot` /
`metrics-capture`), and `PartCountHigh` alerts when a table passes 150 active parts —
ClickHouse's own `parts_to_delay_insert` default, the point where the server starts
throttling writers.

**That alert would NOT have caught incident #1.** This incident's part counts were in
the single digits (a real capture reads 5 active parts at most on any profile here);
`PartCountHigh` is about a table heading for insert throttling, not about a benchmark
measuring un-merged parts. The guard for THIS failure remains the harness's own
`canonicalize` OPTIMIZE plus the direction assert, and only when `make bench` runs.
No alert watches the benchmark's `read_rows`, which is an offline measurement rather
than a scraped metric. What changed is that the un-merged parts are now visible in a
registry instead of being folklore; what did not change is what would have caught this.

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

## Incident 3 — the snapshot that disagreed with itself in the 15th digit

**Symptom.** In 2 of 11 full DONE runs, `make run`'s second reconciliation pass
leaves `report_snapshots` holding two rows for the same `(reported_at,
campaign_id)` whose ROAS differ in the last float digit
(`130.64573570759137` vs `130.6457357075914`, camp-02). `restatement.run()`
before and after the pass then disagree, and the Phase-6 idempotency test
fails — on a re-run it passes.

**Detection.** Found by running the full DONE chain more times than any phase
before (the Phase-17 review gate ran it eleven times). No pin could see it:
every accuracy, report and parity assertion rounds to 4–6 dp, and the exact
comparison that did catch it was read as a flake twice before it was diagnosed.
The determinism question — "could this step give a different answer on a
re-run?" — had a "rarely" answer for every monetary aggregate in a versioned
table.

**Root cause.** `sum(revenue)` / `sum(spend)` were Float64 sums. Float addition
is not associative; ClickHouse sums the rows in the order it visits the parts,
and that order differs between two passes over the same rows (a merge in
between is enough). Each pass wrote a "re-run-identical" row that was not:
ReplacingMergeTree keeps both twins until it merges, and `argMax(roas,
reported_at)` over equal versions returns either. See
[`DECISIONS.md`](../DECISIONS.md) (Post-Phase-11 fixes, "Monetary aggregates in
versioned tables are computed in Decimal").

**Fix.** [`reconcile/rollup.py`](../reconcile/rollup.py): every monetary sum
written to `campaign_hourly` or `report_snapshots` is `sum(toDecimal64(toString(x), 4))`
— exact in any order — cast `toFloat64` on write; the ratios are Float64
divisions of those exact sums (full precision, deterministic). The conversion
goes through `toString`: `toDecimal64(<Float64>, 4)` TRUNCATES the binary value
(`26.08` is `26.0799…` → `26.0799`; the first cut of this fix understated
revenue by 4e-4 and `make bench` caught it in CI) while the decimal string parses
exactly — exact because the producer quantizes money to cents (pinned). Column
types and every reported number are unchanged; `make bench` equality and the
per-campaign value pin prove it.

**Generalization.** A versioned table's row must be a pure function of its
inputs, and a Float64 aggregate over a ClickHouse table is not — its value is a
function of the part layout. The rule: any float aggregate WRITTEN to a versioned
table must be order-independent (Decimal); COMPARED floats use a dp gate (the
`report.sql`/`bench.sql` pair — its open half is a BACKLOG row). Rounding at
write is not a fix: it shrinks the window, it does not close it. And the string
conversion is a bridge: money stored as Float64 is the root cause; Decimal64(4)
end-to-end is the destination (BACKLOG, Phase 18a).

**Would catch it next time.** Two exact two-pass pins in
[`tests/integration/test_reconcile.py`](../tests/integration/test_reconcile.py):
the merge-immune one (every key's money in both versioned tables, read at full
precision, is identical before and after a second pass — whichever twin a merge
keeps) and the direct one (the raw, un-`FINAL` twins of both tables are
byte-identical per key whenever a merge has not yet collapsed them), plus the
value pin (stored money equals the source to the cent) and the offline SQL guard
[`tests/test_rollup_decimal.py`](../tests/test_rollup_decimal.py) (no money
`sum()` without `toDecimal64`). **No alert covers a recurrence**: a last-digit
difference moves no metric past any threshold.
## Incident 4 — the reconciled rows that moved with the laptop's clock

**Symptom.** Every row the reconciliation pass wrote (`path='reconciled'`) carried
`event_time` and `ingest_time` shifted by the **machine's UTC offset** (+6h on an
MDT laptop; 0 in CI), for ten phases (6–16). Campaign totals, the accuracy pins,
the restatement snapshots and every integration test are offset-invariant, so
nothing moved that any guard could see; the exposure was the hour-grain
`campaign_hourly` buckets of reconciled rows — and "same answer on a different
machine", the determinism policy's own question, which had a silent "no".

**Detection.** Found **by inspection, not by any test or alert**: the Phase-17
lake → ClickHouse loader's first live parity check compared lake-loaded rows with
the in-memory oracle and found `c-000000` stored as `18:02:51` for a `12:02:51Z`
event. A direct probe (`_tz_probe`, a naive and an aware copy of the same instant
inserted into a `DateTime64(3,'UTC')` column) showed the naive copy stored +6h.
The same mechanism had been running under `reconcile.recover` since Phase 6 — its
rows were read back naive-UTC and inserted naive — and the Phase-12 lakehouse
parity proof passed because **both** sides were shifted equally.

**Root cause.** `clickhouse-connect` is asymmetric at the boundary: it reads a
`DateTime64(3,'UTC')` column back as a **naive** UTC wall-clock, but interprets a
**naive** datetime on insert as the client's **local** wall-clock. See
[`ARCHITECTURE.md` §8](ARCHITECTURE.md#8-gotchas), gotcha "clickhouse-connect
writes a NAIVE datetime as the client's LOCAL wall-clock", and
[`DECISIONS.md`](../DECISIONS.md) (Phase 17, "clickhouse-connect writes a NAIVE
datetime as local wall-clock"). Incident 2's read-side lesson had been applied
only to the one value used for cross-process identity (`reported_at` /
`reconciled_at`); the row timestamps took the same naive round-trip unnoticed.

**Fix.** Since Phase 17 there is ONE writer of the serving tables,
[`lake/load_serving.py`](../lake/load_serving.py): `_utc` makes every datetime
tz-aware UTC before `client.insert` (a naive value is taken as UTC — what the
readers return by contract — an aware value is converted). The engine and the
reconcile job land to the lake (tz-aware `timestamptz`, ms-truncated) and never
call the ClickHouse client with a row; the old direct sink survives only as the
test oracle (`tests/oracle.py`). No committed artifact carries the shift: the
fixtures are model-serialized producer/engine output, and no doc quotes a
reconciled-row timestamp (checked in Phase 17).

**Generalization.** Incident 2, made stronger: **every datetime is tz-aware UTC at
every I/O boundary — a naive datetime never reaches a client call.** Reading
tz-free (epoch millis, server-side arithmetic) protects identity; writing
tz-aware protects content. Offset-invariant metrics are not evidence that row
content is offset-invariant.

**Would catch it next time.** [`tests/test_tz_invariance.py`](../tests/test_tz_invariance.py)
runs the reconcile write path (candidates shaped exactly as the naive-UTC
read-back → `reconcile` → land → lake read → loader values) under `TZ=UTC` and
under two non-UTC zones (`America/Denver`, `Asia/Kolkata`; `time.tzset()`), and asserts the
serialized rows are byte-identical and every datetime handed to the client is
tz-aware UTC. **No alert covers a recurrence**: a uniform shift keeps every
campaign-grain number and every alert rule exactly where it was.

---

## Known limitation — two state stores: a clean stack is not a clean lake

Since Phase 17 the Iceberg lake under `data/lake/<profile>/` is the system of
record and ClickHouse is loaded from it. `make down` removes compose volumes
only; the lake outlives it on purpose (a record as ephemeral as a volume is not a
record). Consequence: after any `make run`, a later `make run-hot` over the same
profile loads the lake's CURRENT rows — which now include that pass's reconciled
corrections — so a hot-only proof shifts its pins, and a second
`make metrics-capture` sees zero reconcile candidates. Seen live in Phase 17
(three of four lakehouse checks failed against a carried-over lake; a
profile-mixed lake at a default root). The rule, everywhere a clean state is
documented: `make down && make lake-reset PROFILE=<p> CONFIRM=yes && make up`,
and the same `PROFILE=<p>` on every step — each entry point binds its own lake
from `--profile` (no default root). The clean-stack `test-int-*` targets do
this; `tests/test_clean_state_chains.py` pins every documented chain. See
[`DECISIONS.md`](../DECISIONS.md) (Phase 17, "A clean stack is a clean lake")
and [`lake/destructive.py`](../lake/destructive.py).

---

## Known limitation — the engine is a batch drain, not a continuous follow

Not a bug — a scope boundary, owned out loud so it isn't rediscovered as a
surprise.

**What is proven.** The attribution engine runs the real windowing semantics —
**arrival-ordered processing, watermarks + allowed-lateness release, and hot-window
eviction** (Phase 5) — over a **bounded drain** of the seeded stream. It drains
both event topics to memory once (EOF-driven, `common.kafka.drain`), resolves
conversions in-process against the compacted device graph, runs the pure core per
household, and exits at end-of-input. The windowing correctness is real and tested;
what it operates on is finite.

**Why it's batch.** It is a deterministic batch attributor in plain Python by
choice since Phase 16: the earlier Bytewax dataflow was a bounded `TestingSource` +
`fold_final` wrapper around this same drain (Bytewax's Kafka source never signals
end-of-input, so a dataflow built directly on it would not terminate on a finite
seeded stream), and the framework did no work, so it was removed rather than made
real. Draining to memory also guarantees every fan-out row for a `conversion_id` is
present when it is collapsed to one row. See [`ARCHITECTURE.md`
§8](ARCHITECTURE.md#8-gotchas), gotcha "The engine is a batch drain, not a
continuous follow", and [`DECISIONS.md`](../DECISIONS.md) (Phase 3/5/16).

**What is not operated.** Three things the batch shape defers, none of them
exercised here:

- **Continuous unbounded Kafka follow** — the engine does not run as a daemon
  tailing the topics; the move to continuous follow, and the framework to run it on
  (Bytewax proper vs Flink), is a Phase-18+ decision.
- **Spill-to-disk / checkpointed state** — the whole topic is held in memory on a
  single partition; there is no RocksDB-style state backend.
- **TTL'd dedup** — dedup is a full in-memory seen-set, not a windowed/TTL'd one.
  The seeded duplicate is timestamp-identical to its original, so a TTL has nothing
  to measure against here; TTL'd eviction is the continuous-follow story only.

Two of the five alerts carry the same batch-mode honesty in their own comments:
`ConsumerLag` is a backlog **proxy**, not live consumer-group lag, and
`WatermarkStall` is a peak event→ingest lateness **proxy**, not a true
watermark-advance stall — because a batch drain has no advancing watermark to
stall against. Read them as batch-mode signals, not continuous-mode ones.

**Where the continuous version is spec'd.** The 500k/sec port maps every engine
construct to its Flink equivalent — RocksDB state backend, incremental
checkpointing, watermarks + `allowedLateness`, late events to a side output — in
[`SCALING.md` — "Flink mapping"](SCALING.md#flink-mapping-500ksec-port). That's the
operational path to lift this limitation; it is documented, not built.
