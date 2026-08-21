"""Live attribution engine — a deterministic batch attributor (Phase 16).

Teaching notes:
- **Batch drain, with event-time windowing (Phase 5).** We drain both event
  topics start→end once (EOF-driven) and process the finite seeded stream
  in-process, then exit. Within each household bucket the pure core runs an
  arrival-ordered, watermark-gated pass: a conversion is released once the
  event-time watermark (`max(event_time) − allowed_lateness`) reaches its
  `event_time`, and exposures are evicted past `event_time + hot_window`. This is
  windowing **on the batch drain** — the engine stays batch (no continuous Kafka
  follow; that is a Phase-17+ framework decision — ARCHITECTURE §8, DECISIONS
  Phase 5/16).
- **Why no stream framework.** Until Phase 16 this module wrapped the same pass
  in a Bytewax dataflow fed from a bounded `TestingSource`; the framework only
  regrouped lists, so it was removed rather than made real. Continuous follow on
  a real framework (Bytewax proper vs Flink) is chosen later with fresh eyes
  (SCALING.md keeps the Flink mapping as the port target).

The decisions live in streaming/attribute.py (dedup, watermark/release, eviction,
the ambiguous_ip rule); this module only does the household grouping, metrics,
and I/O, calling the SAME leaf functions the offline replay calls, so the two
paths cannot diverge.
"""

import argparse
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from confluent_kafka import Consumer
from prometheus_client import REGISTRY, start_http_server, write_to_textfile

from common.kafka import drain
from lake.land_attributed import land_attributed
from lake.land_exposures import land
from producer.models import (
    AttributedConversion,
    Conversion,
    Exposure,
    ResolvedConversion,
)
from resolve import metrics as resolve_metrics
from resolve.graph_loader import load_graph_index
from resolve.resolver import resolve_one
from streaming import metrics
from streaming.attribute import (
    ALLOWED_LATENESS,
    HOT_WINDOW,
    attribute_household_streaming,
    candidate_households_by_conversion,
    dedup_streams,
    one_row_per_conversion,
)

EXPOSURES_TOPIC = "exposures"
CONVERSIONS_TOPIC = "conversions"


@dataclass(frozen=True)
class EngineRun:
    """One drain's output, in memory: the deduped exposures and the attributed
    rows — what the lake receives (`land_run`) and what the parity oracle
    compares against."""

    exposures: list[Exposure]
    rows: list[AttributedConversion]
    resolved: int
    suppressed: int


def _allowed_lateness() -> timedelta:
    """Release/eviction grace from the env, defaulting to ALLOWED_LATENESS. Must
    be ≥ the seeded profile's late.max_minutes (asserted per profile in tests);
    override with ENGINE_ALLOWED_LATENESS_MINUTES."""
    minutes = os.environ.get("ENGINE_ALLOWED_LATENESS_MINUTES")
    return timedelta(minutes=int(minutes)) if minutes else ALLOWED_LATENESS


def _drain_topic(broker: str, topic: str, group: str) -> list[bytes]:
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,  # drain completes on EOF, not row count
        }
    )
    try:
        return drain(consumer, topic)
    finally:
        consumer.close()


def run_attribution(
    exposures: list[Exposure],
    resolved: list[ResolvedConversion],
    allowed_lateness: timedelta = ALLOWED_LATENESS,
) -> list[AttributedConversion]:
    """The engine over in-memory (already deduped) streams: one row per
    conversion (`one_row_per_conversion`), bucket each household's interleaved
    exposures + conversions, run the watermark-gated evicting pass per
    household, record the join-state and per-row metrics, return the rows sorted
    by `conversion_id`. Pure apart from the metrics; the same function serves the
    live run and every offline oracle test."""
    events: dict[str, list[tuple[str, Exposure | ResolvedConversion]]] = defaultdict(
        list
    )
    for e in exposures:
        events[e.household_id].append(("exp", e))
    # The fan-out's candidate set, captured BEFORE the collapse to one placeholder
    # row — persisted on the deferred row as candidate_households (Phase 17).
    candidates = candidate_households_by_conversion(resolved)
    for r in one_row_per_conversion(resolved):
        events[r.household_id].append(("res", r))

    rows: list[AttributedConversion] = []
    for household_events in events.values():
        result = attribute_household_streaming(
            household_events, HOT_WINDOW, allowed_lateness, candidates
        )
        metrics.observe_state(result.state)
        for cand in result.candidates:
            metrics.observe(cand.row)
            rows.append(cand.row)
    return sorted(rows, key=lambda r: r.conversion_id)


def resolve_conversions(
    broker: str, conversions: list[Conversion]
) -> list[ResolvedConversion]:
    """The in-process resolve step (Phase 16): load the graph from the compacted
    topic, map every conversion through `resolve_one` (device hit, IP fallback,
    shared-IP fan-out), emit the `resolve_` metrics from this process. Same
    function, same metrics as the old stage — minus the topic."""
    index = load_graph_index(broker)
    # Backlog the consumer started behind by: the whole topic, since the drain
    # assigns from OFFSET_BEGINNING each pass (batch proxy for consumer lag).
    resolve_metrics.observe_backlog(len(conversions))
    resolved: list[ResolvedConversion] = []
    for conv in conversions:
        rows = resolve_one(conv, index)
        resolve_metrics.observe(rows)
        resolved.extend(rows)
    return resolved


def run_engine(broker: str) -> EngineRun:
    """Drain the two event topics, resolve conversions in-process, run the engine
    to completion — and return the result WITHOUT writing it. Pure apart from
    the metrics and the drain, so the integration oracle can call it to get the
    rows the old direct sink would have written (Phase 17)."""
    exposures_raw = [
        Exposure.model_validate_json(v)
        for v in _drain_topic(broker, EXPOSURES_TOPIC, "engine-exposures")
    ]
    resolved_raw = resolve_conversions(
        broker,
        [
            Conversion.model_validate_json(v)
            for v in _drain_topic(broker, CONVERSIONS_TOPIC, "engine-conversions")
        ],
    )
    # Dedup (feature 1): drop exact re-sends via a full seen-set before the join
    # and before landing. Full set, not a TTL — the drain holds the whole topic
    # and the seeded duplicate is timestamp-identical (DECISIONS Phase 5).
    # ENGINE_DEDUP=off skips it (dedup is transparent: RMT collapses re-sends on
    # read regardless, so FINAL is identical either way — proven by the
    # dedup-off integration run).
    if os.environ.get("ENGINE_DEDUP", "on").lower() == "off":
        exposures, resolved, suppressed = exposures_raw, resolved_raw, 0
    else:
        exposures, resolved, suppressed = dedup_streams(exposures_raw, resolved_raw)
    metrics.DEDUP_SUPPRESSED.inc(suppressed)
    # Peak arrival lateness over the processed events (engine-side, so the pure
    # core stays a function of (events, window) only — BACKLOG 24). Dedup drops
    # only timestamp-identical re-sends, so the peak is the same pre/post dedup.
    peak_lateness = max(
        (
            (e.ingest_time - e.event_time).total_seconds()
            for e in (*exposures, *resolved)
        ),
        default=0.0,
    )
    metrics.observe_watermark_lag(peak_lateness)
    rows = run_attribution(exposures, resolved, _allowed_lateness())
    return EngineRun(exposures, rows, len(resolved), suppressed)


def land_run(run: EngineRun) -> set[str]:
    """Land the run in the lake of record — raw.exposures + raw.attributed_
    conversions — and return the event_time days touched, which the Dagster load
    then materializes into ClickHouse (spec D5/D6). The engine never writes
    ClickHouse rows."""
    days = land(run.exposures) | land_attributed(run.rows)
    metrics.EXPOSURES_LANDED.inc(len(run.exposures))
    return days


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics-port", type=int, default=None)
    parser.add_argument(
        "--metrics-out",
        default=None,
        help="dump this stage's terminal Prometheus registry to a textfile "
        "(promtool-fixture provenance; see make metrics-capture)",
    )
    args = parser.parse_args(argv)
    broker = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
    if args.metrics_port:
        start_http_server(args.metrics_port, addr="127.0.0.1")
    # Imported here, not at module top: the offline oracle suites import this
    # module and must not pay for (or depend on) the Dagster stack.
    from orchestration.load import materialize_load

    run = run_engine(broker)
    days = land_run(run)
    loaded = materialize_load(days)
    print(
        f"engine: {len(run.exposures)} exposures, {run.resolved} resolved "
        f"({run.suppressed} re-sends deduped) → lake raw.exposures + "
        f"raw.attributed_conversions ({len(days)} day(s)) → ClickHouse "
        f"({loaded['exposures']} exposures, {loaded['attributed']} attributed)"
    )
    if args.metrics_out:
        write_to_textfile(args.metrics_out, REGISTRY)


if __name__ == "__main__":
    main()
