"""Live attribution engine (Bytewax) — the real pipeline component.

Teaching notes:
- **Stateful keyed operators.** Bytewax processes a stream of `(key, value)`
  pairs; a *stateful* operator keeps state per key. `fold_final` accumulates all
  values for a key and emits once the input is exhausted — exactly right for a
  bounded batch. We key by `household_id` to bucket each household's interleaved
  exposures and conversions together (the join). A shared-IP fan-out is
  collapsed to one placeholder row BEFORE the join (`one_row_per_conversion`)
  and emitted unattributed (ambiguous_ip) — no second keyed stage (Phase 16).
- **Batch drain, with event-time windowing (Phase 5).** We drain both Kafka
  topics start→end once (EOF-driven) and feed a bounded source, so the engine
  processes the finite seeded stream and exits (Bytewax's Kafka *source* follows
  forever). Within each household bucket the pure core runs an arrival-ordered,
  watermark-gated pass: a conversion is released once the event-time watermark
  (`max(event_time) − allowed_lateness`) reaches its `event_time`, and exposures
  are evicted past `event_time + hot_window`. This is windowing **on the batch
  drain** — the engine stays batch (no continuous Kafka follow; that is deferred,
  no phase owns it — ARCHITECTURE §8, DECISIONS Phase 5).

The decisions live in streaming/attribute.py (dedup, watermark/release, eviction,
the ambiguous_ip rule); this module only does the keyed grouping, metrics, and
I/O, calling the SAME leaf functions the offline replay calls, so the two paths
cannot diverge.
"""

import argparse
import os
from datetime import timedelta
from functools import partial

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.outputs import Sink
from bytewax.testing import TestingSource, run_main
from confluent_kafka import Consumer
from prometheus_client import REGISTRY, start_http_server, write_to_textfile

from clickhouse.apply import apply as apply_ddl
from common.kafka import drain
from producer.models import Exposure, ResolvedConversion
from streaming import metrics
from streaming.attribute import (
    ALLOWED_LATENESS,
    HOT_WINDOW,
    attribute_household_streaming,
    dedup_streams,
    one_row_per_conversion,
)
from streaming.sink import ClickHouseSink, insert_attributed, insert_exposures

EXPOSURES_TOPIC = "exposures"
RESOLVED_TOPIC = "conversions_resolved"
_BATCH = 256  # items per source poll → fewer, larger ClickHouse inserts


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


def _accumulate(acc: list, tagged: tuple[str, object]) -> list:
    """Collect one household's interleaved (kind, model) events; the streaming
    stage re-sorts them into arrival order before the watermark-gated pass."""
    acc.append(tagged)
    return acc


def _attribute_group(allowed_lateness: timedelta, kv: tuple[str, list]):
    _hid, events = kv
    return attribute_household_streaming(events, HOT_WINDOW, allowed_lateness)


def _emit_and_observe(result) -> list:
    """Record the household's join-state metrics (peak, evictions), count each
    final row, and emit it — one row per conversion_id by construction."""
    metrics.observe_state(result.state)
    rows = [c.row for c in result.candidates]
    for row in rows:
        metrics.observe(row)
    return rows


def _count_exposure(exposure: Exposure) -> Exposure:
    metrics.EXPOSURES_LANDED.inc()
    return exposure


def build_flow(
    exposures: list[Exposure],
    resolved: list[ResolvedConversion],
    attributed_sink: Sink,
    exposures_sink: Sink,
    allowed_lateness: timedelta = ALLOWED_LATENESS,
) -> Dataflow:
    """Wire the engine. Sinks are injected so the same operator graph runs
    against ClickHouse live and against a capturing sink in an offline test —
    proving the Bytewax path matches the pure core without needing services.

    Bytewax carries the keyed state (one bucket of interleaved events per
    household); the watermark-gated release lives in the pure core
    (`attribute_household_streaming`), so live and replay cannot diverge."""
    flow = Dataflow("attribution-engine")

    exp_stream = op.input("exposures", flow, TestingSource(exposures, _BATCH))
    res_stream = op.input(
        "resolved", flow, TestingSource(one_row_per_conversion(resolved), _BATCH)
    )

    # Join: tag, merge, key by household_id, collect each household's interleaved
    # events; the pure streaming stage re-sorts to arrival order and releases.
    exp_tagged = op.map("tag_exp", exp_stream, lambda e: ("exp", e))
    res_tagged = op.map("tag_res", res_stream, lambda r: ("res", r))
    merged = op.merge("merge_household", exp_tagged, res_tagged)
    keyed = op.key_on("key_household", merged, lambda t: t[1].household_id)
    grouped = op.fold_final("group_household", keyed, list, _accumulate)
    results = op.map(
        "attribute_household", grouped, partial(_attribute_group, allowed_lateness)
    )
    rows = op.flat_map("emit_rows", results, _emit_and_observe)
    op.output("sink_attributed", rows, attributed_sink)

    # Land raw exposures for reconciliation + the naive benchmark.
    landed = op.map("count_exposures", exp_stream, _count_exposure)
    op.output("land_exposures", landed, exposures_sink)

    return flow


def run_engine(broker: str, lake_land: bool = False) -> dict[str, int]:
    """Drain the two topics, apply the DDL, run the engine to completion.
    Returns row counts for logging/tests.

    `lake_land` (make lake-land only; off for make run/CI) dual-writes the SAME
    deduped exposure list this run feeds to ClickHouse into the Iceberg lake, so
    the two copies share one input set by construction (DECISIONS Phase 12). Off by
    default keeps the engine path byte-identical and the lake stack out of every
    other run."""
    apply_ddl()
    exposures_raw = [
        Exposure.model_validate_json(v)
        for v in _drain_topic(broker, EXPOSURES_TOPIC, "engine-exposures")
    ]
    resolved_raw = [
        ResolvedConversion.model_validate_json(v)
        for v in _drain_topic(broker, RESOLVED_TOPIC, "engine-resolved")
    ]
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
    run_main(
        build_flow(
            exposures,
            resolved,
            ClickHouseSink(insert_attributed),
            ClickHouseSink(insert_exposures),
            _allowed_lateness(),
        )
    )
    # Dual-write the exact same deduped list into the Iceberg lake (make lake-land
    # only). This is the sole landing site — make run/CI never pass lake_land — so
    # there is no double-land; a re-run is harmless anyway (dedup-on-read).
    if lake_land:
        from lake.land_exposures import land

        land(exposures)
    return {
        "exposures": len(exposures),
        "resolved": len(resolved),
        "suppressed": suppressed,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics-port", type=int, default=None)
    parser.add_argument(
        "--metrics-out",
        default=None,
        help="dump this stage's terminal Prometheus registry to a textfile "
        "(promtool-fixture provenance; see make metrics-capture)",
    )
    parser.add_argument(
        "--lake-land",
        action="store_true",
        help="also append the deduped exposures to the Iceberg lake "
        "(make lake-land; Phase 12). Off for make run/CI.",
    )
    args = parser.parse_args(argv)
    broker = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
    if args.metrics_port:
        start_http_server(args.metrics_port, addr="127.0.0.1")
    counts = run_engine(broker, lake_land=args.lake_land)
    print(
        f"engine: {counts['exposures']} exposures, {counts['resolved']} resolved "
        f"({counts['suppressed']} re-sends deduped) "
        f"→ attributed_conversions + exposures_landed"
        + (" + raw.exposures (lake)" if args.lake_land else "")
    )
    if args.metrics_out:
        write_to_textfile(args.metrics_out, REGISTRY)


if __name__ == "__main__":
    main()
