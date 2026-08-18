"""Live attribution engine (Bytewax) — the real pipeline component.

Teaching notes (first appearance this phase):
- **Stateful keyed operators.** Bytewax processes a stream of `(key, value)`
  pairs; a *stateful* operator keeps state per key. `fold_final` accumulates all
  values for a key and emits once the input is exhausted — exactly right for a
  bounded batch. We key by `household_id` to group each household's exposures
  and resolved conversions together (the join), then re-key by `conversion_id`
  to collapse a shared-IP fan-out to one winner (the reduction).
- **Batch drain, not continuous follow.** Like the resolve stage, we drain both
  Kafka topics start→end once (EOF-driven) and feed them into the dataflow via a
  bounded source, so the engine processes the finite seeded stream and exits.
  Bytewax's Kafka *source* follows forever; draining to memory first is the
  simplest correct batch shape at this scale and lets `fold_final` see every
  candidate for a `conversion_id` together (required for the reduction —
  DECISIONS Phase 3 (b)). Continuous mode with windowing lands in Phase 5.

The decisions live in streaming/attribute.py; this module only does the keyed
grouping and I/O, calling the SAME leaf functions the offline replay calls, so
the two paths cannot diverge.
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
from prometheus_client import start_http_server

from clickhouse.apply import apply as apply_ddl
from common.kafka import drain
from producer.models import Exposure, ResolvedConversion
from streaming import metrics
from streaming.attribute import (
    ALLOWED_LATENESS,
    HOT_WINDOW,
    attribute_household_streaming,
    dedup_streams,
    reduce_conversion,
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


def _attribute_group(allowed_lateness: timedelta, kv: tuple[str, list]) -> list:
    _hid, events = kv
    return attribute_household_streaming(events, HOT_WINDOW, allowed_lateness)


def _collect(acc: list, candidate: object) -> list:
    acc.append(candidate)
    return acc


def _reduce_and_observe(kv: tuple[str, list]):
    _cid, candidates = kv
    row = reduce_conversion(candidates)
    metrics.observe(row, len(candidates))
    return row


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
    res_stream = op.input("resolved", flow, TestingSource(resolved, _BATCH))

    # Join: tag, merge, key by household_id, collect each household's interleaved
    # events; the pure streaming stage re-sorts to arrival order and releases.
    exp_tagged = op.map("tag_exp", exp_stream, lambda e: ("exp", e))
    res_tagged = op.map("tag_res", res_stream, lambda r: ("res", r))
    merged = op.merge("merge_household", exp_tagged, res_tagged)
    keyed = op.key_on("key_household", merged, lambda t: t[1].household_id)
    grouped = op.fold_final("group_household", keyed, list, _accumulate)
    candidates = op.flat_map(
        "attribute_household", grouped, partial(_attribute_group, allowed_lateness)
    )

    # Reduction: re-key by conversion_id, collapse fan-out/duplicates to one row.
    cand_keyed = op.key_on("key_conversion", candidates, lambda c: c.row.conversion_id)
    collected = op.fold_final("group_conversion", cand_keyed, list, _collect)
    winners = op.map("reduce_conversion", collected, _reduce_and_observe)
    op.output("sink_attributed", winners, attributed_sink)

    # Land raw exposures for reconciliation + the naive benchmark.
    landed = op.map("count_exposures", exp_stream, _count_exposure)
    op.output("land_exposures", landed, exposures_sink)

    return flow


def run_engine(broker: str) -> dict[str, int]:
    """Drain the two topics, apply the DDL, run the engine to completion.
    Returns row counts for logging/tests."""
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
    exposures, resolved, suppressed = dedup_streams(exposures_raw, resolved_raw)
    metrics.DEDUP_SUPPRESSED.inc(suppressed)
    run_main(
        build_flow(
            exposures,
            resolved,
            ClickHouseSink(insert_attributed),
            ClickHouseSink(insert_exposures),
            _allowed_lateness(),
        )
    )
    return {
        "exposures": len(exposures),
        "resolved": len(resolved),
        "suppressed": suppressed,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics-port", type=int, default=None)
    args = parser.parse_args(argv)
    broker = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
    if args.metrics_port:
        start_http_server(args.metrics_port, addr="127.0.0.1")
    counts = run_engine(broker)
    print(
        f"engine: {counts['exposures']} exposures, {counts['resolved']} resolved "
        f"({counts['suppressed']} re-sends deduped) "
        f"→ attributed_conversions + exposures_landed"
    )


if __name__ == "__main__":
    main()
