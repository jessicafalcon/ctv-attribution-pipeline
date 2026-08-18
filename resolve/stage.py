"""Live resolve stage: the real pipeline component.

Teaching notes (first appearance this phase):
- **Compacted topic**: `device_graph` keeps only the last message per key
  (household_id), so replaying it start→end reconstructs the current graph
  without every historical edit. We drain it once at startup into a GraphIndex.
- **Consumer group / offsets**: a Kafka consumer reads a partition sequentially
  by *offset*. We drain `conversions` start→end in one batch pass — assign every
  partition at offset 0 and read until each has signalled end-of-partition
  (`_PARTITION_EOF`, enabled via `enable.partition.eof`), then produce a resolved
  record per candidate to `conversions_resolved`. EOF-driven completion (not a
  watermark row count) is correct on a *compacted* topic too, where compaction
  leaves offset gaps that would make `high - low` overcount and hang. Batch (not
  follow-forever) is deliberate for Phase 2: it processes the finite seeded
  stream end-to-end and exits. Continuous follow lands when `make run` wires the
  whole pipeline (Phase 3+).

Both keys land matchable events together: `conversions_resolved` is keyed by
`household_id`, the same key as `exposures`, so the engine's join is partition-
local. Every produced payload is re-validated against ResolvedConversion (the
validate-on-produce contract).
"""

import argparse
import os

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from confluent_kafka import Producer as KafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import start_http_server

from producer.models import Conversion, Household, ResolvedConversion
from producer.schemas import register_subject
from producer.serialize import canonical_bytes
from resolve import metrics
from resolve.index import GraphIndex
from resolve.resolver import resolve_one

RESOLVED_TOPIC = "conversions_resolved"
RESOLVED_SUBJECT = f"{RESOLVED_TOPIC}-value"
# Stall guard only: a healthy drain ends on EOF, not on this. ~5s of dead air
# before every partition has reached end-of-log means the broker stalled → raise
# loud, never return a partial read.
_EMPTY_POLL_LIMIT = 50


def _ensure_topic(broker: str) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    for future in admin.create_topics(
        [NewTopic(RESOLVED_TOPIC, num_partitions=1)]
    ).values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def _drain_messages(consumer: Consumer, topic: str) -> list[Message]:
    """Read `topic` start→end once, driven by end-of-partition; return the raw
    messages (callers pick key and/or value). Completion is when EVERY assigned
    partition has emitted `_PARTITION_EOF` — the standard read-to-end-of-log
    idiom, correct on compacted topics with offset gaps. Requires the consumer
    to set `enable.partition.eof`. If the empty-poll stall guard trips before
    all partitions reach EOF, raise (loud) rather than return a truncated read —
    the consumer must fail as loudly as the producer does."""
    parts = list(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in parts])
    pending = set(parts)  # partitions not yet at end-of-log

    messages: list[Message] = []
    empty = 0
    while pending and empty < _EMPTY_POLL_LIMIT:
        msg = consumer.poll(0.1)
        if msg is None:
            empty += 1
            continue
        empty = 0
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                pending.discard(msg.partition())
                continue
            raise RuntimeError(f"consume error on {topic}: {msg.error()}")
        messages.append(msg)
    if pending:
        raise RuntimeError(
            f"drain of {topic} stalled: {len(pending)} partition(s) never reached "
            f"end-of-log within {_EMPTY_POLL_LIMIT} empty polls"
        )
    return messages


def _drain(consumer: Consumer, topic: str) -> list[bytes]:
    """Read `topic` start→end once; return message values (batch path)."""
    return [msg.value() for msg in _drain_messages(consumer, topic)]


def load_graph_index(broker: str) -> GraphIndex:
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "resolve-graph-loader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,  # drain completes on EOF, not row count
        }
    )
    try:
        households = [
            Household.model_validate_json(v) for v in _drain(consumer, "device_graph")
        ]
    finally:
        consumer.close()
    return GraphIndex.from_households(households)


def run_batch(
    broker: str, registry: str, group_id: str = "resolve-stage"
) -> tuple[int, int]:
    """One pass over `conversions`. Returns (consumed, emitted)."""
    register_subject(registry, RESOLVED_SUBJECT, ResolvedConversion)
    _ensure_topic(broker)
    index = load_graph_index(broker)

    producer = KafkaProducer({"bootstrap.servers": broker, "enable.idempotence": True})
    errors: list[str] = []

    def on_delivery(err: object, _msg: object) -> None:
        if err is not None:
            errors.append(str(err))

    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,  # drain completes on EOF, not row count
        }
    )
    consumed = emitted = 0
    try:
        for value in _drain(consumer, "conversions"):
            conv = Conversion.model_validate_json(value)
            resolved = resolve_one(conv, index)
            metrics.observe(resolved)
            consumed += 1
            for record in resolved:
                payload = canonical_bytes(record)
                ResolvedConversion.model_validate_json(payload)  # validate on produce
                producer.produce(
                    RESOLVED_TOPIC,
                    key=record.household_id.encode(),
                    value=payload,
                    on_delivery=on_delivery,
                )
                emitted += 1
    finally:
        consumer.close()
    undelivered = producer.flush(60)
    if errors or undelivered:
        raise RuntimeError(
            f"resolve produce failed: {len(errors)} errors, {undelivered} undelivered"
        )
    return consumed, emitted


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics-port", type=int, default=None)
    args = parser.parse_args(argv)
    broker = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081")
    if args.metrics_port:
        # 127.0.0.1 only, matching the compose ports — keep metrics off the LAN.
        start_http_server(args.metrics_port, addr="127.0.0.1")
    consumed, emitted = run_batch(broker, registry)
    print(f"resolve: consumed {consumed} conversions → emitted {emitted} records")


if __name__ == "__main__":
    main()
