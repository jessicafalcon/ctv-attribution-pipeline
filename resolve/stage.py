"""Live resolve stage: the real pipeline component.

Teaching notes (first appearance this phase):
- **Compacted topic**: `device_graph` keeps only the last message per key
  (household_id), so replaying it start→end reconstructs the current graph
  without every historical edit. We drain it once at startup into a GraphIndex.
- **Consumer group / offsets**: a Kafka consumer reads a partition sequentially
  by *offset*. We drain `conversions` from the log's low to high watermark in
  one batch pass, then produce a resolved record per candidate to
  `conversions_resolved`. Batch (not follow-forever) is deliberate for Phase 2:
  it processes the finite seeded stream end-to-end and exits, which is what the
  integration test and a tiny/medium run need. Continuous follow lands when
  `make run` wires the whole pipeline (Phase 3+).

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
_EMPTY_POLL_LIMIT = 50  # ~5s of silence past the watermark → stop the batch pass


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


def _drain(consumer: Consumer, topic: str) -> list[bytes]:
    """Read `topic` from low to high watermark once; return message values."""
    parts = list(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
    remaining = 0
    assignment = []
    for p in parts:
        lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        remaining += hi - lo
        assignment.append(TopicPartition(topic, p, OFFSET_BEGINNING))
    consumer.assign(assignment)

    values: list[bytes] = []
    empty = 0
    while remaining > 0 and empty < _EMPTY_POLL_LIMIT:
        msg = consumer.poll(0.1)
        if msg is None:
            empty += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(f"consume error on {topic}: {msg.error()}")
        empty = 0
        remaining -= 1
        values.append(msg.value())
    return values


def load_graph_index(broker: str) -> GraphIndex:
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "resolve-graph-loader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
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
        start_http_server(args.metrics_port)
    consumed, emitted = run_batch(broker, registry)
    print(f"resolve: consumed {consumed} conversions → emitted {emitted} records")


if __name__ == "__main__":
    main()
