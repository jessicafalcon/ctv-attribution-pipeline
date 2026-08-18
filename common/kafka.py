"""Shared Kafka batch-drain: read a topic start→end once, driven by
end-of-partition. Used by both the resolve stage and the attribution engine —
each processes a finite seeded stream end-to-end and exits, so both drain a
topic to the end of its log rather than following forever.

Teaching notes (consumer group / offsets): a Kafka consumer reads a partition
sequentially by *offset*. We assign every partition at offset 0 and read until
each has signalled end-of-partition (`_PARTITION_EOF`, enabled via
`enable.partition.eof`). EOF-driven completion (not a watermark row count) is
correct on a *compacted* topic too, where compaction leaves offset gaps that
would make `high - low` overcount and hang.
"""

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    Message,
    TopicPartition,
)

# Stall guard only: a healthy drain ends on EOF, not on this. ~5s of dead air
# before every partition has reached end-of-log means the broker stalled → raise
# loud, never return a partial read.
_EMPTY_POLL_LIMIT = 50


def drain_messages(consumer: Consumer, topic: str) -> list[Message]:
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


def drain(consumer: Consumer, topic: str) -> list[bytes]:
    """Read `topic` start→end once; return message values (batch path)."""
    return [msg.value() for msg in drain_messages(consumer, topic)]
