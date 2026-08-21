"""Device-graph loader for the in-process resolve step (Phase 16).

Teaching note — **compacted topic**: `device_graph` keeps only the last message
per key (household_id), so replaying it start→end reconstructs the current
graph without every historical edit. The engine drains it once at startup into
a `GraphIndex` and resolves each conversion in-process with
`resolve.resolver.resolve_one` — the Phase-2 function signature is the seam.

Until Phase 16 this module (then `resolve/stage.py`) was a separate consumer
that republished resolved records to a `conversions_resolved` topic. That topic,
its schema-registry subject and the producer path are gone: a second consumer
group + topic + subject bought nothing for an in-memory dict lookup. Resolve
becomes a separate service again when the device graph is owned by another team
or a vendor; the interface is the function, not the topic (DECISIONS Phase 2/16).
"""

from confluent_kafka import Consumer

from common.kafka import drain
from producer.models import Household
from resolve.index import GraphIndex


def load_graph_index(broker: str) -> GraphIndex:
    """Drain the compacted `device_graph` topic once into a lookup index."""
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
            Household.model_validate_json(v) for v in drain(consumer, "device_graph")
        ]
    finally:
        consumer.close()
    return GraphIndex.from_households(households)
