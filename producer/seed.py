"""Entrypoint for `make seed`: generate, register schemas, publish, mirror.

Every payload is re-validated against its model just before produce (the
"validate on produce" contract). All emitted bytes are mirrored to
data/out/<profile>/ so byte-identity between runs is checkable with diff.
Truth links go only to data/truth/<profile>/ — the pipeline never reads them.
"""

import argparse
import os
from pathlib import Path

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from producer.config import Profile, load_profile
from producer.generate import GeneratedStream, generate
from producer.schemas import TOPIC_MODELS, register_schemas
from producer.serialize import canonical_bytes, jsonl

REPO_ROOT = Path(__file__).parent.parent


def create_topics(admin: AdminClient) -> None:
    topics = [
        NewTopic("exposures", num_partitions=1),
        NewTopic("conversions", num_partitions=1),
        NewTopic(
            "device_graph", num_partitions=1, config={"cleanup.policy": "compact"}
        ),
    ]
    for future in admin.create_topics(topics).values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def produce_all(producer: Producer, stream: GeneratedStream) -> int:
    """Produce graph + events; returns message count. Validates every payload."""

    def send(topic: str, key: str, event) -> None:
        payload = canonical_bytes(event)
        TOPIC_MODELS[topic].model_validate_json(payload)  # validate on produce
        producer.produce(topic, key=key.encode(), value=payload)

    n = 0
    for household in stream.graph.households:
        send("device_graph", household.household_id, household)
        n += 1
    for exposure in stream.exposures:
        send("exposures", exposure.household_id, exposure)
        n += 1
    for conversion in stream.conversions:
        send("conversions", conversion.device_id, conversion)
        n += 1
    producer.flush()
    return n


def write_mirrors(profile: Profile, stream: GeneratedStream) -> None:
    out = REPO_ROOT / "data" / "out" / profile.name
    out.mkdir(parents=True, exist_ok=True)
    (out / "device_graph.jsonl").write_text(jsonl(stream.graph.households))
    (out / "exposures.jsonl").write_text(jsonl(stream.exposures))
    (out / "conversions.jsonl").write_text(jsonl(stream.conversions))
    (out / "truth_links.jsonl").write_text(jsonl(stream.truth_links))

    truth_dir = REPO_ROOT / "data" / "truth" / profile.name
    truth_dir.mkdir(parents=True, exist_ok=True)
    (truth_dir / "truth_links.jsonl").write_text(jsonl(stream.truth_links))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    args = parser.parse_args(argv)

    profile = load_profile(args.profile)
    seed = int(os.environ.get("PRODUCER_SEED", profile.seed))
    # 127.0.0.1, not localhost: the compose ports bind IPv4 only, so rdkafka
    # logs a connect-refused for ::1 before falling back. (One such line can
    # still appear post-bootstrap — the broker advertises localhost:19092.)
    broker = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081")

    stream = generate(profile, seed)
    schema_ids = register_schemas(registry)
    create_topics(AdminClient({"bootstrap.servers": broker}))
    n = produce_all(Producer({"bootstrap.servers": broker}), stream)
    write_mirrors(profile, stream)

    caused = len(stream.truth_links)
    print(
        f"seed={seed} profile={profile.name}: {n} messages produced "
        f"({len(stream.graph.households)} graph, {len(stream.exposures)} exposures, "
        f"{len(stream.conversions)} conversions, {caused} truth links). "
        f"schemas: {schema_ids}"
    )


if __name__ == "__main__":
    main()
