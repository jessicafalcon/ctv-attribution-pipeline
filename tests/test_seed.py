"""produce_all and create_topics against fakes — no broker, no network."""

from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from producer.config import load_profile
from producer.generate import generate
from producer.seed import create_topics, produce_all
from producer.serialize import canonical_bytes


class FakeProducer:
    def __init__(self, fail_every: int = 0) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.fail_every = fail_every

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.sent.append((topic, key, value))
        fail = self.fail_every and len(self.sent) % self.fail_every == 0
        on_delivery(KafkaError(KafkaError._MSG_TIMED_OUT) if fail else None, None)

    def flush(self, timeout: float) -> int:
        return 0


def test_produce_all_keys_and_payloads() -> None:
    stream = generate(load_profile("tiny"), 42)
    producer = FakeProducer()
    n = produce_all(producer, stream)  # type: ignore[arg-type]
    assert n == len(producer.sent)

    by_topic: dict[str, list[tuple[bytes, bytes]]] = {}
    for topic, key, value in producer.sent:
        by_topic.setdefault(topic, []).append((key, value))
    assert len(by_topic["device_graph"]) == len(stream.graph.households)
    assert len(by_topic["exposures"]) == len(stream.exposures)
    assert len(by_topic["conversions"]) == len(stream.conversions)

    # Keying contract Phase 2's co-partitioning depends on.
    for (key, value), exposure in zip(
        by_topic["exposures"], stream.exposures, strict=True
    ):
        assert key == exposure.household_id.encode()
        assert value == canonical_bytes(exposure)
    for (key, value), conversion in zip(
        by_topic["conversions"], stream.conversions, strict=True
    ):
        assert key == conversion.device_id.encode()
        assert value == canonical_bytes(conversion)
    for (key, _), household in zip(
        by_topic["device_graph"], stream.graph.households, strict=True
    ):
        assert key == household.household_id.encode()


def test_produce_all_raises_on_delivery_failure() -> None:
    stream = generate(load_profile("tiny"), 42)
    with pytest.raises(RuntimeError, match="delivery errors"):
        produce_all(FakeProducer(fail_every=10), stream)  # type: ignore[arg-type]


class FakeFuture:
    def __init__(self, exc: Exception | None) -> None:
        self.exc = exc

    def result(self) -> None:
        if self.exc is not None:
            raise self.exc


class FakeAdmin:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc
        self.requested: list[Any] = []

    def create_topics(self, topics: list[Any]) -> dict[str, FakeFuture]:
        self.requested = topics
        return {t.topic: FakeFuture(self.exc) for t in topics}


def test_create_topics_compacts_device_graph() -> None:
    admin = FakeAdmin()
    create_topics(admin)  # type: ignore[arg-type]
    by_name = {t.topic: t for t in admin.requested}
    assert set(by_name) == {"exposures", "conversions", "device_graph"}
    assert by_name["device_graph"].config == {"cleanup.policy": "compact"}


def test_producer_seed_env_reaches_generate(monkeypatch: pytest.MonkeyPatch) -> None:
    import producer.seed as seed_module

    captured: dict[str, Any] = {}

    def fake_generate(profile: Any, seed: int) -> Any:
        captured["seed"] = seed
        return generate(profile, seed)

    monkeypatch.setattr(seed_module, "generate", fake_generate)
    monkeypatch.setattr(seed_module, "register_schemas", lambda url: {})
    monkeypatch.setattr(seed_module, "create_topics", lambda admin: None)
    monkeypatch.setattr(seed_module, "produce_all", lambda producer, stream: 0)
    monkeypatch.setattr(seed_module, "write_mirrors", lambda profile, stream: None)
    monkeypatch.setattr(seed_module, "AdminClient", lambda cfg: None)
    monkeypatch.setattr(seed_module, "Producer", lambda cfg: None)

    monkeypatch.delenv("PRODUCER_SEED", raising=False)
    seed_module.main(["--profile", "tiny"])
    assert captured["seed"] == load_profile("tiny").seed  # default: profile's seed

    monkeypatch.setenv("PRODUCER_SEED", "7")
    seed_module.main(["--profile", "tiny"])
    assert captured["seed"] == 7


def test_create_topics_tolerates_existing_only() -> None:
    exists = KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
    create_topics(FakeAdmin(exc=exists))  # type: ignore[arg-type]
    other = KafkaException(KafkaError(KafkaError.INVALID_CONFIG))
    with pytest.raises(KafkaException):
        create_topics(FakeAdmin(exc=other))  # type: ignore[arg-type]
