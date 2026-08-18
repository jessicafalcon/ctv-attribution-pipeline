"""Offline coverage for the EOF-driven batch drain (resolve.stage._drain_messages).

No broker: a fake consumer scripts poll() returns. The live integration test
proves the drain end-to-end, but `make test` runs without a broker, so the two
load-bearing invariants — (a) completion only when EVERY partition has emitted
_PARTITION_EOF, (b) a stall raises loud instead of returning a truncated read —
need an offline guard too."""

import pytest
from confluent_kafka import KafkaError

from resolve.stage import _drain_messages


class _FakeErr:
    def __init__(self, code: int, text: str = "boom"):
        self._code = code
        self._text = text

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return self._text


class _FakeMsg:
    def __init__(self, partition: int = 0, value: bytes = b"", error=None):
        self._partition = partition
        self._value = value
        self._error = error

    def error(self):
        return self._error

    def partition(self) -> int:
        return self._partition

    def value(self) -> bytes:
        return self._value


class _PartsMeta:
    def __init__(self, partitions: dict):
        self.partitions = partitions


class _TopicsMeta:
    def __init__(self, topic: str, partitions: dict):
        self.topics = {topic: _PartsMeta(partitions)}


class _FakeConsumer:
    """Scripts poll(); list_topics/assign are inert (no broker)."""

    def __init__(self, partitions: dict, poll_script: list):
        self._partitions = partitions
        self._script = list(poll_script)
        self.assigned = None

    def list_topics(self, topic: str, timeout: float | None = None) -> _TopicsMeta:
        return _TopicsMeta(topic, self._partitions)

    def assign(self, tps: list) -> None:
        self.assigned = tps

    def poll(self, timeout: float | None = None):
        return self._script.pop(0) if self._script else None


def _eof(partition: int) -> _FakeMsg:
    return _FakeMsg(partition, error=_FakeErr(KafkaError._PARTITION_EOF))


def test_drain_returns_data_and_completes_when_all_partitions_eof() -> None:
    d0a, d0b, d1 = _FakeMsg(0, b"a"), _FakeMsg(0, b"b"), _FakeMsg(1, b"c")
    consumer = _FakeConsumer(
        {0: object(), 1: object()},
        [d0a, d0b, _eof(0), d1, _eof(1)],
    )
    # Returns exactly the data messages (EOFs dropped); no raise ⇒ both
    # partitions cleared the pending set before completion.
    assert _drain_messages(consumer, "conversions") == [d0a, d0b, d1]
    assert len(consumer.assigned) == 2  # both partitions assigned at offset 0


def test_drain_raises_on_stall_instead_of_truncating() -> None:
    # poll() always None: one partition never EOFs → stall guard must raise,
    # never return a partial read. This is the #7 loud-failure invariant.
    consumer = _FakeConsumer({0: object()}, [_FakeMsg(0, b"a")])
    with pytest.raises(RuntimeError, match="stalled"):
        _drain_messages(consumer, "conversions")


def test_drain_raises_on_non_eof_error() -> None:
    consumer = _FakeConsumer(
        {0: object()}, [_FakeMsg(0, error=_FakeErr(KafkaError._ALL_BROKERS_DOWN))]
    )
    with pytest.raises(RuntimeError, match="consume error on conversions"):
        _drain_messages(consumer, "conversions")
