from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from shoppulse_contracts import events, wire
from shoppulse_replay import runner
from shoppulse_replay.checkpoint import CheckpointStore
from shoppulse_replay.kafka_sink import FAULT_HEADER, DeliveryError, KafkaSink
from shoppulse_replay.planner import Emission, Planner
from shoppulse_replay.scenario import Scenario

T0 = datetime(2026, 10, 11, tzinfo=UTC)


def _row(i: int = 0) -> dict[str, Any]:
    return {
        "event_id": f"{i:064x}",
        "store_id": "rees46",
        "event_time": T0,
        "event_type": "cart",
        "product_id": 1005105,
        "category_id": 2053013555631882655,
        "category_code": "electronics.smartphone",
        "brand": "apple",
        "price": Decimal("1302.48"),
        "user_id": 556695836,
        "user_session": "s1",
    }


def _emission(faults: tuple[Any, ...] = (), i: int = 0) -> Emission:
    return Emission(T0, i, _row(i), i, faults)


class FakeProducer:
    """Records produce() calls; can fail deliveries or report a full local queue."""

    def __init__(self, fail_delivery: bool = False, buffer_full_times: int = 0) -> None:
        self.messages: list[dict[str, Any]] = []
        self.polls = 0
        self._pending: list[Callable[[Any, Any], None]] = []
        self._fail = fail_delivery
        self._buffer_full = buffer_full_times

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        headers: list[tuple[str, str | bytes | None]],
        on_delivery: Callable[[Any, Any], None],
    ) -> None:
        if self._buffer_full:
            self._buffer_full -= 1
            raise BufferError("Local: Queue full")
        self.messages.append({"topic": topic, "value": value, "key": key, "headers": headers})
        self._pending.append(on_delivery)

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float) -> int:
        for callback in self._pending:
            callback("Broker: Not enough in-sync replicas" if self._fail else None, None)
        self._pending = []
        return 0


def test_message_is_confluent_framed_avro_with_user_key() -> None:
    producer = FakeProducer()
    KafkaSink(producer, schema_id=7).send(_emission())
    message = producer.messages[0]
    schema_id, body = wire.unframe(message["value"])
    assert schema_id == 7
    assert events.decode(body)["event_id"] == _row()["event_id"]
    assert message["key"] == b"rees46:556695836"
    assert message["topic"] == events.TOPIC
    assert message["headers"] == []


def test_faults_are_labelled_in_a_header() -> None:
    producer = FakeProducer()
    KafkaSink(producer, schema_id=7).send(_emission(("late", "duplicate")))
    assert producer.messages[0]["headers"] == [(FAULT_HEADER, b"late,duplicate")]


def test_malformed_fault_sends_bytes_that_fail_unframing() -> None:
    producer = FakeProducer()
    KafkaSink(producer, schema_id=7).send(_emission(("malformed",)))
    with pytest.raises(wire.WireFormatError):
        wire.unframe(producer.messages[0]["value"])


def test_full_local_queue_applies_back_pressure_then_sends() -> None:
    producer = FakeProducer(buffer_full_times=3)
    KafkaSink(producer, schema_id=7).send(_emission())
    assert len(producer.messages) == 1
    assert producer.polls >= 3  # waited for deliveries between retries


def test_failed_delivery_raises_on_flush() -> None:
    sink = KafkaSink(FakeProducer(fail_delivery=True), schema_id=7)
    sink.send(_emission())
    with pytest.raises(DeliveryError, match="in-sync replicas"):
        sink.flush()


def test_checkpoint_does_not_move_when_kafka_rejects_messages(tmp_path: Path) -> None:
    # Failure mode #2: Kafka unavailable -> replay stops, checkpoint stays where it was.
    store = CheckpointStore(tmp_path / "checkpoint.json")
    sink = KafkaSink(FakeProducer(fail_delivery=True), schema_id=7)
    with pytest.raises(DeliveryError):
        runner.run(
            [_row(i) for i in range(10)], Planner(Scenario(name="x")), sink, store, speed=None
        )
    assert store.load() is None
