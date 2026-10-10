"""Send planned emissions to Kafka, and only report success once Kafka has acknowledged them.

The producer is passed in (a `confluent_kafka.Producer` in production, a fake in unit tests),
so delivery failures and back-pressure are tested without a broker.
"""

import logging
from collections.abc import Callable
from typing import Any, Protocol

from confluent_kafka import KafkaError, Message

from shoppulse_contracts import events, wire
from shoppulse_replay.planner import Emission

log = logging.getLogger(__name__)

FAULT_HEADER = "x-replay-faults"  # test ground truth only: pipelines must never branch on it

Headers = list[tuple[str, str | bytes | None]]
# Message is generic only in confluent-kafka's type hints, hence the string annotation.
DeliveryCallback = Callable[[KafkaError | None, "Message[bytes, bytes]"], None]


class DeliveryError(RuntimeError):
    """Kafka did not acknowledge every message; the checkpoint must not move."""


class Producer(Protocol):
    """The subset of `confluent_kafka.Producer` the sink uses (keyword arguments only)."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        headers: Headers,
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def producer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "shoppulse-replay",
        "acks": "all",
        # Idempotence stops retries inside one producer session from duplicating messages.
        # Across restarts, deterministic event IDs + silver dedup cover it (failure mode #1).
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
        # Bounded buffer: if Kafka is down, give up after 2 minutes instead of growing forever.
        "delivery.timeout.ms": 120_000,
        "queue.buffering.max.messages": 100_000,
    }
    # No message timestamp is set, so Kafka records send time. Event time travels in the
    # payload: shifted event times can be in the future, which Kafka 4 rejects by default.


def malformed_value(event_id: str) -> bytes:
    """Deterministic bytes that are not Confluent-framed Avro (first byte is not 0)."""
    return b"malformed:" + event_id.encode()


class KafkaSink:
    def __init__(self, producer: Producer, schema_id: int, topic: str = events.TOPIC) -> None:
        self._producer = producer
        self._schema_id = schema_id
        self._topic = topic
        self._errors: list[str] = []

    def send(self, emission: Emission) -> None:
        record = events.to_record(emission.row)  # invalid rows fail here, before Kafka (#22)
        if "malformed" in emission.faults:
            value = malformed_value(record["event_id"])
        else:
            value = wire.frame(self._schema_id, events.encode(record))
        headers: Headers = (
            [(FAULT_HEADER, ",".join(emission.faults).encode())] if emission.faults else []
        )
        key = events.message_key(record).encode()

        while True:
            try:
                self._producer.produce(
                    self._topic,
                    value=value,
                    key=key,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # Local queue full (Kafka slow or down): wait for deliveries, then retry.
                # This is the back-pressure that keeps the buffer bounded.
                self._producer.poll(1.0)
        self._producer.poll(0)  # serve delivery callbacks

    def flush(self, timeout: float = 130.0) -> None:
        """Block until everything is acknowledged; raise if anything was not."""
        remaining = self._producer.flush(timeout)
        if remaining:
            raise DeliveryError(f"{remaining} messages still unacknowledged after {timeout}s")
        if self._errors:
            errors, self._errors = self._errors, []
            raise DeliveryError(f"{len(errors)} messages failed, first: {errors[0]}")

    def _on_delivery(self, error: KafkaError | None, message: "Message[bytes, bytes]") -> None:
        if error is not None:
            self._errors.append(str(error))
            log.error("delivery failed: %s", error)
