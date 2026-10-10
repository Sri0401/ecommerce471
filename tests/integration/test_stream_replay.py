"""Replay a small dataset into the real local Kafka + Karapace and read it back.

Run with `make up-stream && make register && make test-integration`.
"""

import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from shoppulse_contracts import events, wire
from shoppulse_contracts.registry import RegistryClient
from shoppulse_replay import runner
from shoppulse_replay.checkpoint import CheckpointStore
from shoppulse_replay.kafka_sink import FAULT_HEADER, KafkaSink, producer_config
from shoppulse_replay.planner import Planner
from shoppulse_replay.scenario import Scenario

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("SHOPPULSE_KAFKA", "localhost:9092")
REGISTRY = os.environ.get("SHOPPULSE_REGISTRY", "http://localhost:8081")
T0 = datetime(2026, 10, 11, tzinfo=UTC)


def _rows(n: int) -> list[dict[str, Any]]:
    run = uuid.uuid4().hex[:8]  # unique IDs per test run
    return [
        {
            "event_id": f"{run}{i:056x}",
            "store_id": "rees46",
            "event_time": T0 + timedelta(seconds=i),
            "event_type": "view",
            "product_id": 1000 + i,
            "category_id": 1,
            "category_code": None,
            "brand": None,
            "price": Decimal("1.00"),
            "user_id": i % 25,
            "user_session": None,
        }
        for i in range(n)
    ]


def _end_offsets(consumer: Consumer) -> list[TopicPartition]:
    partitions = consumer.list_topics(events.TOPIC, timeout=10).topics[events.TOPIC].partitions
    offsets = []
    for p in partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(events.TOPIC, p), timeout=10)
        offsets.append(TopicPartition(events.TOPIC, p, high))
    return offsets


def _consume_from(start: list[TopicPartition], expected: int) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"it-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    consumer.assign(start)
    messages: list[Any] = []
    deadline = datetime.now() + timedelta(seconds=30)
    while len(messages) < expected and datetime.now() < deadline:
        message = consumer.poll(1.0)
        if message is not None and message.error() is None:
            messages.append(message)
    consumer.close()
    return messages


def test_chaos_replay_round_trips_through_kafka(tmp_path: Path) -> None:
    schema_id = RegistryClient(REGISTRY).latest_id(events.SUBJECT)
    probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "it-probe"})
    start = _end_offsets(probe)
    probe.close()

    rows = _rows(500)
    scenario = Scenario(name="it", seed=5, duplicate_rate=0.05, malformed_rate=0.02)

    sink = KafkaSink(Producer(producer_config(BOOTSTRAP)), schema_id)
    sent = runner.run(
        rows, Planner(scenario), sink, CheckpointStore(tmp_path / "c.json"), speed=None
    )

    messages = _consume_from(start, sent)
    assert len(messages) == sent

    good, malformed, duplicates = [], 0, 0
    for message in messages:
        faults = dict(message.headers() or []).get(FAULT_HEADER, b"").decode()
        try:
            schema, body = wire.unframe(message.value())
        except wire.WireFormatError:
            malformed += 1
            assert "malformed" in faults
            continue
        assert schema == schema_id
        record = events.decode(body)
        assert message.key().decode() == events.message_key(record)
        duplicates += "duplicate" in faults
        good.append(record["event_id"])

    assert malformed > 0
    assert len(good) - len(set(good)) == duplicates  # every extra copy is labelled
    assert set(good) == {r["event_id"] for r in rows}  # nothing lost


def test_one_user_always_lands_in_one_partition(tmp_path: Path) -> None:
    schema_id = RegistryClient(REGISTRY).latest_id(events.SUBJECT)
    probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "it-probe"})
    start = _end_offsets(probe)
    probe.close()

    sink = KafkaSink(Producer(producer_config(BOOTSTRAP)), schema_id)
    sent = runner.run(
        _rows(200),
        Planner(Scenario(name="it")),
        sink,
        CheckpointStore(tmp_path / "c.json"),
        speed=None,
    )

    partitions_per_key: dict[bytes, set[int]] = {}
    for message in _consume_from(start, sent):
        partitions_per_key.setdefault(message.key(), set()).add(message.partition())
    assert len(partitions_per_key) == 25
    assert all(len(p) == 1 for p in partitions_per_key.values())
