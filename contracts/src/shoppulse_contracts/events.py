"""The `CommerceEvent` v1 contract.

`to_record` is the single place where a store-specific row becomes a canonical event, so
every producer fails in the same way on bad input, before anything reaches Kafka.
"""

import functools
import io
import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from importlib import resources
from typing import Any, cast

import fastavro

TOPIC = "shoppulse.events.v1"
DLQ_TOPIC = "shoppulse.events.dlq.v1"
SUBJECT = f"{TOPIC}-value"  # registry subject (TopicNameStrategy: one per topic value)
SCHEMA_FILE = "commerce_event.v1.avsc"
EVENT_TYPES = frozenset({"view", "cart", "remove_from_cart", "purchase"})
FIELDS = (
    "event_id",
    "store_id",
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
)
PRICE_EXPONENT = Decimal("0.01")  # scale 2 in the schema


def schema_text() -> str:
    """The raw .avsc text, as registered in the schema registry."""
    return (
        resources.files("shoppulse_contracts").joinpath("schemas", SCHEMA_FILE).read_text("utf-8")
    )


@functools.cache
def parsed_schema() -> dict[str, Any]:
    # fastavro ships no type hints; a record schema always parses to a dict.
    return cast(dict[str, Any], fastavro.parse_schema(json.loads(schema_text())))


def message_key(record: Mapping[str, Any]) -> str:
    """Kafka key: all events of one user land in one partition, in order (sessionisation)."""
    return f"{record['store_id']}:{record['user_id']}"


def to_record(row: Mapping[str, Any]) -> dict[str, Any]:
    """Build a canonical record from a row with (at least) the contract fields.

    Raises ValueError for anything the schema would accept but the platform must not:
    naive timestamps (ambiguous time zone), unknown event types, float prices, or prices
    with more than two decimal places.
    """
    missing = [f for f in FIELDS if f not in row]
    if missing:
        raise ValueError(f"missing fields: {missing}")

    event_time = row["event_time"]
    if not isinstance(event_time, datetime) or event_time.tzinfo is None:
        raise ValueError(f"event_time must be a timezone-aware datetime, got {event_time!r}")

    if row["event_type"] not in EVENT_TYPES:
        raise ValueError(f"unknown event_type {row['event_type']!r}")

    price = row["price"]
    if price is not None:
        if not isinstance(price, Decimal):
            raise ValueError(f"price must be Decimal or None, got {type(price).__name__}")
        if price != price.quantize(PRICE_EXPONENT):
            raise ValueError(f"price {price} has more than two decimal places")
        price = price.quantize(PRICE_EXPONENT)

    record = {field: row[field] for field in FIELDS}
    record["price"] = price
    return record


def encode(record: Mapping[str, Any]) -> bytes:
    """Avro binary body (no registry framing; the Kafka serializer adds that)."""
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, parsed_schema(), dict(record))
    return buffer.getvalue()


def decode(payload: bytes) -> dict[str, Any]:
    result = fastavro.schemaless_reader(io.BytesIO(payload), parsed_schema(), None)
    if not isinstance(result, dict):
        raise ValueError("payload did not decode to a record")
    return result
