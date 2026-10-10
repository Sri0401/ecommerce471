"""Confluent wire format: what actually travels in a Kafka message value.

    byte 0      magic byte, always 0
    bytes 1-4   schema ID in the registry, big-endian
    bytes 5-    Avro binary body (no embedded schema)

Every Confluent-compatible client (Spark, Kafka Connect, kcat) reads this framing, so
consumers can look the writer schema up by ID.
"""

import struct

MAGIC = 0
_HEADER = struct.Struct(">bI")  # signed byte + unsigned 32-bit int, big-endian


class WireFormatError(ValueError):
    """The bytes are not a Confluent-framed Avro message (route to the DLQ)."""


def frame(schema_id: int, body: bytes) -> bytes:
    return _HEADER.pack(MAGIC, schema_id) + body


def unframe(data: bytes) -> tuple[int, bytes]:
    """Return (schema_id, avro_body); raise WireFormatError if the framing is wrong."""
    if len(data) < _HEADER.size:
        raise WireFormatError(f"message too short for wire header: {len(data)} bytes")
    magic, schema_id = _HEADER.unpack_from(data)
    if magic != MAGIC:
        raise WireFormatError(f"unknown magic byte {magic}")
    return schema_id, data[_HEADER.size :]
