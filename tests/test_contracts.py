import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from fastavro.schema import fingerprint, to_parsing_canonical_form

from shoppulse_contracts import events

# Pinned fingerprint of the v1 schema's Parsing Canonical Form (structure only: names, types,
# order; docs and defaults are not part of it). If this test fails, v1 was edited in place:
# create commerce_event.v2.avsc instead, and check it is BACKWARD compatible (ADR-003).
V1_FINGERPRINT = "5a36dbee351e01557a02e5a255f8505fdebeb2a3a514dd3167ab314176571316"


def _rees46_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "event_id": "a" * 64,
        "store_id": "rees46",
        "event_time": datetime(2026, 10, 11, 9, 30, tzinfo=UTC),
        "source_event_time": datetime(2020, 2, 16, 9, 30, tzinfo=UTC),  # extra: ignored
        "event_type": "purchase",
        "product_id": 1005105,
        "category_id": 2053013555631882655,
        "category_code": "electronics.smartphone",
        "brand": "apple",
        "price": Decimal("1302.48"),
        "user_id": 556695836,
        "user_session": "ca5eefc5-11f9-450c-91ed-380285a0bc80",
    }
    row.update(overrides)
    return row


def test_v1_schema_is_frozen() -> None:
    canonical = to_parsing_canonical_form(json.loads(events.schema_text()))
    assert fingerprint(canonical, "SHA-256") == V1_FINGERPRINT


def test_record_round_trips_through_avro() -> None:
    record = events.to_record(_rees46_row())
    decoded = events.decode(events.encode(record))
    assert decoded == record
    assert "source_event_time" not in decoded


def test_store_without_price_or_category_is_valid() -> None:
    # OTTO-shaped event: no price, category or brand (ADR-001 second store).
    record = events.to_record(
        _rees46_row(store_id="otto", price=None, category_id=None, category_code=None, brand=None)
    )
    assert events.decode(events.encode(record))["price"] is None


def test_message_key_groups_a_users_events() -> None:
    assert events.message_key(events.to_record(_rees46_row())) == "rees46:556695836"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"event_time": datetime(2026, 10, 11, 9, 30)}, "timezone-aware"),
        ({"event_type": "click"}, "unknown event_type"),
        ({"price": 1302.48}, "Decimal"),
        ({"price": Decimal("1.005")}, "two decimal places"),
    ],
)
def test_invalid_rows_are_rejected_before_kafka(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        events.to_record(_rees46_row(**overrides))


def test_missing_field_is_rejected() -> None:
    row = _rees46_row()
    del row["user_id"]
    with pytest.raises(ValueError, match="missing fields"):
        events.to_record(row)


def test_trailing_zero_prices_are_normalised() -> None:
    record = events.to_record(_rees46_row(price=Decimal("5")))
    assert record["price"].as_tuple().exponent == -2  # Decimal("5") == Decimal("5.00") anyway
