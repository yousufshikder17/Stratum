"""Deterministic observation ids (spec §4.3): the idempotency key."""

from datetime import UTC, datetime

import pytest

from stratum.schema.ids import CROCKFORD, deterministic_ulid

MARCH = int(datetime(2024, 3, 1, tzinfo=UTC).timestamp() * 1000)
APRIL = int(datetime(2024, 4, 1, tzinfo=UTC).timestamp() * 1000)


def test_ulid_shape() -> None:
    ulid = deterministic_ulid(timestamp_ms=MARCH, key="market_csv|AAA|2024-03-01")
    assert len(ulid) == 26
    assert set(ulid) <= set(CROCKFORD)


def test_same_key_same_id() -> None:
    """The whole point: re-reading a file must not duplicate rows."""
    args = {"timestamp_ms": MARCH, "key": "market_csv|market.bar|AAA|2024-03-01"}
    assert deterministic_ulid(**args) == deterministic_ulid(**args)


def test_different_key_different_id() -> None:
    first = deterministic_ulid(timestamp_ms=MARCH, key="AAA")
    second = deterministic_ulid(timestamp_ms=MARCH, key="BBB")
    assert first != second
    # Same millisecond, so only the entropy half may differ.
    assert first[:10] == second[:10]


def test_ids_sort_in_event_order() -> None:
    """ULIDs are time-sortable; the timestamp half must dominate the entropy."""
    earlier = deterministic_ulid(timestamp_ms=MARCH, key="zzz")
    later = deterministic_ulid(timestamp_ms=APRIL, key="aaa")
    assert earlier < later


def test_timestamp_must_fit_48_bits() -> None:
    with pytest.raises(ValueError, match="48 bits"):
        deterministic_ulid(timestamp_ms=1 << 48, key="x")
