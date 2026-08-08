"""The two clocks are distinct at the type level (spec §4.2)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from stratum.schema.times import AsOf, EventTime, KnowledgeTime, publication_lag

UTC = UTC
T0 = datetime(2024, 3, 1, 12, 0, tzinfo=UTC)


def test_naive_datetimes_are_rejected() -> None:
    naive = datetime(2024, 3, 1, 12, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        EventTime.at(naive)
    with pytest.raises(ValueError, match="timezone-aware"):
        KnowledgeTime.at(naive)
    with pytest.raises(ValueError, match="timezone-aware"):
        AsOf.at(naive)


def test_event_and_knowledge_time_are_not_comparable() -> None:
    et = EventTime.at(T0)
    kt = KnowledgeTime.at(T0)
    with pytest.raises(TypeError):
        _ = et < kt  # type: ignore[operator]
    with pytest.raises(TypeError):
        _ = kt <= et  # type: ignore[operator]


def test_event_and_knowledge_time_are_never_equal() -> None:
    # Same instant, different axes: not interchangeable, not equal.
    assert EventTime.at(T0) != KnowledgeTime.at(T0)


def test_ordering_within_one_axis_works() -> None:
    assert EventTime.at(T0) < EventTime.at(T0 + timedelta(days=1))
    assert KnowledgeTime.at(T0) < KnowledgeTime.at(T0 + timedelta(seconds=1))


def test_pit_selection_rule() -> None:
    kt = KnowledgeTime.at(T0)
    assert kt.knowable(AsOf.at(T0))  # knowable exactly at as_of
    assert kt.knowable(AsOf.at(T0 + timedelta(days=1)))
    assert not kt.knowable(AsOf.at(T0 - timedelta(microseconds=1)))


def test_as_of_embargo_shifts_backwards() -> None:
    as_of = AsOf.at(T0)
    shifted = as_of.less_embargo(timedelta(days=1))
    assert shifted.as_datetime() == T0 - timedelta(days=1)


def test_publication_lag_is_a_plain_timedelta() -> None:
    et = EventTime.at(T0)
    kt = KnowledgeTime.at(T0 + timedelta(days=45))
    assert publication_lag(et, kt) == timedelta(days=45)
    # Forecast-like signals may have negative lag; it must round-trip exactly.
    kt_early = KnowledgeTime.at(T0 - timedelta(hours=6))
    assert publication_lag(et, kt_early) == timedelta(hours=-6)


def test_utc_canonicalization() -> None:
    est = timezone(timedelta(hours=-5))
    assert EventTime.at(T0) == EventTime.at(T0.astimezone(est))
