"""The two clocks (spec §4.2) — plus the store's audit clock and the query scope.

``EventTime`` (when the thing happened) and ``KnowledgeTime`` (when it became
knowable) are deliberately **distinct, non-interchangeable types**. They cannot
be compared to each other, passed for one another, or silently coerced: the
distinction is enforced by the type system, not by convention. The only bridge
between the axes is :func:`publication_lag`, which returns a plain
``timedelta`` diagnostic.

The engine rule (spec §4.4): every read is scoped by an :class:`AsOf` on the
knowledge axis, and a row is visible iff ``knowledge_time.knowable(as_of)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import final

__all__ = [
    "AsOf",
    "EventTime",
    "IngestTime",
    "KnowledgeTime",
    "publication_lag",
]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_NS_PER_S = 1_000_000_000


def _ns_from_datetime(dt: datetime, *, field: str) -> int:
    if dt.tzinfo is None:
        raise ValueError(
            f"{field} requires a timezone-aware datetime; naive timestamps are "
            "ambiguous and forbidden (all Stratum times canonicalize to UTC)"
        )
    delta = dt.astimezone(UTC) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * _NS_PER_S + delta.microseconds * 1_000


def _datetime_from_ns(ns: int) -> datetime:
    return _EPOCH + timedelta(seconds=ns // _NS_PER_S, microseconds=(ns % _NS_PER_S) // 1_000)


@final
@dataclass(frozen=True, order=True, slots=True)
class EventTime:
    """When the underlying real-world event occurred (filing period end, post
    timestamp, search day, image capture). The x-axis factors align on.

    Ordered only against other ``EventTime`` values; comparing against
    ``KnowledgeTime`` raises ``TypeError``.
    """

    ns: int

    @classmethod
    def at(cls, dt: datetime) -> EventTime:
        return cls(_ns_from_datetime(dt, field="event_time"))

    def as_datetime(self) -> datetime:
        return _datetime_from_ns(self.ns)


@final
@dataclass(frozen=True, order=True, slots=True)
class KnowledgeTime:
    """The earliest wall-clock time the value could have been **acted upon**
    (EDGAR acceptance datetime, post creation, provider vintage release).

    This is the selection axis: the engine reads only rows where
    ``knowledge_time <= as_of`` (spec §4.2). Adapters must set it honestly —
    stamping historical rows with "now" is a leakage bug rejected at ingest
    (spec §3.6).
    """

    ns: int

    @classmethod
    def at(cls, dt: datetime) -> KnowledgeTime:
        return cls(_ns_from_datetime(dt, field="knowledge_time"))

    def as_datetime(self) -> datetime:
        return _datetime_from_ns(self.ns)

    def knowable(self, as_of: AsOf) -> bool:
        """The single PIT selection rule: was this value knowable at ``as_of``?"""
        return self.ns <= as_of.ns


@final
@dataclass(frozen=True, order=True, slots=True)
class IngestTime:
    """When Stratum physically stored the row. Set by the core, never by an
    adapter. Audit/debug only — **never used for selection** (spec §4.2).
    """

    ns: int

    @classmethod
    def at(cls, dt: datetime) -> IngestTime:
        return cls(_ns_from_datetime(dt, field="ingest_time"))

    def as_datetime(self) -> datetime:
        return _datetime_from_ns(self.ns)


@final
@dataclass(frozen=True, order=True, slots=True)
class AsOf:
    """A query scope on the knowledge axis: "as it was known on `as_of`".

    Every store read and every factor/backtest evaluation is parameterized by
    exactly one of these; there is no unscoped read (spec §4.4, §5.4).
    """

    ns: int

    @classmethod
    def at(cls, dt: datetime) -> AsOf:
        return cls(_ns_from_datetime(dt, field="as_of"))

    @classmethod
    def from_knowledge_time(cls, kt: KnowledgeTime) -> AsOf:
        return cls(kt.ns)

    def as_datetime(self) -> datetime:
        return _datetime_from_ns(self.ns)

    def less_embargo(self, embargo: timedelta) -> AsOf:
        """``as_of = t - embargo`` (spec §5.4, §6.1): the engine's decision lag."""
        return AsOf(self.ns - int(embargo.total_seconds() * _NS_PER_S))


def publication_lag(event_time: EventTime, knowledge_time: KnowledgeTime) -> timedelta:
    """``knowledge_time - event_time`` (spec §4.2).

    Recorded and surfaced as a diagnostic so researchers can model actionable
    delay explicitly (e.g., fundamentals knowable ~45 days after period end).
    May be negative for forecast-like signals that carry a ``forecast_horizon``.
    """
    delta_ns = knowledge_time.ns - event_time.ns
    return timedelta(seconds=delta_ns // _NS_PER_S, microseconds=(delta_ns % _NS_PER_S) // 1_000)
