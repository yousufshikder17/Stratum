"""Shared CSV plumbing for the file-backed adapters (spec §3.4).

The CSV adapters exist so the whole pipeline — adapter -> guard -> store ->
factor engine — can be exercised end to end with **synthetic local fixtures
and no network**. They are also the reference implementation of the two rules
that every file-backed source must obey:

1. **A knowledge stamp is a column, not a default.** These adapters declare
   ``knowledge_time_basis = publication``, so the guard demands a real
   source-provided stamp. Where a file omits one, the adapter derives it from
   a *documented, configurable* publication convention (a bar is knowable at
   its session close) and never from ``now`` — see
   :func:`session_close_knowledge_time`. A file that omits the stamp *and*
   has no such convention is a configuration error, not a defaulted row.

2. **Ids are derived from content**, so re-reading a file is idempotent
   rather than duplicating (spec §3.2 rule 4). See
   :func:`stratum.schema.ids.deterministic_ulid`.
"""

from __future__ import annotations

import csv
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path

from stratum.schema.times import EventTime, KnowledgeTime

__all__ = [
    "ENTITY_COLUMNS",
    "CsvCache",
    "CsvFormatError",
    "day_start",
    "optional_date",
    "optional_float",
    "read_csv",
    "require_date",
    "require_entity",
    "require_float",
    "session_close_knowledge_time",
]


class CsvFormatError(ValueError):
    """A CSV is missing required columns or holds an unparseable value.

    Raised rather than skipping the row: a silently dropped price or
    membership record is a survivorship bug that surfaces months later.
    """


#: Accepted aliases naming the security a row is about. The adapter keeps the
#: value verbatim as ``native_entity``; canonical identity is the resolver's
#: job, never the adapter's (spec §3.2 rule 2).
ENTITY_COLUMNS = ("security", "ticker", "symbol")


def read_csv(
    path: Path, *, required: Sequence[str | tuple[str, ...]]
) -> Iterator[Mapping[str, str]]:
    """Yield rows as ``{lower-cased column: stripped value}``.

    An entry in ``required`` may be a tuple, meaning "at least one of these".
    Blank lines are skipped; a missing required column fails the whole file up
    front rather than row by row, because a half-read price file is worse than
    no price file.
    """
    if not path.exists():
        raise CsvFormatError(f"csv not found: {path}")
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        header = [name.strip().lower() for name in (reader.fieldnames or [])]
        missing = [
            group
            for group in required
            if not (set(group) & set(header) if isinstance(group, tuple) else group in header)
        ]
        if missing:
            raise CsvFormatError(
                f"{path.name} is missing required column(s) {missing} — found {header}"
            )
        flat = [
            name for group in required for name in ((group,) if isinstance(group, str) else group)
        ]
        for raw in reader:
            row = {
                (key or "").strip().lower(): (value or "").strip()
                for key, value in raw.items()
                if key is not None
            }
            if any(row.get(name) for name in flat):
                yield row


def require_entity(row: Mapping[str, str], *, origin: str) -> str:
    """The row's native entity key, from whichever alias the file uses."""
    for column in ENTITY_COLUMNS:
        value = row.get(column, "")
        if value:
            return value
    raise CsvFormatError(f"{origin}: one of {list(ENTITY_COLUMNS)} must name the security")


def require_date(row: Mapping[str, str], column: str, *, origin: str) -> date:
    value = row.get(column, "")
    if not value:
        raise CsvFormatError(f"{origin}: column {column!r} is required and must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise CsvFormatError(f"{origin}: column {column!r} is not an ISO date: {value!r}") from exc


def optional_date(row: Mapping[str, str], column: str, *, origin: str) -> date | None:
    if not row.get(column):
        return None
    return require_date(row, column, origin=origin)


def require_float(row: Mapping[str, str], column: str, *, origin: str) -> float:
    value = row.get(column, "")
    if not value:
        raise CsvFormatError(f"{origin}: column {column!r} is required and must be numeric")
    try:
        return float(value)
    except ValueError as exc:
        raise CsvFormatError(f"{origin}: column {column!r} is not numeric: {value!r}") from exc


def optional_float(row: Mapping[str, str], column: str, *, origin: str) -> float | None:
    if not row.get(column):
        return None
    return require_float(row, column, origin=origin)


def day_start(day: date) -> EventTime:
    """The event-time axis for daily data: midnight UTC on the record's date."""
    return EventTime.at(datetime(day.year, day.month, day.day, tzinfo=UTC))


def session_close_knowledge_time(day: date, close: time) -> KnowledgeTime:
    """When a daily record for ``day`` became knowable: that day's session close.

    This is the documented publication convention for daily bars — the value
    is actionable once the session settles, not at midnight on the bar date
    (which would let a strategy trade on a close it could not have seen) and
    not at ingestion time (which would be the backfill bug). ``close`` is
    configurable per market so the convention stays explicit rather than
    hardcoded to one exchange.
    """
    return KnowledgeTime.at(
        datetime(day.year, day.month, day.day, close.hour, close.minute, tzinfo=UTC)
    )


@dataclass(frozen=True, slots=True)
class _Stamp:
    """What makes a cached parse stale: the file changed size or mtime."""

    mtime_ns: int
    size: int

    @classmethod
    def of(cls, path: Path) -> _Stamp:
        stat = path.stat()
        return cls(mtime_ns=stat.st_mtime_ns, size=stat.st_size)


class CsvCache[T]:
    """Parse a CSV once, re-parse when the file actually changes.

    A single ``backfill()`` used to walk each file several times — once for
    ``capabilities()``, once per record kind — re-reading and re-parsing every
    row each pass. Caching removes that.

    The invalidation is the load-bearing part. ``poll()`` exists to pick up
    rows appended since the last pass, so a cache that never expired would
    quietly turn incremental polling into "read the file once at startup and
    never notice another row again" — a worse bug than the waste it fixed.
    Keying on (mtime, size) means an appended file re-parses on the next call.
    """

    __slots__ = ("_parse", "_stamp", "_value")

    def __init__(self, parse: Callable[[Path], T]) -> None:
        self._parse = parse
        self._stamp: _Stamp | None = None
        self._value: T | None = None

    def get(self, path: Path) -> T:
        if not path.exists():
            raise CsvFormatError(f"csv not found: {path}")
        stamp = _Stamp.of(path)
        if self._stamp != stamp or self._value is None:
            self._value = self._parse(path)
            self._stamp = stamp
        return self._value

    def invalidate(self) -> None:
        self._stamp = None
        self._value = None
