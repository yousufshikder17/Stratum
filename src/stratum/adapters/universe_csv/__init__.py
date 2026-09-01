"""Point-in-time universe CSV adapter (spec §3.4, §6.3, MVP item 5).

Supplies historical index/listing membership so the backtest universe at date
``D`` reflects what existed *then* — including since-delisted names, excluding
not-yet-listed ones. The survivorship-correct universe is non-negotiable (spec
§9 risk 3): a "current constituents" list applied to history is the single
most common way a backtest lies to you, so this adapter **refuses** a flat
membership file. The format requires effective-dated intervals.

**Four dates, two axes.** Each membership row carries an effective interval on
the event axis and, separately, when each end of that interval was *published*
on the knowledge axis:

=========================  ================================================
``added_on``               event — the day the name entered the index
``announced_on``           knowledge — when the addition was published
``removed_on``             event — the day it left (blank = still a member)
``removed_announced_on``   knowledge — when the removal was published
=========================  ================================================

Index changes are announced days or weeks before they take effect, so the axes
genuinely differ: on 2024-01-25 you may already know that a name joins on
2024-02-01. Both ends need their own stamp — a removal is announced separately
from the addition, and letting it inherit the addition's date would back-date
knowledge by years. A row with ``removed_on`` and no ``removed_announced_on``
is therefore rejected rather than defaulted.

:meth:`CsvUniverseAdapter.members_as_of` reads both axes, which produces the
one result people find surprising and is nonetheless correct: a removal that
has taken effect but has **not yet been announced** does not remove the name,
because as of that date nobody knew. Ignoring the knowledge axis leaks the
future; ignoring the event axis resurrects dead companies.

Membership corrections append new vintages rather than overwriting
(``supports_restatement = true``), keyed by the announcement date that made
each edge knowable.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from stratum.adapters.base import (
    CapabilityManifest,
    TimeWindow,
    UniverseAdapter,
    UniverseSpec,
)
from stratum.adapters.context import AdapterContext
from stratum.adapters.csv_common import (
    ENTITY_COLUMNS,
    CsvCache,
    CsvFormatError,
    day_start,
    optional_date,
    read_csv,
    require_date,
    require_entity,
)
from stratum.adapters.manifest import load_manifest
from stratum.schema.data_class import DataClass
from stratum.schema.ids import deterministic_ulid
from stratum.schema.observation import Observation
from stratum.schema.payloads import MetaCoverage
from stratum.schema.times import KnowledgeTime

__all__ = ["CsvUniverseAdapter", "MembershipInterval"]

_MANIFEST_PATH = Path(__file__).with_name("manifest.toml")

#: A file with an entity column but no ``added_on`` is a current-constituents
#: list. Naming the mistake makes the rejection actionable.
_FLAT_LIST_HINT = (
    "this looks like a flat current-constituents list. Applying today's index "
    "membership to history is survivorship bias (spec §6.3, §9 risk 3) and is "
    "rejected, not warned about. Supply effective-dated intervals instead: "
    "columns security, added_on, announced_on, and — for names that left — "
    "removed_on with removed_announced_on."
)


def _midnight(day: date) -> KnowledgeTime:
    return KnowledgeTime.at(datetime(day.year, day.month, day.day, tzinfo=UTC))


@dataclass(frozen=True, kw_only=True, slots=True)
class MembershipInterval:
    """One effective-dated membership record, stamped on both axes."""

    security: str
    added_on: date
    announced_on: date
    removed_on: date | None = None
    removed_announced_on: date | None = None

    def __post_init__(self) -> None:
        if self.removed_on is not None and self.removed_announced_on is None:
            raise CsvFormatError(
                f"{self.security}: removed_on {self.removed_on} needs "
                "removed_announced_on — a removal is announced separately from the "
                "addition, and inheriting the addition's stamp would back-date "
                "knowledge of the removal"
            )

    def known_at(self, day: date) -> bool:
        """Had the *addition* been announced by ``day``? (knowledge axis)"""
        return self.announced_on <= day

    def removal_in_effect_at(self, day: date) -> bool:
        """Has the removal both taken effect **and** been announced by ``day``?

        Both halves are required. A removal that has happened but has not been
        published is not yet knowable, and a published removal that has not
        taken effect has not happened.
        """
        if self.removed_on is None or self.removed_announced_on is None:
            return False
        return self.removed_on <= day and self.removed_announced_on <= day

    def member_at(self, day: date) -> bool:
        """Was the name in the index on ``day``, as far as anyone knew then?"""
        if not self.known_at(day) or day < self.added_on:
            return False
        return not self.removal_in_effect_at(day)


class CsvUniverseAdapter(UniverseAdapter):
    """Historical index membership from an effective-dated CSV."""

    ADAPTER_ID = "universe_csv"

    def __init__(self) -> None:
        self._ctx: AdapterContext | None = None
        self._path: Path | None = None
        self._universe_id = ""
        self._source_id = self.ADAPTER_ID
        self._manifest = load_manifest(_MANIFEST_PATH)
        self._polled_through_ns: int | None = None
        #: `capabilities()`, `members_as_of()` and `backfill()` each walk the
        #: membership file; parse it once per change instead.
        self._cache: CsvCache[list[MembershipInterval]] = CsvCache(self._parse_intervals)

    # -- lifecycle -------------------------------------------------------------

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        membership = config.get("membership_path")
        if not membership:
            raise CsvFormatError("universe_csv requires 'membership_path'")
        universe_id = config.get("universe_id")
        if not universe_id:
            raise CsvFormatError(
                "universe_csv requires 'universe_id' — a membership history "
                "defines exactly one named universe (e.g. sp1500_pit)"
            )
        self._ctx = ctx
        self._path = Path(membership)
        if not self._path.exists():
            raise CsvFormatError(f"membership_path does not exist: {self._path}")
        self._universe_id = str(universe_id)
        self._source_id = str(config.get("source_id", self.ADAPTER_ID))

    def capabilities(self) -> CapabilityManifest:
        intervals = list(self._read_intervals())
        return CapabilityManifest(
            signal_types=("meta.coverage",),
            native_frequency="sparse",
            supports_restatement=True,  # membership corrections append vintages
            coverage_start=min((i.added_on for i in intervals), default=None),
            entities=sorted({i.security for i in intervals}),
        )

    # -- the survivorship-correct lookup (spec §3.4, §6.3) ---------------------

    async def members_as_of(self, as_of: date, definition: UniverseSpec) -> list[str]:
        """Constituents **known** as of ``as_of``.

        Includes names later delisted or removed; excludes names not yet
        added, and changes not yet announced. ``as_of`` is read on both axes:
        it is the knowledge cut *and* the membership date.
        """
        if definition.id != self._universe_id:
            raise CsvFormatError(
                f"this adapter serves universe {self._universe_id!r}, not {definition.id!r}"
            )
        return sorted(
            {interval.security for interval in self._read_intervals() if interval.member_at(as_of)}
        )

    # -- ingestion -------------------------------------------------------------

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        """Membership changes as ``meta.coverage`` observations.

        The store keeps the membership history itself; these rows record what
        the universe knew and when, so a run manifest can prove which
        membership vintage a backtest used.
        """
        start, end = window.start.date(), window.end.date()
        for observation in self._iter_observations():
            day = observation.event_time.as_datetime().date()
            if start <= day < end:
                yield observation

    async def poll(self) -> AsyncIterator[Observation]:
        """Membership changes announced since the last poll."""
        now_ns = KnowledgeTime.at(self._clock()).ns
        floor = self._polled_through_ns
        high_water = floor
        for observation in self._iter_observations():
            stamp_ns = observation.knowledge_time.ns
            if stamp_ns > now_ns:
                continue
            if floor is not None and stamp_ns <= floor:
                continue
            high_water = stamp_ns if high_water is None else max(high_water, stamp_ns)
            yield observation
        self._polled_through_ns = high_water if high_water is not None else floor

    # -- internals -------------------------------------------------------------

    def _clock(self) -> datetime:
        if self._ctx is not None:
            return self._ctx.clock()
        return datetime.now(UTC)

    def _iter_observations(self) -> Iterator[Observation]:
        """One row per membership *edge* — the add, and the removal if any.

        Each edge carries its own announcement as ``knowledge_time``, so the
        store records the removal as knowable in 2024 even though the name
        joined the index in 2019.

        Both edges are emitted with ``source_up=True`` and no gap marker: the
        source is healthy, the name simply left the index. A coverage gap (a
        source that went down) is a different thing, and conflating them would
        fabricate delistings out of outages.
        """
        for interval in self._read_intervals():
            yield self._coverage_observation(
                interval,
                day=interval.added_on,
                announced_on=interval.announced_on,
                edge="added",
            )
            if interval.removed_on is not None and interval.removed_announced_on is not None:
                yield self._coverage_observation(
                    interval,
                    day=interval.removed_on,
                    announced_on=interval.removed_announced_on,
                    edge="removed",
                )

    def _coverage_observation(
        self, interval: MembershipInterval, *, day: date, announced_on: date, edge: str
    ) -> Observation:
        vintage = announced_on.isoformat()
        key = f"{self._source_id}|{self._universe_id}|{interval.security}|{day}|{edge}|{vintage}"
        event_time = day_start(day)
        return Observation(
            observation_id=deterministic_ulid(timestamp_ms=event_time.ns // 1_000_000, key=key),
            signal_type="meta.coverage",
            run_id=self._ctx.run_id if self._ctx is not None else "unconfigured",
            source_id=self._source_id,
            adapter_id=self.ADAPTER_ID,
            native_entity=interval.security,
            series_id=self._universe_id,
            event_time=event_time,
            # This edge's own announcement — never the other edge's.
            knowledge_time=_midnight(announced_on),
            # Restatement-capable, so every row is vintage-keyed (the guard
            # requires it); the announcement date is the natural vintage.
            vintage_id=vintage,
            data_class=DataClass.PUBLIC_AGG,
            license_tag=self._manifest.license_tag,
            payload=MetaCoverage(
                source_up=True,
                entities_covered=1,
                gap_marker=False,
                note=f"{self._universe_id}:{edge}",
            ),
        )

    def _read_intervals(self) -> Iterator[MembershipInterval]:
        if self._path is None:
            return
        yield from self._cache.get(self._path)

    def _parse_intervals(self, path: Path) -> list[MembershipInterval]:
        parsed: list[MembershipInterval] = []
        try:
            rows = list(read_csv(path, required=[ENTITY_COLUMNS, "added_on", "announced_on"]))
        except CsvFormatError as exc:
            if "added_on" in str(exc):
                raise CsvFormatError(f"{path.name}: {_FLAT_LIST_HINT}") from exc
            raise
        for row in rows:
            origin = f"{path.name}[{row.get('added_on', '?')}]"
            security = require_entity(row, origin=origin)
            added_on = require_date(row, "added_on", origin=origin)
            removed_on = optional_date(row, "removed_on", origin=origin)
            if removed_on is not None and removed_on <= added_on:
                raise CsvFormatError(
                    f"{origin}: {security} has removed_on {removed_on} on or before "
                    f"added_on {added_on} — an empty membership interval"
                )
            parsed.append(
                MembershipInterval(
                    security=security,
                    added_on=added_on,
                    announced_on=require_date(row, "announced_on", origin=origin),
                    removed_on=removed_on,
                    removed_announced_on=optional_date(row, "removed_announced_on", origin=origin),
                )
            )
        return parsed
