"""CSV market-data adapter (spec §3.4, MVP item 5).

Market data is a *peer* adapter, not a core bolt-on: prices, volume,
corporate actions (splits/dividends), and delisting events flow through the
identical guard-and-store pipeline and live in the same bitemporal store as
alt-data (spec §2.2). Swappable to Stooq/Tiingo/Nasdaq Data Link behind the
same ABC — no vendor lock-in.

**Point-in-time contract.** The manifest declares
``knowledge_time_basis = publication``, so every row needs a real
source-provided stamp:

- **Bars** are knowable at their session close. The convention is explicit and
  configurable (``session_close_utc``, default 21:00 UTC ≈ a US equity close),
  never midnight on the bar date — which would let a strategy trade on a close
  it could not yet have seen — and never ingestion time. A file may override
  per row with a ``knowledge_time`` column.
- **Corporate actions and delistings** carry ``announced_on``: the ex-date is
  the *event*, the announcement is the *knowledge*. A file without
  ``announced_on`` is rejected rather than back-stamped with the ex-date,
  because the market knew about the split before it happened (spec §6.2).

Unadjusted values are what get stored; ``adjustment_ref`` names the as-of-date
adjustment factor set so the engine can apply only adjustments knowable by
``t`` (spec §6.2).

Ids are derived from ``(source, signal, entity, date)``, so re-reading the
same file is idempotent. If a row's *values* change under a stable key the
store raises rather than silently accepting it — this adapter declares
``supports_restatement = false``, so a changed bar is a contradiction worth
hearing about, not a new vintage.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

from stratum.adapters.base import (
    Bar,
    CapabilityManifest,
    CorporateAction,
    DelistingEvent,
    MarketDataAdapter,
    TimeWindow,
)
from stratum.adapters.context import AdapterContext
from stratum.adapters.csv_common import (
    ENTITY_COLUMNS,
    CsvCache,
    CsvFormatError,
    day_start,
    optional_date,
    optional_float,
    read_csv,
    require_date,
    require_entity,
    require_float,
    session_close_knowledge_time,
)
from stratum.adapters.manifest import load_manifest
from stratum.schema.data_class import DataClass
from stratum.schema.ids import deterministic_ulid
from stratum.schema.observation import Observation
from stratum.schema.payloads import (
    MarketBar,
    MarketCorporateAction,
    MarketDelisting,
    Payload,
)
from stratum.schema.times import EventTime, KnowledgeTime

__all__ = ["CsvMarketDataAdapter"]

_MANIFEST_PATH = Path(__file__).with_name("manifest.toml")
_DEFAULT_SESSION_CLOSE = time(21, 0)

_DATA_CLASSES = {
    "public_agg": DataClass.PUBLIC_AGG,
    "public_text": DataClass.PUBLIC_TEXT,
    "licensed": DataClass.LICENSED,
}


def _parse_session_close(text: str) -> time:
    try:
        return time.fromisoformat(text)
    except ValueError as exc:
        raise CsvFormatError(
            f"session_close_utc {text!r} is not an ISO time (e.g. '21:00')"
        ) from exc


def _in_window(event_time: EventTime, window: TimeWindow) -> bool:
    """Half-open ``[start, end)`` on the event axis."""
    return EventTime.at(window.start).ns <= event_time.ns < EventTime.at(window.end).ns


class CsvMarketDataAdapter(MarketDataAdapter):
    """Daily bars, corporate actions, and delistings from local CSV files."""

    ADAPTER_ID = "market_csv"

    def __init__(self) -> None:
        self._ctx: AdapterContext | None = None
        self._bars_path: Path | None = None
        self._actions_path: Path | None = None
        self._delistings_path: Path | None = None
        self._session_close = _DEFAULT_SESSION_CLOSE
        self._source_id = self.ADAPTER_ID
        self._adjustment_ref: str | None = None
        #: Conservative default: price data is usually vendor-licensed, so
        #: export policy should refuse redistribution unless told otherwise.
        self._data_class = DataClass.LICENSED
        self._manifest = load_manifest(_MANIFEST_PATH)
        #: Knowledge-axis watermark for incremental ``poll()``.
        self._polled_through_ns: int | None = None
        #: Parse each file once per change. `backfill()` and `capabilities()`
        #: both walk the bars; re-reading them every pass was pure waste.
        self._bars_cache: CsvCache[list[Bar]] = CsvCache(self._parse_bars)
        self._actions_cache: CsvCache[list[CorporateAction]] = CsvCache(self._parse_actions)
        self._delistings_cache: CsvCache[list[DelistingEvent]] = CsvCache(self._parse_delistings)

    # -- lifecycle -------------------------------------------------------------

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        bars = config.get("bars_path")
        if not bars:
            raise CsvFormatError("market_csv requires 'bars_path'")
        self._ctx = ctx
        self._bars_path = Path(bars)
        if not self._bars_path.exists():
            raise CsvFormatError(f"bars_path does not exist: {self._bars_path}")
        actions = config.get("corporate_actions_path")
        self._actions_path = Path(actions) if actions else None
        delistings = config.get("delistings_path")
        self._delistings_path = Path(delistings) if delistings else None
        if "session_close_utc" in config:
            self._session_close = _parse_session_close(str(config["session_close_utc"]))
        self._source_id = str(config.get("source_id", self.ADAPTER_ID))
        adjustment_ref = config.get("adjustment_ref")
        self._adjustment_ref = str(adjustment_ref) if adjustment_ref else None
        if "data_class" in config:
            key = str(config["data_class"]).lower()
            if key not in _DATA_CLASSES:
                raise CsvFormatError(f"data_class {key!r} must be one of {sorted(_DATA_CLASSES)}")
            self._data_class = _DATA_CLASSES[key]

    def capabilities(self) -> CapabilityManifest:
        days = []
        entities = set()
        for bar in self._read_bars():
            entities.add(bar.native_entity)
            days.append(bar.event_time.as_datetime().date())
        return CapabilityManifest(
            signal_types=("market.bar", "market.corporate_action", "market.delisting"),
            native_frequency="daily",
            supports_restatement=False,
            coverage_start=min(days, default=None),
            entities=sorted(entities),
        )

    # -- typed record readers (spec §3.4) --------------------------------------

    async def bars(self, ids: list[str], window: TimeWindow) -> AsyncIterator[Bar]:
        wanted = set(ids)
        for bar in self._read_bars():
            if wanted and bar.native_entity not in wanted:
                continue
            if _in_window(bar.event_time, window):
                yield bar

    async def corporate_actions(
        self, ids: list[str], window: TimeWindow
    ) -> AsyncIterator[CorporateAction]:
        wanted = set(ids)
        for action in self._read_actions():
            if wanted and action.native_entity not in wanted:
                continue
            if _in_window(action.event_time, window):
                yield action

    async def delistings(self, window: TimeWindow) -> AsyncIterator[DelistingEvent]:
        for event in self._read_delistings():
            if _in_window(event.event_time, window):
                yield event

    # -- ingestion -------------------------------------------------------------

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        """Every record whose *event* falls in ``[start, end)``.

        Honest under ``basis = publication``: each row's knowledge stamp comes
        from the data (or the declared session-close convention), so nothing
        here claims we knew a 2019 close in 2026.
        """
        for observation in self._iter_observations():
            if _in_window(observation.event_time, window):
                yield observation

    async def poll(self) -> AsyncIterator[Observation]:
        """Everything that has become *knowable* since the last poll.

        The watermark rides the knowledge axis, not the event axis, and tracks
        the highest stamp actually *emitted* rather than the wall clock.
        Advancing it to "now" looks equivalent and quietly breaks the normal
        case: appending yesterday's bar to the file today would leave it
        invisible forever, because its publication stamp is older than the
        clock reading taken during the previous poll.

        Rows stamped in the future are held back for a later poll rather than
        emitted early (the guard would reject them anyway).
        """
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
        for bar in self._read_bars():
            yield self._observation(
                signal_type="market.bar",
                native_entity=bar.native_entity,
                event_time=bar.event_time,
                knowledge_time=bar.knowledge_time,
                payload=MarketBar(
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    vwap=bar.vwap,
                    adjustment_ref=bar.adjustment_ref,
                ),
            )
        for action in self._read_actions():
            yield self._observation(
                signal_type="market.corporate_action",
                native_entity=action.native_entity,
                event_time=action.event_time,
                knowledge_time=action.knowledge_time,
                payload=MarketCorporateAction(
                    action_type=action.action_type,
                    ratio=action.ratio,
                    amount=action.amount,
                    ex_date=action.ex_date,
                    record_date=action.record_date,
                    pay_date=action.pay_date,
                ),
                # Two actions can share an ex-date (a split and a dividend);
                # the type keeps their ids distinct.
                key_suffix=action.action_type,
            )
        for event in self._read_delistings():
            yield self._observation(
                signal_type="market.delisting",
                native_entity=event.native_entity,
                event_time=event.event_time,
                knowledge_time=event.knowledge_time,
                payload=MarketDelisting(
                    reason=event.reason,
                    last_trade_date=event.last_trade_date,
                    final_value_policy=event.final_value_policy,
                ),
            )

    def _observation(
        self,
        *,
        signal_type: str,
        native_entity: str,
        event_time: EventTime,
        knowledge_time: KnowledgeTime,
        payload: Payload,
        key_suffix: str = "",
    ) -> Observation:
        day = event_time.as_datetime().date().isoformat()
        key = f"{self._source_id}|{signal_type}|{native_entity}|{day}|{key_suffix}"
        return Observation(
            observation_id=deterministic_ulid(timestamp_ms=event_time.ns // 1_000_000, key=key),
            signal_type=signal_type,
            run_id=self._ctx.run_id if self._ctx is not None else "unconfigured",
            source_id=self._source_id,
            adapter_id=self.ADAPTER_ID,
            native_entity=native_entity,
            event_time=event_time,
            knowledge_time=knowledge_time,
            data_class=self._data_class,
            license_tag=self._manifest.license_tag,
            payload=payload,
        )

    def _read_bars(self) -> Iterator[Bar]:
        if self._bars_path is None:
            return
        yield from self._bars_cache.get(self._bars_path)

    def _parse_bars(self, path: Path) -> list[Bar]:
        required: list[str | tuple[str, ...]] = [
            ENTITY_COLUMNS,
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        parsed: list[Bar] = []
        for row in read_csv(path, required=required):
            origin = f"{path.name}[{row.get('date', '?')}]"
            day = require_date(row, "date", origin=origin)
            parsed.append(
                Bar(
                    native_entity=require_entity(row, origin=origin),
                    event_time=day_start(day),
                    knowledge_time=self._bar_knowledge_time(row, day=day, origin=origin),
                    open=require_float(row, "open", origin=origin),
                    high=require_float(row, "high", origin=origin),
                    low=require_float(row, "low", origin=origin),
                    close=require_float(row, "close", origin=origin),
                    volume=require_float(row, "volume", origin=origin),
                    vwap=optional_float(row, "vwap", origin=origin),
                    adjustment_ref=row.get("adjustment_ref") or self._adjustment_ref,
                )
            )
        return parsed

    def _bar_knowledge_time(
        self, row: Mapping[str, str], *, day: date, origin: str
    ) -> KnowledgeTime:
        """Per-row override if the file supplies one, else the convention."""
        explicit = row.get("knowledge_time", "")
        if not explicit:
            return session_close_knowledge_time(day, self._session_close)
        try:
            stamp = datetime.fromisoformat(explicit)
        except ValueError as exc:
            raise CsvFormatError(
                f"{origin}: knowledge_time {explicit!r} is not an ISO datetime"
            ) from exc
        if stamp.tzinfo is None:
            raise CsvFormatError(
                f"{origin}: knowledge_time {explicit!r} needs a timezone — "
                "a naive publication stamp is ambiguous"
            )
        return KnowledgeTime.at(stamp)

    def _read_actions(self) -> Iterator[CorporateAction]:
        if self._actions_path is None:
            return
        yield from self._actions_cache.get(self._actions_path)

    def _parse_actions(self, path: Path) -> list[CorporateAction]:
        required: list[str | tuple[str, ...]] = [
            ENTITY_COLUMNS,
            "action_type",
            "ex_date",
            "announced_on",
        ]
        parsed: list[CorporateAction] = []
        for row in read_csv(path, required=required):
            origin = f"{path.name}[{row.get('ex_date', '?')}]"
            ex_date = require_date(row, "ex_date", origin=origin)
            parsed.append(
                CorporateAction(
                    native_entity=require_entity(row, origin=origin),
                    action_type=row["action_type"],
                    event_time=day_start(ex_date),
                    knowledge_time=session_close_knowledge_time(
                        require_date(row, "announced_on", origin=origin),
                        self._session_close,
                    ),
                    ratio=optional_float(row, "ratio", origin=origin),
                    amount=optional_float(row, "amount", origin=origin),
                    ex_date=ex_date,
                    record_date=optional_date(row, "record_date", origin=origin),
                    pay_date=optional_date(row, "pay_date", origin=origin),
                )
            )
        return parsed

    def _read_delistings(self) -> Iterator[DelistingEvent]:
        if self._delistings_path is None:
            return
        yield from self._delistings_cache.get(self._delistings_path)

    def _parse_delistings(self, path: Path) -> list[DelistingEvent]:
        required: list[str | tuple[str, ...]] = [
            ENTITY_COLUMNS,
            "reason",
            "last_trade_date",
            "announced_on",
        ]
        parsed: list[DelistingEvent] = []
        for row in read_csv(path, required=required):
            origin = f"{path.name}[{row.get('last_trade_date', '?')}]"
            last_trade = require_date(row, "last_trade_date", origin=origin)
            parsed.append(
                DelistingEvent(
                    native_entity=require_entity(row, origin=origin),
                    event_time=day_start(last_trade),
                    knowledge_time=session_close_knowledge_time(
                        require_date(row, "announced_on", origin=origin),
                        self._session_close,
                    ),
                    reason=row["reason"],
                    last_trade_date=last_trade,
                    final_value_policy=row.get("final_value_policy", ""),
                )
            )
        return parsed
