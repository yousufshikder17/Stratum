"""Adapter lifecycle contract and family ABCs.

Adapters are the extension seam of the whole platform. Five families share one
lifecycle contract: source adapters (alt-data), market-data adapters,
universe adapters, cost-model adapters, and output adapters.

Type-level PIT enforcement: every record type an adapter can emit —
:class:`~stratum.schema.observation.Observation`, :class:`SignalValue`,
:class:`Bar`, :class:`CorporateAction`, :class:`DelistingEvent` — has a
required, default-less ``knowledge_time: KnowledgeTime`` field. There is no
constructor path that produces adapter output without an explicit knowledge
stamp, and an ``EventTime`` cannot be passed in its place.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from stratum.adapters.context import AdapterContext
from stratum.schema.observation import Observation
from stratum.schema.payloads import Payload
from stratum.schema.times import EventTime, KnowledgeTime

__all__ = [
    "Adapter",
    "Bar",
    "CapabilityManifest",
    "CorporateAction",
    "CostBreakdown",
    "CostModelAdapter",
    "DelistingEvent",
    "EntityHint",
    "HealthState",
    "HealthStatus",
    "MarketDataAdapter",
    "MarketSnapshot",
    "OutputAdapter",
    "ProposedFill",
    "SignalValue",
    "SourceAdapter",
    "TimeWindow",
    "UniverseAdapter",
    "UniverseSpec",
]


# ---------------------------------------------------------------------------
# Shared value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True, slots=True)
class TimeWindow:
    """Half-open ``[start, end)`` window, tz-aware UTC."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("TimeWindow bounds must be timezone-aware")
        if not self.start < self.end:
            raise ValueError("TimeWindow requires start < end")


@dataclass(frozen=True, kw_only=True, slots=True)
class EntityHint:
    """A candidate entity reference for the resolver to canonicalize (§3.3)."""

    kind: str  # "cashtag" | "ticker" | "cik" | "company_name" | "search_term" | ...
    value: str
    context: str | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class SignalValue:
    """One typed signal value mapped from a native record (§3.3).

    ``knowledge_time`` is required with no default — a signal value cannot
    exist without an honest knowledge stamp.
    """

    signal_type: str
    native_entity: str
    event_time: EventTime
    knowledge_time: KnowledgeTime
    payload: Payload
    vintage_id: str = ""


@dataclass(frozen=True, kw_only=True, slots=True)
class CapabilityManifest:
    """What this source can report (spec §2.2, §3.2): emitted alongside data
    so coverage is explicit, never assumed."""

    signal_types: Sequence[str]
    native_frequency: str
    supports_restatement: bool
    coverage_start: date | None = None
    #: None = open-ended (e.g. any ticker mentioned); else the covered set.
    entities: Sequence[str] | None = None


class HealthState(StrEnum):
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


@dataclass(frozen=True, kw_only=True, slots=True)
class HealthStatus:
    state: HealthState
    detail: str = ""
    checked_at: datetime | None = None


# ---------------------------------------------------------------------------
# Market-data record types (§3.4) — knowledge_time required on every one
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True, slots=True)
class Bar:
    native_entity: str
    event_time: EventTime  # bar date
    knowledge_time: KnowledgeTime  # when the bar was publishable/knowable
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    adjustment_ref: str | None = None  # as-of-date adjustment factors (§6.2)


@dataclass(frozen=True, kw_only=True, slots=True)
class CorporateAction:
    native_entity: str
    action_type: str  # "split" | "dividend" | "spinoff"
    event_time: EventTime  # ex-date axis
    knowledge_time: KnowledgeTime  # announcement/knowable stamp (§6.2)
    ratio: float | None = None
    amount: float | None = None
    ex_date: date | None = None
    record_date: date | None = None
    pay_date: date | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class DelistingEvent:
    native_entity: str
    event_time: EventTime
    knowledge_time: KnowledgeTime
    reason: str
    last_trade_date: date | None = None
    final_value_policy: str = ""


# ---------------------------------------------------------------------------
# Universe & cost-model value types (§3.4, §6.4)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True, slots=True)
class UniverseSpec:
    """Names a historical-membership definition (e.g. ``sp1500_pit``)."""

    id: str
    description: str = ""


@dataclass(frozen=True, kw_only=True, slots=True)
class ProposedFill:
    security_id: str
    side: str  # "buy" | "sell"
    quantity: float
    notional: float


@dataclass(frozen=True, kw_only=True, slots=True)
class MarketSnapshot:
    """The market context a cost model prices a fill against."""

    security_id: str
    price: float
    adv: float  # average daily volume (shares)
    volatility: float  # daily sigma, decimal
    half_spread_bps: float


@dataclass(frozen=True, kw_only=True, slots=True)
class CostBreakdown:
    """Commission + spread + slippage + (optional) impact, in bps (§3.4, §6.4)."""

    commission_bps: float
    spread_bps: float
    slippage_bps: float
    impact_bps: float = 0.0
    borrow_bps: float = 0.0

    @property
    def total_bps(self) -> float:
        return (
            self.commission_bps
            + self.spread_bps
            + self.slippage_bps
            + self.impact_bps
            + self.borrow_bps
        )


# ---------------------------------------------------------------------------
# Lifecycle contract (§3.2)
# ---------------------------------------------------------------------------


class Adapter(ABC):
    """Base contract shared by all adapter families. Ingestion methods are
    async; the core owns scheduling (spec §3.2)."""

    @abstractmethod
    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        """Validate config, allocate nothing yet. ``ctx`` provides logger,
        clock, data dir, secrets handle, rate-limiter, and the (guarded)
        write handle into the PIT store."""

    @abstractmethod
    def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        """Historical pull for ``[start, end)``.

        MUST yield observations whose ``knowledge_time`` reflects when each
        value was actually knowable, NOT "now". Adapters that cannot
        reconstruct historical knowledge_time must say so in the manifest and
        refuse silent backfill (spec §3.6) rather than fabricate stamps.
        """

    @abstractmethod
    def poll(self) -> AsyncIterator[Observation]:
        """Incremental forward pull. ``knowledge_time = ingestion time`` is
        honest here because we are learning values as they appear."""

    @abstractmethod
    def capabilities(self) -> CapabilityManifest:
        """Entities covered, signal fields emitted, native frequency,
        restatement behavior, coverage start date."""

    async def health(self) -> HealthStatus:
        return HealthStatus(state=HealthState.OK)

    async def close(self) -> None:
        return None


class SourceAdapter(Adapter):
    """Alt-data source adapters (§3.3)."""

    @abstractmethod
    def entity_hint(self, raw_record: dict[str, Any]) -> list[EntityHint]:
        """Extract candidate entity references (cashtags, company names, CIKs,
        search terms) for the resolver to canonicalize."""

    @abstractmethod
    def to_signal(self, raw_record: dict[str, Any]) -> list[SignalValue]:
        """Map a native record to one or more typed signal values (e.g.
        reddit -> sentiment_score, mention_count, author_diversity)."""


class MarketDataAdapter(Adapter):
    """Prices/corporate actions/delistings — a *peer* of the alt-data
    adapters, flowing through the identical pipeline (spec §2.2, §3.4)."""

    @abstractmethod
    def bars(self, ids: list[str], window: TimeWindow) -> AsyncIterator[Bar]: ...

    @abstractmethod
    def corporate_actions(
        self, ids: list[str], window: TimeWindow
    ) -> AsyncIterator[CorporateAction]: ...

    @abstractmethod
    def delistings(self, window: TimeWindow) -> AsyncIterator[DelistingEvent]: ...


class UniverseAdapter(Adapter):
    """Historical membership — the survivorship-correct universe (§3.4, §6.3)."""

    @abstractmethod
    async def members_as_of(self, as_of: date, definition: UniverseSpec) -> list[str]:
        """Constituents KNOWN as of ``as_of``, including names later delisted
        and excluding names not yet listed/added."""


class CostModelAdapter(Adapter):
    """Pluggable transaction-cost model (§3.4, §6.4)."""

    @abstractmethod
    def cost_bps(self, fill: ProposedFill, mkt: MarketSnapshot) -> CostBreakdown:
        """Commission + spread + slippage + (optional) impact, in bps."""


class OutputAdapter(Adapter):
    """Exporters/report generators (§3.5). They read from the PIT store via
    the same public SDK. Consumers such as Ledger connectors can implement
    this interface without source-specific hooks in core."""

    @abstractmethod
    async def export(self, request: dict[str, Any]) -> None:
        """Materialize an output (e.g. a Parquet panel at a fixed ``as_of``),
        always stamping the ``as_of`` and the source vintages used (§4.8), and
        honoring ``DataClass``/``license_tag`` export policy (§4.7)."""
