"""Typed payloads for the standard signal types (spec §4.3, §4.6).

Every observation shares one envelope (:class:`~stratum.schema.observation.Observation`);
payloads are typed per signal kind. Unknown payload types are preserved via
:class:`GenericPayload`, never dropped (forward compatibility, spec §4.1).

Every derived numeric signal records ``sample_size``/coverage so the factor
engine and backtester can downweight or exclude thin observations rather than
treat a 2-mention day like a 2000-mention day (spec §4.6).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import ClassVar

__all__ = [
    "STANDARD_PAYLOAD_TYPES",
    "FilingEvent",
    "FundamentalFact",
    "GenericPayload",
    "GeoActivityIndex",
    "HiringAttention",
    "MarketBar",
    "MarketCorporateAction",
    "MarketDelisting",
    "MetaCoverage",
    "ModelRef",
    "Payload",
    "SearchInterest",
    "SocialAttention",
    "SocialSentiment",
    "WeatherAnomaly",
]


@dataclass(frozen=True, kw_only=True, slots=True)
class ModelRef:
    """Model id + version for derived values (spec §4.4 rule 3).

    Re-scoring history with a new model produces a new vintage with a new
    ``knowledge_time`` — you didn't know the new score earlier.
    """

    model_id: str
    version: str


@dataclass(frozen=True, kw_only=True, slots=True)
class Payload:
    """Base class for typed payloads. Subclasses set ``SIGNAL_TYPE``."""

    SIGNAL_TYPE: ClassVar[str] = ""

    def signal_type(self) -> str:
        return type(self).SIGNAL_TYPE


@dataclass(frozen=True, kw_only=True, slots=True)
class GenericPayload(Payload):
    """Carrier for unknown/extension signal types — preserved, not dropped."""

    type_name: str
    fields: Mapping[str, object] = field(default_factory=dict)

    def signal_type(self) -> str:
        return self.type_name


@dataclass(frozen=True, kw_only=True, slots=True)
class SocialSentiment(Payload):
    SIGNAL_TYPE: ClassVar[str] = "social.sentiment"

    score: float  # -1..1
    magnitude: float
    model: ModelRef
    sample_size: int


@dataclass(frozen=True, kw_only=True, slots=True)
class SocialAttention(Payload):
    SIGNAL_TYPE: ClassVar[str] = "social.attention"

    mention_count: int
    unique_authors: int
    velocity: float
    share_of_voice: float | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class SearchInterest(Payload):
    """Google Trends-style interest. Values are relative and rescaled; the
    ``rescale_basis`` and per-pull vintage keep pulls comparable (spec §3.3)."""

    SIGNAL_TYPE: ClassVar[str] = "attention.search_interest"

    interest: float
    rescale_basis: str
    geo: str = ""


@dataclass(frozen=True, kw_only=True, slots=True)
class HiringAttention(Payload):
    SIGNAL_TYPE: ClassVar[str] = "attention.hiring"

    open_reqs: int
    mom_change: float | None = None
    role_class: str = ""
    geo: str = ""


@dataclass(frozen=True, kw_only=True, slots=True)
class FundamentalFact(Payload):
    SIGNAL_TYPE: ClassVar[str] = "fundamental.fact"

    concept: str  # XBRL concept
    value: float
    unit: str
    period_end: date
    period_type: str  # "instant" | "duration"


@dataclass(frozen=True, kw_only=True, slots=True)
class FilingEvent(Payload):
    SIGNAL_TYPE: ClassVar[str] = "event.filing"

    form_type: str  # "10-K", "10-K/A", ...
    accession: str
    items: tuple[str, ...] = ()
    amends_ref: str | None = None  # accession of the filing this amends


@dataclass(frozen=True, kw_only=True, slots=True)
class WeatherAnomaly(Payload):
    SIGNAL_TYPE: ClassVar[str] = "environment.weather_anomaly"

    metric: str
    deviation_vs_climatology: float
    georef: str
    forecast_horizon: timedelta | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class GeoActivityIndex(Payload):
    SIGNAL_TYPE: ClassVar[str] = "geo.activity_index"

    value: float
    provider: str
    processing_vintage: str
    georef: str


@dataclass(frozen=True, kw_only=True, slots=True)
class MarketBar(Payload):
    SIGNAL_TYPE: ClassVar[str] = "market.bar"

    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    #: Reference to the as-of-date adjustment factor set — the engine applies
    #: only adjustments knowable by ``t`` (spec §6.2). Unadjusted retained.
    adjustment_ref: str | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class MarketCorporateAction(Payload):
    SIGNAL_TYPE: ClassVar[str] = "market.corporate_action"

    action_type: str  # "split" | "dividend" | "spinoff"
    ratio: float | None = None
    amount: float | None = None
    ex_date: date | None = None
    record_date: date | None = None
    pay_date: date | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class MarketDelisting(Payload):
    """Delisting event — the engine applies a delisting return instead of
    silently dropping the position (spec §6.3, the classic survivorship error)."""

    SIGNAL_TYPE: ClassVar[str] = "market.delisting"

    reason: str  # "bankruptcy" | "acquisition" | "voluntary" | ...
    last_trade_date: date | None = None
    final_value_policy: str = ""  # e.g. "zero", "cash_terms", "modeled_recovery"


@dataclass(frozen=True, kw_only=True, slots=True)
class MetaCoverage(Payload):
    """Source up/down + coverage gap markers, modeled as missing data with an
    explicit fill policy, never as zeros (spec §6.3 rule 4)."""

    SIGNAL_TYPE: ClassVar[str] = "meta.coverage"

    source_up: bool
    entities_covered: int = 0
    gap_marker: bool = False
    note: str = ""


#: Signal type name -> payload class, for the v1 standard vocabulary (§4.6).
STANDARD_PAYLOAD_TYPES: Mapping[str, type[Payload]] = {
    cls.SIGNAL_TYPE: cls
    for cls in (
        SocialSentiment,
        SocialAttention,
        SearchInterest,
        HiringAttention,
        FundamentalFact,
        FilingEvent,
        WeatherAnomaly,
        GeoActivityIndex,
        MarketBar,
        MarketCorporateAction,
        MarketDelisting,
        MetaCoverage,
    )
}
