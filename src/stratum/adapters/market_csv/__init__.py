"""Unimplemented CSV market-data adapter scaffold.

Market data is a *peer* adapter, not a core bolt-on: prices, volume,
corporate actions (splits/dividends), and delisting events flow through the
identical pipeline and live in the same bitemporal store as alt-data
(spec §2.2). Adjusted and unadjusted are both retained; the engine applies
only as-of-date adjustment factors (spec §6.2). Swappable to
Stooq/Tiingo/Nasdaq Data Link behind the same ABC — no vendor lock-in.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
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
from stratum.schema.observation import Observation

__all__ = ["CsvMarketDataAdapter"]


class CsvMarketDataAdapter(MarketDataAdapter):
    ADAPTER_ID = "market_csv"

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        raise NotImplementedError("CSV market-data configuration is not implemented")

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        # Bars carry a real publication stamp (close/settlement date), so
        # historical backfill is honest under basis = publication.
        raise NotImplementedError("CSV market-data backfill is not implemented")
        yield  # pragma: no cover - marks this as an async generator

    async def poll(self) -> AsyncIterator[Observation]:
        raise NotImplementedError("CSV market-data polling is not implemented")
        yield  # pragma: no cover

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=("market.bar", "market.corporate_action", "market.delisting"),
            native_frequency="daily",
            supports_restatement=False,
        )

    async def bars(self, ids: list[str], window: TimeWindow) -> AsyncIterator[Bar]:
        raise NotImplementedError("CSV market bars are not implemented")
        yield  # pragma: no cover

    async def corporate_actions(
        self, ids: list[str], window: TimeWindow
    ) -> AsyncIterator[CorporateAction]:
        raise NotImplementedError("CSV corporate actions are not implemented")
        yield  # pragma: no cover

    async def delistings(self, window: TimeWindow) -> AsyncIterator[DelistingEvent]:
        raise NotImplementedError("CSV delistings are not implemented")
        yield  # pragma: no cover
