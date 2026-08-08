"""Default cost model (spec §6.4): commission + half-spread + sqrt-impact
slippage, conservative by design. Pluggable like every other adapter family.
"""

from __future__ import annotations

import math
from collections.abc import AsyncIterator
from typing import Any

from stratum.adapters.base import (
    CapabilityManifest,
    CostBreakdown,
    CostModelAdapter,
    MarketSnapshot,
    ProposedFill,
    TimeWindow,
)
from stratum.adapters.context import AdapterContext
from stratum.schema.observation import Observation

__all__ = ["DefaultCostModelAdapter"]


class DefaultCostModelAdapter(CostModelAdapter):
    """k·sigma·sqrt(order/ADV) slippage (square-root market-impact style),
    plus explicit commission and half-spread. Defaults are conservative;
    every parameter is visible in the run manifest."""

    ADAPTER_ID = "cost_default"

    def __init__(
        self,
        *,
        commission_bps: float = 1.0,
        impact_coefficient: float = 1.0,
    ) -> None:
        self._commission_bps = commission_bps
        self._k = impact_coefficient

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        self._commission_bps = float(config.get("commission_bps", self._commission_bps))
        self._k = float(config.get("impact_coefficient", self._k))

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        return  # cost models ingest nothing
        yield  # pragma: no cover - marks this as an async generator

    async def poll(self) -> AsyncIterator[Observation]:
        return
        yield  # pragma: no cover

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=(), native_frequency="n/a", supports_restatement=False
        )

    def cost_bps(self, fill: ProposedFill, mkt: MarketSnapshot) -> CostBreakdown:
        participation = 0.0
        if mkt.adv > 0:
            participation = fill.quantity / mkt.adv
        slippage_bps = self._k * mkt.volatility * math.sqrt(max(participation, 0.0)) * 1e4
        return CostBreakdown(
            commission_bps=self._commission_bps,
            spread_bps=mkt.half_spread_bps,
            slippage_bps=slippage_bps,
        )
