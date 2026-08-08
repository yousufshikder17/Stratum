"""Unimplemented SEC EDGAR source-adapter scaffold.

PIT mapping: ``knowledge_time = acceptance_datetime`` (EDGAR's official
"available to public" stamp), ``event_time = period_of_report``. Restatements
(10-K/A etc.) arrive as **new vintages** keyed to the same period. XBRL facts
map to ``fundamental.*`` signals; filing events to ``event.filing``.

Transport: ``httpx`` + EDGAR ``submissions`` JSON / full-text / financial
statement datasets — no heavy SDK (spec §9).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from stratum.adapters.base import (
    CapabilityManifest,
    EntityHint,
    SignalValue,
    SourceAdapter,
    TimeWindow,
)
from stratum.adapters.context import AdapterContext
from stratum.schema.observation import Observation

__all__ = ["SecEdgarAdapter"]


class SecEdgarAdapter(SourceAdapter):
    ADAPTER_ID = "sec_edgar"

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        raise NotImplementedError("SEC EDGAR configuration is not implemented")

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        # Honest historical backfill IS possible here: acceptance_datetime is
        # a real historical knowledge stamp (basis = publication).
        raise NotImplementedError("SEC EDGAR backfill is not implemented")
        yield  # pragma: no cover - marks this as an async generator

    async def poll(self) -> AsyncIterator[Observation]:
        raise NotImplementedError("SEC EDGAR polling is not implemented")
        yield  # pragma: no cover

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=("fundamental.fact", "event.filing"),
            native_frequency="per-filing",
            supports_restatement=True,  # amendments -> new vintages, never overwrite
        )

    def entity_hint(self, raw_record: dict[str, Any]) -> list[EntityHint]:
        raise NotImplementedError("SEC EDGAR entity hints are not implemented")

    def to_signal(self, raw_record: dict[str, Any]) -> list[SignalValue]:
        raise NotImplementedError("SEC EDGAR signal conversion is not implemented")
