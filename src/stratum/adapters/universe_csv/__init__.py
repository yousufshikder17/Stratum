"""Unimplemented point-in-time universe CSV adapter scaffold.

Supplies historical index/listing membership so the backtest universe at date
D reflects what existed *then* — including since-delisted names, excluding
not-yet-listed ones. The survivorship-correct universe is non-negotiable
(spec §9 risk 3): a "current constituents" file applied to history is a hard
error in the engine, and this adapter's data format requires effective-dated
membership intervals, not a flat list.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from typing import Any

from stratum.adapters.base import (
    CapabilityManifest,
    TimeWindow,
    UniverseAdapter,
    UniverseSpec,
)
from stratum.adapters.context import AdapterContext
from stratum.schema.observation import Observation

__all__ = ["CsvUniverseAdapter"]


class CsvUniverseAdapter(UniverseAdapter):
    ADAPTER_ID = "universe_csv"

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        raise NotImplementedError("CSV universe configuration is not implemented")

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        raise NotImplementedError("CSV universe backfill is not implemented")
        yield  # pragma: no cover - marks this as an async generator

    async def poll(self) -> AsyncIterator[Observation]:
        raise NotImplementedError("CSV universe polling is not implemented")
        yield  # pragma: no cover

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=("meta.coverage",),
            native_frequency="sparse",
            supports_restatement=True,  # membership corrections append vintages
        )

    async def members_as_of(self, as_of: date, definition: UniverseSpec) -> list[str]:
        raise NotImplementedError("CSV universe point-in-time membership lookup is not implemented")
