"""Unimplemented Google Trends source-adapter scaffold.

Tricky PIT: Trends returns *relative, rescaled, and sometimes backfilled*
series — the same historical date can return different values across pulls.
``knowledge_time`` ~= query date, and every observation records the **request
vintage** (``vintage_id``) plus ``rescale_basis`` so the engine never mixes
incomparable pulls. The vintage-capture wrapper is mandatory, not optional:
without it, Trends-based factors are quietly non-reproducible and leak.

Depends on ``pytrends`` via the ``stratum[trends]`` extra.
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

__all__ = ["GoogleTrendsAdapter"]


class GoogleTrendsAdapter(SourceAdapter):
    ADAPTER_ID = "google_trends"

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        raise NotImplementedError("Google Trends configuration is not implemented")

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        # A "historical" Trends pull is still a pull made NOW: its values are
        # knowable now, not then. knowledge_time = request time, and each pull
        # is a fresh provider vintage — the guard requires vintage_id per row.
        raise NotImplementedError("Google Trends backfill is not implemented")
        yield  # pragma: no cover - marks this as an async generator

    async def poll(self) -> AsyncIterator[Observation]:
        raise NotImplementedError("Google Trends polling is not implemented")
        yield  # pragma: no cover

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=("attention.search_interest",),
            native_frequency="daily/weekly",
            supports_restatement=True,  # silent backfill by the provider -> new vintages
        )

    def entity_hint(self, raw_record: dict[str, Any]) -> list[EntityHint]:
        raise NotImplementedError("Google Trends entity hints are not implemented")

    def to_signal(self, raw_record: dict[str, Any]) -> list[SignalValue]:
        raise NotImplementedError("Google Trends signal conversion is not implemented")
