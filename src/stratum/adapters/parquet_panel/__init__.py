"""Unimplemented Parquet panel output-adapter scaffold.

The intended output is an entity/date/signal panel at a fixed ``as_of`` with
source vintages and export-policy metadata. No export behavior or policy
enforcement is implemented.

Other consumers, including Ledger connectors, can implement this same public
output-adapter interface without source-specific hooks in core.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from stratum.adapters.base import CapabilityManifest, OutputAdapter, TimeWindow
from stratum.adapters.context import AdapterContext
from stratum.schema.observation import Observation

__all__ = ["ParquetPanelExporter"]


class ParquetPanelExporter(OutputAdapter):
    ADAPTER_ID = "parquet_panel"

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        raise NotImplementedError("Parquet exporter configuration is not implemented")

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        return  # outputs ingest nothing
        yield  # pragma: no cover - marks this as an async generator

    async def poll(self) -> AsyncIterator[Observation]:
        return
        yield  # pragma: no cover

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=(), native_frequency="n/a", supports_restatement=False
        )

    async def export(self, request: dict[str, Any]) -> None:
        raise NotImplementedError("Parquet panel export is not implemented")
