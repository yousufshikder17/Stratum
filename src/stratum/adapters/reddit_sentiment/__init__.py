"""Unimplemented Reddit sentiment source-adapter scaffold.

PIT mapping: ``knowledge_time = post/comment created_utc`` — publicly visible
at creation, hence honest. Emits ``social.sentiment`` (score, magnitude,
model id) and ``social.attention`` (mention_count, unique_authors, velocity).
The sentiment model id + version is pinned per observation so re-scoring
history is a new vintage, not a silent change (spec §4.4 rule 3).

Depends on ``asyncpraw`` via the ``stratum[reddit]`` extra; the import stays
inside this package — core never sees it (spec §1).
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

__all__ = ["RedditSentimentAdapter"]


class RedditSentimentAdapter(SourceAdapter):
    ADAPTER_ID = "reddit_sentiment"

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        raise NotImplementedError("Reddit adapter configuration is not implemented")

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        # knowledge_time_basis = ingestion: honest historical backfill needs a
        # ToS-permitted archive with original created_utc stamps; without one
        # this adapter must refuse silent backfill (spec §3.6), never stamp now.
        raise NotImplementedError("Reddit backfill is not implemented")
        yield  # pragma: no cover - marks this as an async generator

    async def poll(self) -> AsyncIterator[Observation]:
        raise NotImplementedError("Reddit polling is not implemented")
        yield  # pragma: no cover

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=("social.sentiment", "social.attention"),
            native_frequency="intraday->daily",
            supports_restatement=False,
            entities=None,  # open-ended: any cashtag/name mentioned
        )

    def entity_hint(self, raw_record: dict[str, Any]) -> list[EntityHint]:
        raise NotImplementedError("Reddit entity hints are not implemented")

    def to_signal(self, raw_record: dict[str, Any]) -> list[SignalValue]:
        raise NotImplementedError("Reddit signal conversion is not implemented")
