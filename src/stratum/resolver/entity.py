"""Point-in-time entity resolution (spec §4.5).

Maps native keys (ticker, cashtag, CIK, search term) to a canonical internal
``security_id``. The mapping is itself bitemporal — "what did ``$X`` refer to
*as of* that date?" — because tickers get reused, symbols change, and
companies merge. Ambiguity is preserved with a confidence score and candidate
set rather than forced; low-confidence social mentions can be filtered
downstream. Resolver tables are versioned data, shipped and updatable, never
hardcoded.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from stratum.adapters.base import EntityHint
from stratum.schema.times import AsOf

__all__ = ["Candidate", "EntityResolver", "Resolution"]


@dataclass(frozen=True, kw_only=True, slots=True)
class Candidate:
    security_id: str
    confidence: float  # 0..1


@dataclass(frozen=True, kw_only=True, slots=True)
class Resolution:
    hint: EntityHint
    candidates: Sequence[Candidate]

    @property
    def best(self) -> Candidate | None:
        return max(self.candidates, key=lambda c: c.confidence, default=None)


class EntityResolver:
    """Resolver interface. ``as_of`` is required because
    identity itself changes over time."""

    def __init__(self, tables_version: str = "v0") -> None:
        self.tables_version = tables_version

    def resolve(self, hint: EntityHint, *, as_of: AsOf) -> Resolution:
        raise NotImplementedError(
            "not implemented: bitemporal lookup over versioned mapping tables "
            "(ticker <-> CIK <-> security_id with history)"
        )
