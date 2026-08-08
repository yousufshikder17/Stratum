"""Factor-engine interface; computation is not implemented.

The interface is intended to query at ``as_of = t - embargo``. No factor
calculation, transform execution, or build cache exists yet.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

from stratum.factors.definition import FactorDefinition
from stratum.store.interface import SignalStore, SnapshotRef

__all__ = ["ExposurePanel", "FactorEngine"]


@dataclass(frozen=True, kw_only=True)
class ExposurePanel:
    """Cross-sectional exposures: rebalance date -> (security_id -> value),
    plus the coverage diagnostics that keep thin alt-data honest (spec §5.4)."""

    factor_id: str
    factor_version: str
    exposures: Mapping[date, Mapping[str, float]]
    coverage: Mapping[date, int]
    #: Content address of (definition hash, snapshot hash) for caching/repro.
    build_hash: str


class FactorEngine:
    def __init__(self, *, store: SignalStore, snapshot: SnapshotRef) -> None:
        self._store = store
        self._snapshot = snapshot

    def build(
        self, definition: FactorDefinition, *, rebalance_dates: Sequence[date]
    ) -> ExposurePanel:
        """Planned: read inputs at ``as_of = t - embargo``,
        apply the PIT-safe transform pipeline within the cross-section,
        exclude/downweight entities under ``min_coverage``."""
        raise NotImplementedError("point-in-time factor build is not implemented")
