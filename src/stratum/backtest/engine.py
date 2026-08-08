"""Backtest configuration guards and an unimplemented engine interface.

Configuration objects reject selected unsafe settings. The computational
backtest loop is not implemented.

Structural guards in this module:

- ``BacktestSpec`` requires a snapshot, a universe adapter (PIT membership),
  and a cost model; costs and delisting handling default **on**.
- Disabling costs requires the explicit ``label_gross_illustrative=True``
  acknowledgment, and every output is then labeled ``gross/illustrative``
  (spec §6.4).
- There is no parameter that accepts a "current constituents" list: the
  universe is only reachable through ``UniverseAdapter.members_as_of(t)``.
  Feeding a static membership snapshot to history is a hard error
  (:class:`SurvivorshipBiasError`), not a warning (spec §6.3 rule 1).
- A backtest is a pure function of (snapshot, factor def, universe def, cost
  model, seed): same hashes, bit-identical output on any machine (spec §6.6).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from stratum.adapters.base import CostModelAdapter, UniverseAdapter, UniverseSpec
from stratum.backtest.report import DiagnosticsReport
from stratum.factors.definition import FactorDefinition
from stratum.store.interface import SignalStore, SnapshotRef

__all__ = [
    "BacktestEngine",
    "BacktestSpec",
    "FillPolicy",
    "GuardConfigError",
    "SurvivorshipBiasError",
]


class GuardConfigError(ValueError):
    """A bias guard was disabled without the explicit, loud acknowledgment."""


class SurvivorshipBiasError(ValueError):
    """A current-constituents list was applied to history — hard error (§6.3)."""


@dataclass(frozen=True, kw_only=True, slots=True)
class FillPolicy:
    """Next-bar fills (spec §6.1 rule 1): a signal known at close t fills at
    open/VWAP of t+1. There is no same-bar fill mode."""

    at: str = "next_open"  # "next_open" | "next_vwap"
    #: Volume cap: can't trade more than this fraction of bar volume (§6.4),
    #: so illiquid alt-data names don't get free fills.
    max_participation: float = 0.1


@dataclass(frozen=True, kw_only=True, slots=True)
class BacktestSpec:
    factor: FactorDefinition
    universe: UniverseSpec
    snapshot: SnapshotRef
    start: date
    end: date
    #: Decision lag (spec §6.1 rule 2): exposures at t use as_of = t - embargo.
    embargo: str = "1d"
    fill: FillPolicy = field(default_factory=FillPolicy)
    seed: int = 0

    # Bias guards — on by default; disabling is explicit and labeled.
    costs_enabled: bool = True
    delisting_handling_enabled: bool = True
    #: Must be set to True to run with costs_enabled=False; all outputs are
    #: then stamped "gross/illustrative" (spec §6.4).
    label_gross_illustrative: bool = False

    def __post_init__(self) -> None:
        if not self.costs_enabled and not self.label_gross_illustrative:
            raise GuardConfigError(
                "costs_enabled=False requires label_gross_illustrative=True; a "
                "cost-free backtest is only ever produced as an explicitly labeled "
                "gross/illustrative diagnostic (spec §6.4)"
            )
        if not self.delisting_handling_enabled:
            raise GuardConfigError(
                "delisting handling cannot be disabled: silently dropping delisted "
                "positions erases losses — the classic survivorship error (spec §6.3)"
            )


class BacktestEngine:
    """Evaluates a factor against a PIT universe with costs and slippage.

    The engine reads only from the immutable snapshot named in the spec —
    research never touches the network (spec §2.4) — and only through
    ``as_of``-scoped store reads.
    """

    def __init__(
        self,
        *,
        store: SignalStore,
        universe_adapter: UniverseAdapter,
        cost_model: CostModelAdapter,
    ) -> None:
        self._store = store
        self._universe = universe_adapter
        self._costs = cost_model

    def run(self, spec: BacktestSpec) -> DiagnosticsReport:
        """Planned per-rebalance behavior:

        1. universe = UniverseAdapter.members_as_of(t) — includes later-
           delisted names, excludes not-yet-listed ones (§6.3);
        2. exposures at as_of = t - embargo (only knowledge_time <= as_of);
        3. next-bar fills with participation caps; costs on every fill (§6.4);
        4. delisting returns applied from PIT corporate-action data (§6.3);
        5. emit DiagnosticsReport: leakage suite, bias scorecard, robustness,
           reproducibility stamp (§6.5) — diagnostics, never a P&L headline.
        """
        raise NotImplementedError("bias-controlled backtest loop is not implemented")
