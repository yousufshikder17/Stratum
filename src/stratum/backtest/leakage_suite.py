"""Types for a planned post-run leakage suite; execution is not implemented."""

from __future__ import annotations

from dataclasses import dataclass

from stratum.backtest.engine import BacktestSpec

__all__ = ["LeakageSuite", "LeakageSuiteResult"]


@dataclass(frozen=True, kw_only=True, slots=True)
class LeakageSuiteResult:
    future_shuffle_passed: bool
    embargo_sensitivity_passed: bool
    as_of_recomputation_passed: bool

    @property
    def passed(self) -> bool:
        return (
            self.future_shuffle_passed
            and self.embargo_sensitivity_passed
            and self.as_of_recomputation_passed
        )


class LeakageSuite:
    def run(self, spec: BacktestSpec) -> LeakageSuiteResult:
        """Planned checks:

        - **future-shuffle test**: re-run slices with shuffled future data;
          performance that depends on it means leakage;
        - **embargo-sensitivity sweep**: performance collapsing as embargo
          grows from 0 flags knowledge-time dishonesty;
        - **as-of recomputation**: re-run a sample of dates and confirm
          exposures match what the store says was knowable.
        """
        raise NotImplementedError("computational leakage suite is not implemented")
