"""The bias-controlled backtest engine (spec §6)."""

from stratum.backtest.engine import (
    BacktestEngine,
    BacktestSpec,
    FillPolicy,
    GuardConfigError,
    SurvivorshipBiasError,
)
from stratum.backtest.leakage_suite import LeakageSuite, LeakageSuiteResult
from stratum.backtest.report import (
    BiasScorecard,
    DiagnosticsReport,
    HonestFramingError,
    ReproStamp,
    assert_honest_framing,
    render_markdown,
)

__all__ = [
    "BacktestEngine",
    "BacktestSpec",
    "BiasScorecard",
    "DiagnosticsReport",
    "FillPolicy",
    "GuardConfigError",
    "HonestFramingError",
    "LeakageSuite",
    "LeakageSuiteResult",
    "ReproStamp",
    "SurvivorshipBiasError",
    "assert_honest_framing",
    "render_markdown",
]
