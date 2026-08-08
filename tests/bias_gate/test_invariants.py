"""Structural bias-guard checks run as a separate CI job.

These tests assert the *structural* guarantees — the ones that make bias hard
to commit rather than merely discouraged. A change that breaks any of these
is, by definition, a change that opens a leakage or survivorship path, and CI
fails it. This does not replace end-to-end storage or leakage-suite tests.
"""

from __future__ import annotations

import inspect
from datetime import UTC, date, datetime

import pytest

from stratum.adapters.base import UniverseSpec
from stratum.backtest.engine import BacktestSpec, GuardConfigError
from stratum.backtest.report import (
    BiasScorecard,
    DiagnosticsReport,
    HonestFramingError,
    ReproStamp,
    assert_honest_framing,
)
from stratum.factors.definition import FactorDefinition, FactorFamily, PitPolicy
from stratum.guard.leakage import ValidatedObservation
from stratum.schema.times import AsOf
from stratum.store.interface import SignalStore, SnapshotRef


def test_store_write_accepts_only_validated_observations() -> None:
    """The leakage guard sits between adapters and the store: the write
    signature's record type must be ValidatedObservation, which only the
    guard can construct."""
    sig = inspect.signature(SignalStore.write)
    annotation = str(sig.parameters["records"].annotation)
    assert "ValidatedObservation" in annotation


def test_store_read_requires_as_of_with_no_default() -> None:
    """There is no unscoped read (spec §4.4 rule 1)."""
    sig = inspect.signature(SignalStore.read)
    as_of = sig.parameters["as_of"]
    assert as_of.kind is inspect.Parameter.KEYWORD_ONLY
    assert as_of.default is inspect.Parameter.empty


def test_validated_observation_has_no_public_constructor() -> None:
    from tests.conftest import make_observation

    with pytest.raises(TypeError):
        ValidatedObservation(make_observation())


def test_factor_pit_policy_only_selects_on_knowledge_time() -> None:
    from stratum.factors.definition import InvalidFactorDefinition

    assert PitPolicy().as_of_rule == "knowledge_time"
    with pytest.raises(InvalidFactorDefinition):
        PitPolicy(as_of_rule="event_time")


def _spec(**overrides: object) -> BacktestSpec:
    defaults: dict[str, object] = {
        "factor": FactorDefinition(
            id="f", version="0.1.0", family=FactorFamily.CUSTOM, description="test"
        ),
        "universe": UniverseSpec(id="sp1500_pit"),
        "snapshot": SnapshotRef(
            content_hash="deadbeef",
            as_of=AsOf.at(datetime(2026, 1, 1, tzinfo=UTC)),
        ),
        "start": date(2023, 1, 1),
        "end": date(2026, 1, 1),
    }
    defaults.update(overrides)
    return BacktestSpec(**defaults)  # type: ignore[arg-type]


def test_backtest_guards_default_on() -> None:
    spec = _spec()
    assert spec.costs_enabled is True
    assert spec.delisting_handling_enabled is True
    assert spec.embargo == "1d"
    assert spec.fill.at.startswith("next_")  # next-bar fills only (spec §6.1)


def test_disabling_costs_without_gross_label_is_loud() -> None:
    with pytest.raises(GuardConfigError, match="gross/illustrative"):
        _spec(costs_enabled=False)
    # Explicit acknowledgment is the only way, and it labels every output.
    spec = _spec(costs_enabled=False, label_gross_illustrative=True)
    assert spec.label_gross_illustrative is True


def test_delisting_handling_cannot_be_disabled() -> None:
    with pytest.raises(GuardConfigError, match="survivorship"):
        _spec(delisting_handling_enabled=False)


def test_bias_scorecard_is_a_required_report_section() -> None:
    # DiagnosticsReport cannot be built without a scorecard and a repro stamp.
    with pytest.raises(TypeError):
        DiagnosticsReport(  # type: ignore[call-arg]
            factor_id="f", factor_version="0.1.0", baseline=None, metrics={}
        )
    report = DiagnosticsReport(
        factor_id="f",
        factor_version="0.1.0",
        baseline="price_momentum_12_1",
        metrics={"rank_ic_5d": 0.02},
        scorecard=BiasScorecard(
            pit_universe_used=True,
            costs_enabled=True,
            no_whole_sample_normalization=True,
            delisting_handling_on=True,
            leakage_suite_passed=True,
        ),
        stamp=ReproStamp(
            snapshot_hash="deadbeef",
            factor_def_hash="cafebabe",
            universe_id="sp1500_pit",
            cost_model_id="cost_default",
            seed=0,
            code_version="0.1.0.dev0",
        ),
    )
    assert report.caveats  # honest-framing caveat present by default


def test_honest_framing_rejects_promise_language() -> None:
    with pytest.raises(HonestFramingError):
        assert_honest_framing("This factor generates alpha.")
    with pytest.raises(HonestFramingError):
        assert_honest_framing("Expected return of 12% annualized.")
    assert_honest_framing("Rank-IC decayed from 0.03 (1d) to 0.01 (21d); turnover 40%/wk.")
