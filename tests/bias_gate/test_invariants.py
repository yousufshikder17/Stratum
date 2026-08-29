"""Structural bias-guard checks run as a separate CI job.

These tests assert the *structural* guarantees — the ones that make bias hard
to commit rather than merely discouraged. A change that breaks any of these
is, by definition, a change that opens a leakage or survivorship path, and CI
fails it. This does not replace end-to-end storage or leakage-suite tests.
"""

from __future__ import annotations

import ast
import dataclasses
import importlib
import inspect
import pathlib
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


# -- the ingest path (added with MVP items 5 and 6) --------------------------


def test_guarded_writer_is_the_only_sink_implementation_in_core() -> None:
    """Adapters write through `AdapterContext.sink`. If core ever grows a
    second `ObservationSink` that is not the guarded writer, that is a
    guard bypass — this test is where it should fail."""
    import pkgutil

    import stratum
    from stratum.adapters.context import ObservationSink
    from stratum.guard.writer import GuardedWriter

    implementations = []
    for module_info in pkgutil.walk_packages(stratum.__path__, "stratum."):
        module = importlib.import_module(module_info.name)
        for obj in vars(module).values():
            if (
                isinstance(obj, type)
                and obj.__module__.startswith("stratum.")
                and obj is not ObservationSink
                and hasattr(obj, "submit")
                and callable(getattr(obj, "submit", None))
            ):
                implementations.append(obj)
    assert set(implementations) == {GuardedWriter}, (
        "core gained a write path that is not the guarded writer: "
        f"{sorted(c.__qualname__ for c in implementations)}"
    )


def _ingest_source() -> str:
    """The ingest runner's source, read from disk.

    Two traps here, both of which make a naive version of this helper pass
    while proving nothing. `inspect.getsource` reads linecache, which serves
    whatever was cached at first import — so an absence-assertion can pass
    against stale text. And ``import stratum.run.ingest as m`` binds the
    re-exported *function* ``ingest``, not the module, because
    ``stratum/run/__init__.py`` shadows the submodule name; ``m`` would then be
    a function object that quietly satisfies "does not contain .write(".
    """
    module = importlib.import_module("stratum.run.ingest")
    path = getattr(module, "__file__", None)
    assert path is not None, "expected a module, got a shadowing re-export"
    return pathlib.Path(path).read_text(encoding="utf-8")


def test_ingest_never_touches_the_store_directly() -> None:
    """The runner drives adapters and submits through the writer. A direct
    `store.write(...)` in the ingest path would skip resolution and stamping
    even though the guard's proof type would still be required."""
    assert ".write(" not in _ingest_source()


def test_entity_resolution_requires_an_as_of() -> None:
    """Identity changes over time; a resolver without an as_of would merge two
    issuers that happened to share a ticker (spec §4.5)."""
    from stratum.resolver.entity import EntityResolver

    sig = inspect.signature(EntityResolver.resolve)
    as_of = sig.parameters["as_of"]
    assert as_of.kind is inspect.Parameter.KEYWORD_ONLY
    assert as_of.default is inspect.Parameter.empty


def test_universe_membership_requires_an_as_of() -> None:
    """A "current constituents" lookup applied to history is survivorship bias
    (spec §6.3, §9 risk 3)."""
    from stratum.adapters.base import UniverseAdapter

    sig = inspect.signature(UniverseAdapter.members_as_of)
    assert "as_of" in sig.parameters
    assert sig.parameters["as_of"].default is inspect.Parameter.empty


def test_store_inspection_returns_no_observations() -> None:
    """`stratum store` queries the physical table without an as_of. That is
    only safe while every value it returns is an aggregate."""
    from stratum.store.inspect import SignalSummary, StoreSummary, summarize

    returned = inspect.signature(summarize).return_annotation
    assert returned is StoreSummary or returned == "StoreSummary"
    field_types = {
        name: field.type
        for name, field in {
            **StoreSummary.__dataclass_fields__,
            **SignalSummary.__dataclass_fields__,
        }.items()
    }
    assert not any("Observation" in str(t) for t in field_types.values())


def test_adapter_output_types_all_require_a_knowledge_stamp() -> None:
    """Every record type an adapter can emit carries a required, default-less
    `knowledge_time`. There is no constructor path to an unstamped record."""
    from stratum.adapters.base import Bar, CorporateAction, DelistingEvent, SignalValue
    from stratum.schema.times import KnowledgeTime

    for record_type in (Bar, CorporateAction, DelistingEvent, SignalValue):
        field = record_type.__dataclass_fields__["knowledge_time"]
        assert field.default is dataclasses.MISSING, record_type.__name__
        assert field.default_factory is dataclasses.MISSING, record_type.__name__
        assert field.type in ("KnowledgeTime", KnowledgeTime), record_type.__name__


def test_network_adapters_declare_themselves() -> None:
    """`network = true` is what lets the core refuse to run a remote source
    without a rate limit (spec §3.2 rule 5). A network adapter that forgets the
    flag silently opts out of throttling."""
    from stratum.adapters.discovery import discover

    manifests = {d.name: d.manifest for d in discover() if d.manifest is not None}
    remote = {"reddit_sentiment", "sec_edgar", "google_trends"}
    for name, manifest in manifests.items():
        assert manifest.network is (name in remote), name


def test_the_writer_cannot_be_left_holding_rows() -> None:
    """Batched writes mean the tail of a run is unwritten until `flush()`. The
    ingest runner must call it from a `finally`, or a source that dies
    mid-stream silently discards what it already produced.

    Checked against the AST rather than the text, so a comment or a reflow
    cannot break it and a `flush()` moved out of the `finally` cannot slip
    past it.
    """
    tree = ast.parse(_ingest_source())
    flushes_in_finally = [
        node
        for handler in ast.walk(tree)
        if isinstance(handler, ast.Try)
        for stmt in handler.finalbody
        for node in ast.walk(stmt)
        if isinstance(node, ast.Attribute) and node.attr == "flush"
    ]
    all_flushes = [
        node for node in ast.walk(tree) if isinstance(node, ast.Attribute) and node.attr == "flush"
    ]
    assert all_flushes, "the ingest runner never flushes the write buffer"
    assert len(flushes_in_finally) == len(all_flushes), (
        "every writer.flush() must sit in a finally block, so a source that "
        "raises mid-stream still persists what it produced"
    )
