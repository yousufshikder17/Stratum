"""FactorEngine: PIT reads -> cross-sectional transform pipeline (spec §5.4)."""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

from stratum.factors.definition import load_factor
from stratum.factors.engine import FactorEngine
from stratum.guard.leakage import IngestMode, LeakageGuard
from stratum.schema.data_class import DataClass
from stratum.schema.observation import Observation
from stratum.schema.payloads import FundamentalFact, MarketBar, SocialAttention
from stratum.schema.times import AsOf, EventTime, KnowledgeTime
from stratum.store.duckdb_store import DuckDBSignalStore
from stratum.store.snapshot_store import SnapshotStore
from tests.conftest import make_manifest

GUARD = LeakageGuard(now=lambda: datetime(2026, 8, 1, tzinfo=UTC))
MANIFEST = make_manifest()


def validated(*observations):
    return [GUARD.validate(o, manifest=MANIFEST, mode=IngestMode.BACKFILL) for o in observations]


def attention(obs_id, sid, event_day, know_dt, velocity, mention_count=100):
    return Observation(
        observation_id=obs_id,
        signal_type="social.attention",
        run_id="run-1",
        source_id="test",
        adapter_id="test",
        security_id=sid,
        native_entity=sid,
        event_time=EventTime.at(datetime(2026, *event_day, tzinfo=UTC)),
        knowledge_time=KnowledgeTime.at(know_dt),
        vintage_id="v1",
        data_class=DataClass.PUBLIC_AGG,
        license_tag="test",
        payload=SocialAttention(
            mention_count=mention_count,
            unique_authors=40,
            velocity=velocity,
        ),
    )


def bar(sid, day, close):
    dt = datetime(2026, *day, tzinfo=UTC)
    return Observation(
        observation_id=f"bar-{sid}-{day[0]}{day[1]}",
        signal_type="market.bar",
        run_id="run-1",
        source_id="test",
        adapter_id="test",
        security_id=sid,
        native_entity=sid,
        event_time=EventTime.at(dt),
        knowledge_time=KnowledgeTime.at(dt),
        vintage_id="v1",
        data_class=DataClass.PUBLIC_AGG,
        license_tag="test",
        payload=MarketBar(open=close, high=close, low=close, close=close, volume=10_000.0),
    )


JUL1 = datetime(2026, 7, 1, 12, tzinfo=UTC)
JUL8 = datetime(2026, 7, 8, 12, tzinfo=UTC)


@pytest.fixture
def store(tmp_path):
    s = DuckDBSignalStore(tmp_path / "stratum.duckdb")
    rows = [
        # Attention velocities on two dates so pct_change has a base.
        attention("att-AAA-71", "AAA", (7, 1), JUL1, 1.0),
        attention("att-AAA-78", "AAA", (7, 8), JUL8, 2.0),
        attention("att-BBB-71", "BBB", (7, 1), JUL1, 3.0),
        attention("att-BBB-78", "BBB", (7, 8), JUL8, 3.3),
        attention("att-CCC-71", "CCC", (7, 1), JUL1, 2.0),
        attention("att-CCC-78", "CCC", (7, 8), JUL8, 4.4),
        bar("AAA", (7, 9), 100.0),
        bar("BBB", (7, 9), 200.0),
        bar("CCC", (7, 9), 300.0),
    ]
    s.write(validated(*rows), run_id="run-1")
    snap = s.snapshot(as_of=AsOf.at(datetime(2026, 8, 1, tzinfo=UTC)), target_dir=tmp_path / "snap")
    snapshot_store = SnapshotStore(snap.path)
    yield snapshot_store, snap
    snapshot_store.close()
    s.close()


def write_factor_yaml(tmp_path: Path, text: str):
    path = tmp_path / "factor.yaml"
    path.write_text(text.strip())
    return load_factor(path)


REBALANCE = [date(2026, 7, 15)]


def test_build_zscore_pipeline(store, tmp_path):
    s, snap = store
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: test_velocity
  version: 0.1.0
  family: sentiment
inputs:
  - signal: social.attention
    field: velocity
pit:
  embargo: 1d
transform:
  - op: zscore
""",
    )
    panel = FactorEngine(store=s, snapshot=snap).build(definition, rebalance_dates=REBALANCE)
    values = panel.exposures[date(2026, 7, 15)]
    assert set(values) == {"AAA", "BBB", "CCC"}
    assert sum(values.values()) == pytest.approx(0.0, abs=1e-9)
    assert panel.coverage[date(2026, 7, 15)] == 3
    assert len(panel.build_hash) == 64


def test_pct_change_window_base_is_pit_scoped(store, tmp_path):
    s, snap = store
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: test_pct
  version: 0.1.0
  family: sentiment
inputs:
  - signal: social.attention
    field: velocity
    window: 7d
pit:
  embargo: 1d
transform:
  - op: pct_change
""",
    )
    panel = FactorEngine(store=s, snapshot=snap).build(definition, rebalance_dates=REBALANCE)
    values = panel.exposures[date(2026, 7, 15)]
    # as_of Jul 14; base read at Jul 7 -> latest knowable velocity is Jul 1's.
    assert values["AAA"] == pytest.approx((2.0 - 1.0) / abs(1.0))
    assert values["BBB"] == pytest.approx(0.3 / 3.0)
    assert values["CCC"] == pytest.approx(2.4 / 2.0)


def test_embargo_excludes_fresh_knowledge(store, tmp_path):
    s, snap = store
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: test_embargo
  version: 0.1.0
  family: sentiment
inputs:
  - signal: social.attention
    field: velocity
pit:
  embargo: 45d
transform:
  - op: zscore
""",
    )
    # t - 45d lands before anything was knowable -> empty cross-section.
    panel = FactorEngine(store=s, snapshot=snap).build(definition, rebalance_dates=REBALANCE)
    assert panel.exposures[date(2026, 7, 15)] == {}
    assert panel.coverage[date(2026, 7, 15)] == 0


def test_unknown_op_fails_loudly(store, tmp_path):
    s, snap = store
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: test_bad
  version: 0.1.0
  family: custom
inputs:
  - signal: social.attention
    field: velocity
transform:
  - op: full_sample_standardize
""",
    )
    engine = FactorEngine(store=s, snapshot=snap)
    with pytest.raises(KeyError, match="unknown transform op"):
        engine.build(definition, rebalance_dates=REBALANCE)


def test_market_bar_default_field_is_close(store, tmp_path):
    s, snap = store
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: test_price
  version: 0.1.0
  family: value
inputs:
  - market: market.bar
transform:
  - op: cross_sectional_rank
""",
    )
    panel = FactorEngine(store=s, snapshot=snap).build(definition, rebalance_dates=REBALANCE)
    values = panel.exposures[date(2026, 7, 15)]
    assert values["AAA"] == pytest.approx(0.0)
    assert values["CCC"] == pytest.approx(1.0)


def test_build_hash_binds_definition_and_snapshot(store, tmp_path):
    s, snap = store
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: test_hash
  version: 0.1.0
  family: custom
inputs:
  - signal: social.attention
    field: velocity
transform:
  - op: zscore
""",
    )
    engine = FactorEngine(store=s, snapshot=snap)
    first = engine.build(definition, rebalance_dates=REBALANCE)
    second = engine.build(definition, rebalance_dates=REBALANCE)
    assert first.build_hash == second.build_hash


def test_reference_factor_runs_and_applies_coverage_floor(store):
    s, snap = store
    definition = load_factor(
        Path(__file__).parents[1]
        / "src"
        / "stratum"
        / "factors"
        / "reference"
        / "reddit_attention_momentum.yaml"
    )
    panel = FactorEngine(store=s, snapshot=snap).build(
        definition,
        rebalance_dates=[date(2026, 7, 12)],
    )
    assert panel.coverage[date(2026, 7, 12)] == 3


def test_minimum_coverage_excludes_thin_observations(store, tmp_path):
    live = DuckDBSignalStore(tmp_path / "coverage.duckdb")
    live.write(
        validated(
            attention("att-AAA-cov", "AAA", (7, 8), JUL8, 2.0),
            attention("att-BBB-cov", "BBB", (7, 8), JUL8, 3.0),
            attention("att-DDD-cov", "DDD", (7, 8), JUL8, 9.0, mention_count=10),
        ),
        run_id="run-2",
    )
    snap = live.snapshot(
        as_of=AsOf.at(datetime(2026, 8, 1, tzinfo=UTC)),
        target_dir=tmp_path / "coverage-snapshot",
    )
    snapshot_store = SnapshotStore(snap.path)
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: coverage_floor
  version: 0.1.0
  family: sentiment
inputs:
  - signal: social.attention
    field: velocity
    min_coverage: 50
pit:
  embargo: 1d
transform:
  - op: cross_sectional_rank
""",
    )
    try:
        panel = FactorEngine(store=snapshot_store, snapshot=snap).build(
            definition,
            rebalance_dates=REBALANCE,
        )
        assert set(panel.exposures[REBALANCE[0]]) == {"AAA", "BBB"}
    finally:
        snapshot_store.close()
        live.close()


def test_rebalance_after_snapshot_cutoff_is_rejected(store, tmp_path):
    s, snap = store
    definition = write_factor_yaml(
        tmp_path,
        """
factor:
  id: cutoff
  version: 0.1.0
  family: custom
inputs:
  - market: market.bar
""",
    )
    with pytest.raises(ValueError, match="exceeds snapshot"):
        FactorEngine(store=s, snapshot=snap).build(
            definition,
            rebalance_dates=[date(2026, 8, 3)],
        )


def test_multi_series_input_requires_and_honors_series_id(tmp_path):
    live = DuckDBSignalStore(tmp_path / "series.duckdb")
    event = datetime(2025, 12, 31, tzinfo=UTC)
    known = datetime(2026, 2, 1, tzinfo=UTC)

    def fact(obs_id, sid, concept, value):
        return Observation(
            observation_id=obs_id,
            signal_type="fundamental.fact",
            series_id=f"{concept}|USD||2025-12-31",
            run_id="run-1",
            source_id="test",
            adapter_id="test",
            security_id=sid,
            native_entity=sid,
            event_time=EventTime.at(event),
            knowledge_time=KnowledgeTime.at(known),
            vintage_id="filing-1",
            data_class=DataClass.PUBLIC_AGG,
            license_tag="test",
            payload=FundamentalFact(
                concept=concept,
                value=value,
                unit="USD",
                period_end=date(2025, 12, 31),
                period_type="instant",
            ),
        )

    live.write(
        validated(
            fact("fact-AAA-assets", "AAA", "Assets", 100.0),
            fact("fact-AAA-revenue", "AAA", "Revenues", 25.0),
            fact("fact-BBB-assets", "BBB", "Assets", 200.0),
            fact("fact-BBB-revenue", "BBB", "Revenues", 40.0),
        ),
        run_id="run-1",
    )
    snap = live.snapshot(
        as_of=AsOf.at(datetime(2026, 3, 1, tzinfo=UTC)),
        target_dir=tmp_path / "series-snapshot",
    )
    snapshot_store = SnapshotStore(snap.path)
    selected = write_factor_yaml(
        tmp_path,
        """
factor:
  id: assets
  version: 0.1.0
  family: value
inputs:
  - signal: fundamental.fact
    series_id: Assets|USD||2025-12-31
    field: value
""",
    )
    ambiguous = write_factor_yaml(
        tmp_path,
        """
factor:
  id: ambiguous
  version: 0.1.0
  family: value
inputs:
  - signal: fundamental.fact
    field: value
""",
    )
    engine = FactorEngine(store=snapshot_store, snapshot=snap)
    try:
        panel = engine.build(selected, rebalance_dates=[date(2026, 2, 3)])
        assert panel.exposures[date(2026, 2, 3)] == {"AAA": 100.0, "BBB": 200.0}
        with pytest.raises(ValueError, match="series_id"):
            engine.build(ambiguous, rebalance_dates=[date(2026, 2, 3)])
    finally:
        snapshot_store.close()
        live.close()
