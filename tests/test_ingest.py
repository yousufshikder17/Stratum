"""The guarded ingestion run (spec §2.2): adapter -> resolve -> guard -> store.

These drive the *real* pipeline — real adapters, the real leakage guard, a
real DuckDB store on disk — because the guarantees under test are structural.
A mocked store could not tell us whether the guard is actually in the path.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

from stratum.adapters.sec_edgar import SecEdgarError
from stratum.config import ConfigError, ResearchConfig
from stratum.guard.leakage import IngestMode, LeakageGuard
from stratum.run.ingest import format_report, ingest
from stratum.schema.times import AsOf, EventTime
from stratum.store.duckdb_store import DuckDBSignalStore
from stratum.store.inspect import summarize
from tests.conftest import write_csv

NOW = datetime(2026, 7, 1, tzinfo=UTC)

BARS_HEADER = ["security", "date", "open", "high", "low", "close", "volume"]
BARS = [
    ["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000"],
    ["AAA", "2024-01-03", "10.2", "10.9", "10.1", "10.7", "1200"],
    ["ZZZ", "2024-01-02", "3.0", "3.1", "2.7", "2.8", "5000"],
]
MEMBERSHIP_HEADER = ["security", "added_on", "announced_on", "removed_on", "removed_announced_on"]
MEMBERSHIP = [
    ["AAA", "2020-01-02", "2019-12-20", "", ""],
    ["ZZZ", "2019-06-03", "2019-05-28", "2024-02-16", "2024-02-09"],
]
ENTITIES_HEADER = ["kind", "value", "security_id", "valid_from", "valid_to", "known_from"]
ENTITIES = [
    ["ticker", "AAA", "SEC-1", "2015-01-02", "", "2015-01-02"],
    ["ticker", "ZZZ", "SEC-3", "2019-06-03", "2024-02-16", "2019-05-28"],
]


def build_config(tmp_path: Path, *, with_resolver: bool = True, extra: str = "") -> Path:
    write_csv(tmp_path / "bars.csv", BARS_HEADER, BARS)
    write_csv(tmp_path / "membership.csv", MEMBERSHIP_HEADER, MEMBERSHIP)
    write_csv(tmp_path / "entities.csv", ENTITIES_HEADER, ENTITIES)
    resolver = '[resolver]\ntables = "entities.csv"\nversion = "test-v0"\n' if with_resolver else ""
    config = tmp_path / "research.toml"
    config.write_text(
        f"""
[run]
id = "run-under-test"

[store]
path = "stratum.duckdb"

{resolver}
[backfill]
start = "2024-01-01"
end = "2024-03-01"

[[adapters]]
id = "market_csv"
entity_kind = "ticker"
  [adapters.config]
  bars_path = "bars.csv"

[[adapters]]
id = "universe_csv"
entity_kind = "ticker"
  [adapters.config]
  membership_path = "membership.csv"
  universe_id = "sp1500_pit"
{extra}
""",
        encoding="utf-8",
    )
    return config


async def run(tmp_path: Path, **kwargs: object) -> object:
    config = ResearchConfig.load(build_config(tmp_path, **kwargs))  # type: ignore[arg-type]
    return await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)


def _market_manifest():
    """The real market_csv manifest (network = false)."""
    from stratum.adapters.discovery import discover

    return next(d.manifest for d in discover() if d.name == "market_csv")


def _provider_config(adapter_id: str, provider: str):
    from stratum.config import AdapterConfig

    return AdapterConfig(id=adapter_id, provider=provider)


def make_bar_observation():
    from stratum.schema.data_class import DataClass
    from stratum.schema.observation import Observation
    from stratum.schema.payloads import MarketBar
    from stratum.schema.times import EventTime as ET
    from stratum.schema.times import KnowledgeTime as KT

    return Observation(
        observation_id="01J0000000000000000000PART",
        signal_type="market.bar",
        run_id="partial",
        source_id="market_csv",
        adapter_id="market_csv",
        native_entity="AAA",
        event_time=ET.at(datetime(2024, 1, 2, tzinfo=UTC)),
        knowledge_time=KT.at(datetime(2024, 1, 2, 21, tzinfo=UTC)),
        data_class=DataClass.PUBLIC_AGG,
        license_tag="user-supplied-csv",
        payload=MarketBar(open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0),
    )


# -- the happy path -----------------------------------------------------------


async def test_ingest_writes_through_the_guard(tmp_path: Path) -> None:
    report = await run(tmp_path)
    assert report.clean  # type: ignore[attr-defined]
    assert report.rows_written == 4  # 3 bars + 1 membership edge in window
    assert {r.adapter_id for r in report.results} == {"market_csv", "universe_csv"}


async def test_store_holds_what_was_ingested(tmp_path: Path) -> None:
    config = ResearchConfig.load(build_config(tmp_path))
    await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    store = DuckDBSignalStore(config.store_path)
    try:
        summary = summarize(store)
        by_type = {s.signal_type: s for s in summary.signals}
        assert by_type["market.bar"].rows == 3
        assert by_type["market.bar"].resolution_coverage == 1.0
        assert by_type["meta.coverage"].rows == 1
    finally:
        store.close()


async def test_reads_are_as_of_scoped_end_to_end(tmp_path: Path) -> None:
    """The payoff: a bar stamped knowable at 21:00 is invisible at 12:00 the
    same day, straight through the ingest path."""
    config = ResearchConfig.load(build_config(tmp_path))
    await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    store = DuckDBSignalStore(config.store_path)
    try:
        midday = AsOf.at(datetime(2024, 1, 2, 12, 0, tzinfo=UTC))
        evening = AsOf.at(datetime(2024, 1, 2, 23, 0, tzinfo=UTC))
        assert list(store.read(signal_type="market.bar", as_of=midday)) == []
        visible = list(store.read(signal_type="market.bar", as_of=evening))
        assert {o.native_entity for o in visible} == {"AAA", "ZZZ"}
    finally:
        store.close()


async def test_resolution_attaches_canonical_ids(tmp_path: Path) -> None:
    config = ResearchConfig.load(build_config(tmp_path))
    await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    store = DuckDBSignalStore(config.store_path)
    try:
        rows = list(
            store.read(
                signal_type="market.bar",
                as_of=AsOf.at(datetime(2024, 2, 1, tzinfo=UTC)),
                event_start=EventTime.at(datetime(2024, 1, 1, tzinfo=UTC)),
            )
        )
        assert {o.security_id for o in rows} == {"SEC-1", "SEC-3"}
    finally:
        store.close()


async def test_unresolved_rows_are_stored_not_dropped(tmp_path: Path) -> None:
    """An unknown key means "we don't know who this is", which is a NULL
    security_id — not a dropped row and not a guess."""
    config = ResearchConfig.load(build_config(tmp_path))
    # Drop ZZZ from the tables after the fixtures are laid down, so one entity
    # in the bars has no mapping at all.
    write_csv(
        tmp_path / "entities.csv",
        ENTITIES_HEADER,
        [["ticker", "AAA", "SEC-1", "2015-01-02", "", "2015-01-02"]],
    )
    report = await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    market = next(r for r in report.results if r.adapter_id == "market_csv")
    assert market.resolved == 2  # the two AAA bars
    assert market.unresolved == 1  # ZZZ has no mapping
    assert market.rows_written == 3


async def test_ingest_is_idempotent(tmp_path: Path) -> None:
    """Re-running the same backfill re-reads the same rows and writes none."""
    config = ResearchConfig.load(build_config(tmp_path))
    first = await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    second = await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    assert first.rows_written == 4
    assert second.rows_written == 0
    assert second.rows_skipped == 4


async def test_only_one_adapter(tmp_path: Path) -> None:
    config = ResearchConfig.load(build_config(tmp_path))
    report = await ingest(config, mode=IngestMode.BACKFILL, only="market_csv", clock=lambda: NOW)
    assert [r.adapter_id for r in report.results] == ["market_csv"]


async def test_unknown_adapter_selection_is_rejected(tmp_path: Path) -> None:
    config = ResearchConfig.load(build_config(tmp_path))
    with pytest.raises(ConfigError, match="not configured"):
        await ingest(config, mode=IngestMode.BACKFILL, only="sec_edgar", clock=lambda: NOW)


async def test_backfill_without_a_window_is_refused(tmp_path: Path) -> None:
    """An unbounded historical pull has no defined end and cannot be reproduced."""
    path = build_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            '[backfill]\nstart = "2024-01-01"\nend = "2024-03-01"\n', ""
        ),
        encoding="utf-8",
    )
    config = ResearchConfig.load(path)
    with pytest.raises(ConfigError, match="backfill"):
        await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)


# -- rejections ---------------------------------------------------------------


async def test_future_knowledge_is_rejected_and_reported(tmp_path: Path) -> None:
    """A row the guard refuses is dropped from the store *and* named in the
    report. A guard that silently discarded rows would look like a data gap."""
    config = ResearchConfig.load(build_config(tmp_path))
    report = await ingest(
        config,
        mode=IngestMode.BACKFILL,
        clock=lambda: NOW,
        # Pretend "now" is 2023: every 2024 bar is stamped in the future.
        guard=LeakageGuard(now=lambda: datetime(2023, 1, 1, tzinfo=UTC)),
    )
    assert not report.clean
    assert report.rows_written == 0
    rules = {r.rule for r in report.rejections}
    assert rules == {"future-knowledge"}
    assert "REJECTED [future-knowledge]" in "\n".join(format_report(report))


async def test_report_formats_totals(tmp_path: Path) -> None:
    report = await run(tmp_path)
    text = "\n".join(format_report(report))
    assert "run run-under-test" in text
    assert "resolver tables: test-v0" in text
    assert "4 written, 0 skipped, 0 rejected" in text


# -- poll ---------------------------------------------------------------------


async def test_poll_mode_ingests_what_is_knowable(tmp_path: Path) -> None:
    config = ResearchConfig.load(build_config(tmp_path))
    report = await ingest(config, mode=IngestMode.POLL, clock=lambda: NOW)
    assert report.clean
    assert report.rows_written > 0
    assert {r.mode for r in report.results} == {"poll"}


# -- batching -----------------------------------------------------------------


async def test_writer_buffers_and_the_runner_flushes(tmp_path: Path) -> None:
    """Validation stays per row; only the write is batched. Nothing may be
    left in the buffer when the run ends."""
    config = ResearchConfig.load(build_config(tmp_path))
    report = await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    assert report.rows_written == 4
    store = DuckDBSignalStore(config.store_path)
    try:
        assert summarize(store).rows == 4
    finally:
        store.close()


async def test_a_tiny_batch_size_produces_the_same_store(tmp_path: Path) -> None:
    """Batch size is a performance knob, never a semantic one."""
    from stratum.guard.writer import GuardedWriter

    original = GuardedWriter.__init__

    def tiny(self, **kwargs):  # type: ignore[no-untyped-def]
        original(self, **{**kwargs, "batch_size": 1})

    config = ResearchConfig.load(build_config(tmp_path))
    GuardedWriter.__init__ = tiny  # type: ignore[method-assign]
    try:
        report = await ingest(config, mode=IngestMode.BACKFILL, clock=lambda: NOW)
    finally:
        GuardedWriter.__init__ = original  # type: ignore[method-assign]
    assert report.rows_written == 4


async def test_a_source_that_dies_midstream_keeps_what_it_produced(tmp_path: Path) -> None:
    """The flush lives in a `finally`: a provider that drops the connection
    halfway should not discard the rows it already handed over."""
    from stratum.guard.leakage import LeakageGuard
    from stratum.guard.writer import GuardedWriter

    config = ResearchConfig.load(build_config(tmp_path))
    store = DuckDBSignalStore(config.store_path)
    manifest = _market_manifest()
    writer = GuardedWriter(
        store=store,
        guard=LeakageGuard(now=lambda: NOW),
        manifest=manifest,
        mode=IngestMode.BACKFILL,
        run_id="partial",
        clock=lambda: NOW,
        batch_size=1000,
    )
    try:
        observation = make_bar_observation()
        await writer.submit(observation)
        assert writer.pending == 1  # buffered, not yet durable
        await writer.flush()
        assert writer.pending == 0
        assert summarize(store).rows == 1
    finally:
        store.close()


# -- rate limiting ------------------------------------------------------------


async def test_file_adapters_are_not_throttled(tmp_path: Path) -> None:
    report = await run(tmp_path)
    assert all(r.throttled_seconds == 0.0 for r in report.results)  # type: ignore[attr-defined]


async def test_network_adapter_without_a_rate_limit_is_refused(tmp_path: Path) -> None:
    """`network = true` in the manifest means the core must be told the budget.
    Forgetting should fail at startup, not at the provider's discretion."""
    path = build_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8")
        + '\n[[adapters]]\nid = "sec_edgar"\n  [adapters.config]\n',
        encoding="utf-8",
    )
    config = ResearchConfig.load(path)
    with pytest.raises(ConfigError, match="no rate limit"):
        await ingest(config, mode=IngestMode.BACKFILL, only="sec_edgar", clock=lambda: NOW)


async def test_a_configured_budget_admits_the_network_adapter(tmp_path: Path) -> None:
    path = build_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8")
        + "\n[rate_limits.sec_edgar]\nrequests_per_second = 8.0\n"
        + '\n[[adapters]]\nid = "sec_edgar"\n  [adapters.config]\n',
        encoding="utf-8",
    )
    config = ResearchConfig.load(path)
    assert config.rate_limits["sec_edgar"].requests_per_second == 8.0
    # Provider configuration is reached only after the shared rate-limit gate.
    with pytest.raises(SecEdgarError, match="user_agent"):
        await ingest(config, mode=IngestMode.BACKFILL, only="sec_edgar", clock=lambda: NOW)


async def test_adapters_sharing_a_provider_share_one_bucket(tmp_path: Path) -> None:
    """Two adapters against one host are one client to that host."""
    from stratum.adapters.ratelimit import RateLimitConfig, TokenBucketRateLimiter
    from stratum.run.ingest import _rate_limiter

    limits = {"edgar": RateLimitConfig(requests_per_second=1.0)}
    cache: dict[str, TokenBucketRateLimiter] = {}
    manifest = _market_manifest()
    first = _rate_limiter(_provider_config("a", "edgar"), manifest, limits, cache)
    second = _rate_limiter(_provider_config("b", "edgar"), manifest, limits, cache)
    assert first is second
