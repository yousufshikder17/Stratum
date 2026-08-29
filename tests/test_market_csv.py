"""CSV market-data adapter (spec §3.4): honest publication stamps, PIT windows."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from stratum.adapters.base import TimeWindow
from stratum.adapters.context import AdapterContext
from stratum.adapters.csv_common import CsvFormatError
from stratum.adapters.market_csv import CsvMarketDataAdapter
from stratum.schema.data_class import DataClass
from stratum.schema.observation import Observation
from stratum.schema.payloads import MarketBar, MarketCorporateAction, MarketDelisting
from tests.conftest import write_csv

BARS_HEADER = ["security", "date", "open", "high", "low", "close", "volume"]
BARS = [
    ["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000"],
    ["AAA", "2024-01-03", "10.2", "10.9", "10.1", "10.7", "1200"],
    ["BBB", "2024-01-02", "50.0", "51.0", "49.5", "50.5", "900"],
]

WINDOW = TimeWindow(start=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 2, 1, tzinfo=UTC))


class _Sink:
    def __init__(self) -> None:
        self.submitted: list[Observation] = []

    async def submit(self, observation: Observation) -> None:
        self.submitted.append(observation)


class _NoLimit:
    async def acquire(self, cost: int = 1) -> None:
        return None


class _NoSecrets:
    def get(self, key: str) -> str | None:
        return None


def make_ctx(tmp_path: Path, *, now: datetime | None = None) -> AdapterContext:
    stamp = now or datetime(2026, 7, 1, tzinfo=UTC)
    return AdapterContext(
        logger=logging.getLogger("test"),
        clock=lambda: stamp,
        data_dir=tmp_path,
        secrets=_NoSecrets(),
        rate_limiter=_NoLimit(),
        sink=_Sink(),
        run_id="run-test",
    )


async def configured(tmp_path: Path, **overrides: object) -> CsvMarketDataAdapter:
    config: dict[str, object] = {
        "bars_path": str(write_csv(tmp_path / "bars.csv", BARS_HEADER, BARS))
    }
    config.update(overrides)
    adapter = CsvMarketDataAdapter()
    await adapter.configure(config, make_ctx(tmp_path))
    return adapter


# -- knowledge stamps ---------------------------------------------------------


async def test_bar_is_knowable_at_session_close_not_midnight(tmp_path: Path) -> None:
    """Midnight on the bar date would let a strategy trade on a close it had
    not yet seen — the exact leak the two clocks exist to prevent."""
    adapter = await configured(tmp_path)
    bars = [b async for b in adapter.bars([], WINDOW)]
    first = next(b for b in bars if b.native_entity == "AAA")
    assert first.event_time.as_datetime() == datetime(2024, 1, 2, tzinfo=UTC)
    assert first.knowledge_time.as_datetime() == datetime(2024, 1, 2, 21, 0, tzinfo=UTC)


async def test_session_close_is_configurable(tmp_path: Path) -> None:
    adapter = await configured(tmp_path, session_close_utc="16:30")
    bar = next(iter([b async for b in adapter.bars(["AAA"], WINDOW)]))
    assert bar.knowledge_time.as_datetime() == datetime(2024, 1, 2, 16, 30, tzinfo=UTC)


async def test_explicit_knowledge_time_column_wins(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path / "bars.csv",
        [*BARS_HEADER, "knowledge_time"],
        [["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000", "2024-01-03T08:00:00Z"]],
    )
    adapter = CsvMarketDataAdapter()
    await adapter.configure({"bars_path": str(path)}, make_ctx(tmp_path))
    bar = next(iter([b async for b in adapter.bars([], WINDOW)]))
    assert bar.knowledge_time.as_datetime() == datetime(2024, 1, 3, 8, 0, tzinfo=UTC)


async def test_naive_knowledge_time_is_rejected(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path / "bars.csv",
        [*BARS_HEADER, "knowledge_time"],
        [["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000", "2024-01-03T08:00:00"]],
    )
    adapter = CsvMarketDataAdapter()
    await adapter.configure({"bars_path": str(path)}, make_ctx(tmp_path))
    with pytest.raises(CsvFormatError, match="needs a timezone"):
        [b async for b in adapter.bars([], WINDOW)]


async def test_corporate_action_knowledge_is_the_announcement(tmp_path: Path) -> None:
    """The ex-date is the event; the announcement is the knowledge. Stamping a
    split with its ex-date would hide that the market knew weeks earlier."""
    actions = write_csv(
        tmp_path / "actions.csv",
        ["security", "action_type", "ex_date", "announced_on", "ratio"],
        [["AAA", "split", "2024-01-20", "2024-01-05", "2.0"]],
    )
    adapter = await configured(tmp_path, corporate_actions_path=str(actions))
    action = next(iter([a async for a in adapter.corporate_actions([], WINDOW)]))
    assert action.event_time.as_datetime() == datetime(2024, 1, 20, tzinfo=UTC)
    assert action.knowledge_time.as_datetime() == datetime(2024, 1, 5, 21, 0, tzinfo=UTC)
    assert action.ratio == 2.0


async def test_action_without_announcement_is_rejected(tmp_path: Path) -> None:
    actions = write_csv(
        tmp_path / "actions.csv",
        ["security", "action_type", "ex_date"],
        [["AAA", "split", "2024-01-20"]],
    )
    adapter = await configured(tmp_path, corporate_actions_path=str(actions))
    with pytest.raises(CsvFormatError, match="announced_on"):
        [a async for a in adapter.corporate_actions([], WINDOW)]


async def test_delistings_carry_reason_and_policy(tmp_path: Path) -> None:
    delistings = write_csv(
        tmp_path / "delistings.csv",
        ["security", "reason", "last_trade_date", "announced_on", "final_value_policy"],
        [["ZZZ", "bankruptcy", "2024-01-15", "2024-01-09", "zero"]],
    )
    adapter = await configured(tmp_path, delistings_path=str(delistings))
    event = next(iter([d async for d in adapter.delistings(WINDOW)]))
    assert event.reason == "bankruptcy"
    assert event.final_value_policy == "zero"
    assert event.knowledge_time.as_datetime() == datetime(2024, 1, 9, 21, 0, tzinfo=UTC)


# -- observations -------------------------------------------------------------


async def test_backfill_emits_typed_payloads(tmp_path: Path) -> None:
    actions = write_csv(
        tmp_path / "actions.csv",
        ["security", "action_type", "ex_date", "announced_on", "amount"],
        [["AAA", "dividend", "2024-01-10", "2024-01-04", "0.5"]],
    )
    delistings = write_csv(
        tmp_path / "delistings.csv",
        ["security", "reason", "last_trade_date", "announced_on"],
        [["ZZZ", "acquisition", "2024-01-25", "2024-01-11"]],
    )
    adapter = await configured(
        tmp_path, corporate_actions_path=str(actions), delistings_path=str(delistings)
    )
    observations = [o async for o in adapter.backfill(WINDOW)]
    kinds = {type(o.payload) for o in observations}
    assert kinds == {MarketBar, MarketCorporateAction, MarketDelisting}
    assert len(observations) == 5
    assert {o.signal_type for o in observations} == {
        "market.bar",
        "market.corporate_action",
        "market.delisting",
    }


async def test_backfill_window_is_half_open_on_the_event_axis(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    window = TimeWindow(
        start=datetime(2024, 1, 2, tzinfo=UTC), end=datetime(2024, 1, 3, tzinfo=UTC)
    )
    days = {o.event_time.as_datetime().date().isoformat() async for o in adapter.backfill(window)}
    assert days == {"2024-01-02"}


async def test_ids_are_stable_across_reads(tmp_path: Path) -> None:
    """Re-reading the same file must produce the same ids, or every re-pull
    would duplicate the store."""
    adapter = await configured(tmp_path)
    first = [o.observation_id async for o in adapter.backfill(WINDOW)]
    second = [o.observation_id async for o in adapter.backfill(WINDOW)]
    assert first == second
    assert len(set(first)) == len(first)


async def test_license_tag_comes_from_the_manifest(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    observation = next(iter([o async for o in adapter.backfill(WINDOW)]))
    assert observation.license_tag == "user-supplied-csv"
    assert observation.adapter_id == "market_csv"
    assert observation.run_id == "run-test"


async def test_data_class_defaults_to_licensed_and_is_configurable(tmp_path: Path) -> None:
    """Conservative default: price data is usually vendor-licensed."""
    default = await configured(tmp_path)
    assert next(iter([o async for o in default.backfill(WINDOW)])).data_class is DataClass.LICENSED
    public = await configured(tmp_path, data_class="public_agg")
    assert next(iter([o async for o in public.backfill(WINDOW)])).data_class is DataClass.PUBLIC_AGG


async def test_unknown_data_class_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(CsvFormatError, match="data_class"):
        await configured(tmp_path, data_class="secret")


# -- poll ---------------------------------------------------------------------


async def test_poll_advances_on_the_knowledge_axis(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    first = [o async for o in adapter.poll()]
    assert first, "first poll should catch up on everything already knowable"
    second = [o async for o in adapter.poll()]
    assert second == [], "nothing new became knowable between polls"


async def test_poll_withholds_rows_not_yet_knowable(tmp_path: Path) -> None:
    """A bar dated after the clock is not emitted early — the file may run
    ahead of the world, but knowledge may not."""
    path = write_csv(
        tmp_path / "bars.csv",
        BARS_HEADER,
        [
            ["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000"],
            ["AAA", "2024-06-01", "11.0", "11.5", "10.8", "11.2", "1000"],
        ],
    )
    adapter = CsvMarketDataAdapter()
    await adapter.configure(
        {"bars_path": str(path)}, make_ctx(tmp_path, now=datetime(2024, 3, 1, tzinfo=UTC))
    )
    days = {o.event_time.as_datetime().date().isoformat() async for o in adapter.poll()}
    assert days == {"2024-01-02"}


# -- configuration ------------------------------------------------------------


async def test_missing_bars_path_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(CsvFormatError, match="bars_path"):
        await CsvMarketDataAdapter().configure({}, make_ctx(tmp_path))


async def test_nonexistent_bars_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(CsvFormatError, match="does not exist"):
        await CsvMarketDataAdapter().configure(
            {"bars_path": str(tmp_path / "nope.csv")}, make_ctx(tmp_path)
        )


async def test_missing_columns_fail_the_whole_file(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path / "bars.csv", ["security", "date", "close"], [["AAA", "2024-01-02", "1"]]
    )
    adapter = CsvMarketDataAdapter()
    await adapter.configure({"bars_path": str(path)}, make_ctx(tmp_path))
    with pytest.raises(CsvFormatError, match="missing required column"):
        [b async for b in adapter.bars([], WINDOW)]


async def test_entity_column_aliases(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path / "bars.csv",
        ["ticker", "date", "open", "high", "low", "close", "volume"],
        [["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000"]],
    )
    adapter = CsvMarketDataAdapter()
    await adapter.configure({"bars_path": str(path)}, make_ctx(tmp_path))
    assert [b.native_entity async for b in adapter.bars([], WINDOW)] == ["AAA"]


async def test_capabilities_report_real_coverage(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    caps = adapter.capabilities()
    assert list(caps.entities or []) == ["AAA", "BBB"]
    assert caps.coverage_start is not None
    assert caps.coverage_start.isoformat() == "2024-01-02"
    assert caps.supports_restatement is False


async def test_bars_filter_by_ids(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    assert {b.native_entity async for b in adapter.bars(["BBB"], WINDOW)} == {"BBB"}


async def test_poll_picks_up_a_row_appended_later(tmp_path: Path) -> None:
    """The watermark tracks the highest stamp emitted, not the clock. Appending
    yesterday's bar today is the normal case for a daily file, and advancing to
    "now" would hide it permanently."""
    path = write_csv(tmp_path / "bars.csv", BARS_HEADER, [BARS[0]])
    adapter = CsvMarketDataAdapter()
    await adapter.configure({"bars_path": str(path)}, make_ctx(tmp_path))
    assert len([o async for o in adapter.poll()]) == 1
    write_csv(path, BARS_HEADER, [BARS[0], BARS[1]])
    fresh = [o.event_time.as_datetime().date().isoformat() async for o in adapter.poll()]
    assert fresh == ["2024-01-03"]
