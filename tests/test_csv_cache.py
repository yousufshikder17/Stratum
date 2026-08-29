"""The CSV parse cache — and, more importantly, when it must *not* hold.

Caching a parsed file is an obvious win: `backfill()` and `capabilities()`
each walk the same rows. The risk is that it silently breaks `poll()`, whose
whole job is to notice rows appended since the last pass. These tests pin the
invalidation, not the speedup.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from stratum.adapters.context import AdapterContext
from stratum.adapters.csv_common import CsvCache, CsvFormatError
from stratum.adapters.market_csv import CsvMarketDataAdapter
from stratum.schema.observation import Observation
from tests.conftest import write_csv

BARS_HEADER = ["security", "date", "open", "high", "low", "close", "volume"]
ROW_A = ["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000"]
ROW_B = ["AAA", "2024-01-03", "10.2", "10.9", "10.1", "10.7", "1200"]


class _Sink:
    async def submit(self, observation: Observation) -> None:
        return None


class _NoLimit:
    async def acquire(self, cost: int = 1) -> None:
        return None


class _NoSecrets:
    def get(self, key: str) -> str | None:
        return None


def make_ctx(tmp_path: Path, now: datetime) -> AdapterContext:
    return AdapterContext(
        logger=logging.getLogger("test"),
        clock=lambda: now,
        data_dir=tmp_path,
        secrets=_NoSecrets(),
        rate_limiter=_NoLimit(),
        sink=_Sink(),
        run_id="run-cache",
    )


# -- the cache itself ---------------------------------------------------------


def test_parses_once_for_an_unchanged_file(tmp_path: Path) -> None:
    path = write_csv(tmp_path / "f.csv", ["a"], [["1"]])
    calls: list[Path] = []
    cache: CsvCache[int] = CsvCache(lambda p: (calls.append(p), len(calls))[1])
    assert cache.get(path) == 1
    assert cache.get(path) == 1
    assert cache.get(path) == 1
    assert len(calls) == 1


def test_reparses_when_the_file_changes(tmp_path: Path) -> None:
    """Keyed on (mtime, size). An appended file must re-parse, or `poll()`
    would read the file once at startup and never see another row."""
    path = write_csv(tmp_path / "f.csv", ["a"], [["1"]])
    calls: list[Path] = []
    cache: CsvCache[int] = CsvCache(lambda p: (calls.append(p), len(calls))[1])
    assert cache.get(path) == 1
    write_csv(path, ["a"], [["1"], ["2"]])
    assert cache.get(path) == 2
    assert len(calls) == 2


def test_invalidate_forces_a_reparse(tmp_path: Path) -> None:
    path = write_csv(tmp_path / "f.csv", ["a"], [["1"]])
    calls: list[Path] = []
    cache: CsvCache[int] = CsvCache(lambda p: (calls.append(p), len(calls))[1])
    cache.get(path)
    cache.invalidate()
    cache.get(path)
    assert len(calls) == 2


def test_missing_file_still_raises(tmp_path: Path) -> None:
    cache: CsvCache[int] = CsvCache(lambda p: 0)
    with pytest.raises(CsvFormatError, match="not found"):
        cache.get(tmp_path / "nope.csv")


# -- the property that matters ------------------------------------------------


async def test_poll_still_sees_appended_rows(tmp_path: Path) -> None:
    """The regression the cache could have introduced, asserted end to end."""
    path = write_csv(tmp_path / "bars.csv", BARS_HEADER, [ROW_A])
    adapter = CsvMarketDataAdapter()
    await adapter.configure(
        {"bars_path": str(path)}, make_ctx(tmp_path, datetime(2026, 7, 1, tzinfo=UTC))
    )

    first = [o.event_time.as_datetime().date().isoformat() async for o in adapter.poll()]
    assert first == ["2024-01-02"]

    write_csv(path, BARS_HEADER, [ROW_A, ROW_B])
    second = [o.event_time.as_datetime().date().isoformat() async for o in adapter.poll()]
    assert second == ["2024-01-03"], "an appended row must be picked up, not cached away"


async def test_repeated_backfills_agree(tmp_path: Path) -> None:
    path = write_csv(tmp_path / "bars.csv", BARS_HEADER, [ROW_A, ROW_B])
    adapter = CsvMarketDataAdapter()
    await adapter.configure(
        {"bars_path": str(path)}, make_ctx(tmp_path, datetime(2026, 7, 1, tzinfo=UTC))
    )
    from stratum.adapters.base import TimeWindow

    window = TimeWindow(
        start=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 2, 1, tzinfo=UTC)
    )
    first = [o.observation_id async for o in adapter.backfill(window)]
    second = [o.observation_id async for o in adapter.backfill(window)]
    assert first == second


async def test_capabilities_reflect_an_edited_file(tmp_path: Path) -> None:
    path = write_csv(tmp_path / "bars.csv", BARS_HEADER, [ROW_A])
    adapter = CsvMarketDataAdapter()
    await adapter.configure(
        {"bars_path": str(path)}, make_ctx(tmp_path, datetime(2026, 7, 1, tzinfo=UTC))
    )
    assert list(adapter.capabilities().entities or []) == ["AAA"]
    write_csv(path, BARS_HEADER, [ROW_A, ["BBB", "2024-01-02", "5", "6", "4", "5", "10"]])
    assert list(adapter.capabilities().entities or []) == ["AAA", "BBB"]
