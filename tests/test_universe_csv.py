"""Point-in-time universe (spec §6.3): the survivorship-correct membership set."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from stratum.adapters.base import TimeWindow, UniverseSpec
from stratum.adapters.context import AdapterContext
from stratum.adapters.csv_common import CsvFormatError
from stratum.adapters.universe_csv import CsvUniverseAdapter, MembershipInterval
from stratum.schema.observation import Observation
from stratum.schema.payloads import MetaCoverage
from tests.conftest import write_csv

HEADER = ["security", "added_on", "announced_on", "removed_on", "removed_announced_on"]

#: AAA is a long-standing member; BBB joins mid-window with a week's notice;
#: ZZZ goes bankrupt and is removed — the name a "current constituents" file
#: would silently erase along with its loss.
MEMBERSHIP = [
    ["AAA", "2020-01-02", "2019-12-20", "", ""],
    ["BBB", "2024-02-01", "2024-01-25", "", ""],
    ["ZZZ", "2019-06-03", "2019-05-28", "2024-02-16", "2024-02-09"],
]

UNIVERSE = UniverseSpec(id="sp1500_pit")


class _RefusingSink:
    async def submit(self, observation: Observation) -> None:
        raise AssertionError("universe lookups must not write")


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
        sink=_RefusingSink(),
        run_id="run-test",
    )


async def configured(
    tmp_path: Path, rows: list[list[str]] | None = None, **overrides: object
) -> CsvUniverseAdapter:
    path = write_csv(tmp_path / "membership.csv", HEADER, rows if rows is not None else MEMBERSHIP)
    config: dict[str, object] = {"membership_path": str(path), "universe_id": "sp1500_pit"}
    config.update(overrides)
    adapter = CsvUniverseAdapter()
    await adapter.configure(config, make_ctx(tmp_path))
    return adapter


# -- the survivorship guarantee ----------------------------------------------


async def test_delisted_name_is_still_a_member_before_it_dies(tmp_path: Path) -> None:
    """The whole point of a PIT universe: on 2024-02-01 ZZZ was in the index,
    and a backtest must hold it — and take its loss — rather than pretend the
    index only ever contained survivors."""
    adapter = await configured(tmp_path)
    assert await adapter.members_as_of(date(2024, 2, 1), UNIVERSE) == ["AAA", "BBB", "ZZZ"]


async def test_removed_name_is_gone_afterwards(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    assert await adapter.members_as_of(date(2024, 2, 20), UNIVERSE) == ["AAA", "BBB"]


async def test_removal_is_half_open_on_its_effective_date(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    assert "ZZZ" in await adapter.members_as_of(date(2024, 2, 15), UNIVERSE)
    assert "ZZZ" not in await adapter.members_as_of(date(2024, 2, 16), UNIVERSE)


# -- the knowledge axis -------------------------------------------------------


async def test_addition_is_invisible_before_it_is_announced(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    assert "BBB" not in await adapter.members_as_of(date(2024, 1, 24), UNIVERSE)


async def test_announced_but_not_yet_effective_is_not_a_member(tmp_path: Path) -> None:
    """Knowing a name joins next week does not put it in this week's index."""
    adapter = await configured(tmp_path)
    assert "BBB" not in await adapter.members_as_of(date(2024, 1, 26), UNIVERSE)
    assert "BBB" in await adapter.members_as_of(date(2024, 2, 1), UNIVERSE)


async def test_unannounced_removal_does_not_remove(tmp_path: Path) -> None:
    """The surprising-but-correct case: a removal that took effect but has not
    been published yet cannot change what anyone knew at the time."""
    rows = [["ZZZ", "2019-06-03", "2019-05-28", "2024-02-16", "2024-03-15"]]
    adapter = await configured(tmp_path, rows)
    assert await adapter.members_as_of(date(2024, 2, 20), UNIVERSE) == ["ZZZ"]
    assert await adapter.members_as_of(date(2024, 3, 20), UNIVERSE) == []


# -- format rejections --------------------------------------------------------


async def test_flat_constituent_list_is_rejected(tmp_path: Path) -> None:
    """A current-constituents file is the classic survivorship bug. It is
    refused with an actionable message, not accepted with a warning."""
    path = write_csv(tmp_path / "flat.csv", ["security"], [["AAA"], ["BBB"]])
    adapter = CsvUniverseAdapter()
    await adapter.configure(
        {"membership_path": str(path), "universe_id": "sp1500_pit"}, make_ctx(tmp_path)
    )
    with pytest.raises(CsvFormatError, match="survivorship bias"):
        await adapter.members_as_of(date(2024, 2, 1), UNIVERSE)


async def test_removal_without_its_own_announcement_is_rejected(tmp_path: Path) -> None:
    """Letting a removal inherit the addition's stamp would back-date
    knowledge of the removal by years."""
    rows = [["ZZZ", "2019-06-03", "2019-05-28", "2024-02-16", ""]]
    adapter = await configured(tmp_path, rows)
    with pytest.raises(CsvFormatError, match="removed_announced_on"):
        await adapter.members_as_of(date(2024, 2, 1), UNIVERSE)


async def test_empty_interval_is_rejected(tmp_path: Path) -> None:
    rows = [["AAA", "2024-02-16", "2024-01-01", "2024-02-16", "2024-02-01"]]
    adapter = await configured(tmp_path, rows)
    with pytest.raises(CsvFormatError, match="empty membership interval"):
        await adapter.members_as_of(date(2024, 2, 1), UNIVERSE)


async def test_wrong_universe_id_is_rejected(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    with pytest.raises(CsvFormatError, match="serves universe"):
        await adapter.members_as_of(date(2024, 2, 1), UniverseSpec(id="russell_pit"))


async def test_universe_id_is_required(tmp_path: Path) -> None:
    path = write_csv(tmp_path / "membership.csv", HEADER, MEMBERSHIP)
    with pytest.raises(CsvFormatError, match="universe_id"):
        await CsvUniverseAdapter().configure({"membership_path": str(path)}, make_ctx(tmp_path))


# -- observations -------------------------------------------------------------


async def test_each_edge_carries_its_own_announcement(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    window = TimeWindow(
        start=datetime(2019, 1, 1, tzinfo=UTC), end=datetime(2025, 1, 1, tzinfo=UTC)
    )
    observations = {
        (o.native_entity, str(o.payload.note).rsplit(":", 1)[-1]): o  # type: ignore[union-attr]
        async for o in adapter.backfill(window)
    }
    added = observations[("ZZZ", "added")]
    removed = observations[("ZZZ", "removed")]
    assert added.knowledge_time.as_datetime() == datetime(2019, 5, 28, tzinfo=UTC)
    # The removal is knowable in 2024, not in 2019 when the name joined.
    assert removed.knowledge_time.as_datetime() == datetime(2024, 2, 9, tzinfo=UTC)
    assert removed.event_time.as_datetime() == datetime(2024, 2, 16, tzinfo=UTC)


async def test_every_row_is_vintage_keyed(tmp_path: Path) -> None:
    """The manifest declares supports_restatement, so the guard requires a
    vintage on every row; the announcement date is it."""
    adapter = await configured(tmp_path)
    window = TimeWindow(
        start=datetime(2019, 1, 1, tzinfo=UTC), end=datetime(2025, 1, 1, tzinfo=UTC)
    )
    observations = [o async for o in adapter.backfill(window)]
    assert observations
    assert all(o.vintage_id for o in observations)
    assert all(isinstance(o.payload, MetaCoverage) for o in observations)


async def test_backfill_filters_on_the_event_axis(tmp_path: Path) -> None:
    adapter = await configured(tmp_path)
    window = TimeWindow(
        start=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 3, 1, tzinfo=UTC)
    )
    entities = {o.native_entity async for o in adapter.backfill(window)}
    assert entities == {"BBB", "ZZZ"}  # AAA joined in 2020


async def test_removal_is_not_reported_as_a_coverage_gap(tmp_path: Path) -> None:
    """A name leaving the index is not the source going down. Conflating them
    would fabricate delistings out of outages."""
    adapter = await configured(tmp_path)
    window = TimeWindow(
        start=datetime(2019, 1, 1, tzinfo=UTC), end=datetime(2025, 1, 1, tzinfo=UTC)
    )
    payloads = [o.payload async for o in adapter.backfill(window)]
    assert all(isinstance(p, MetaCoverage) and p.source_up and not p.gap_marker for p in payloads)


# -- the interval type --------------------------------------------------------


def test_interval_requires_a_removal_stamp() -> None:
    with pytest.raises(CsvFormatError, match="removed_announced_on"):
        MembershipInterval(
            security="ZZZ",
            added_on=date(2019, 6, 3),
            announced_on=date(2019, 5, 28),
            removed_on=date(2024, 2, 16),
        )


def test_interval_membership_reads_both_axes() -> None:
    interval = MembershipInterval(
        security="ZZZ",
        added_on=date(2019, 6, 3),
        announced_on=date(2019, 5, 28),
        removed_on=date(2024, 2, 16),
        removed_announced_on=date(2024, 2, 9),
    )
    assert not interval.member_at(date(2019, 5, 30))  # announced, not effective
    assert interval.member_at(date(2020, 1, 1))
    assert interval.member_at(date(2024, 2, 15))
    assert not interval.member_at(date(2024, 2, 16))
