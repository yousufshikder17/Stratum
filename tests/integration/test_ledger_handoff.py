"""Optional sibling check: Ledger consumes a real Stratum snapshot."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

from tests.conftest import make_manifest

from stratum.adapters.manifest import KnowledgeTimeBasis
from stratum.guard.leakage import IngestMode, LeakageGuard
from stratum.schema.data_class import DataClass
from stratum.schema.observation import Observation
from stratum.schema.payloads import MarketBar
from stratum.schema.times import AsOf, EventTime, KnowledgeTime
from stratum.store.duckdb_store import DuckDBSignalStore

LEDGER_REPO = Path(os.environ.get("LEDGER_REPO", Path(__file__).resolve().parents[3] / "Ledger"))
if not (LEDGER_REPO / "src" / "ledger").exists():
    pytest.skip("set LEDGER_REPO to run the sibling contract check", allow_module_level=True)
sys.path.insert(0, str(LEDGER_REPO / "src"))

from ledger.adapters.base import TimeWindow  # noqa: E402
from ledger.adapters.context import AdapterContext  # noqa: E402
from ledger.adapters.stratum import StratumAdapter  # noqa: E402


def test_ledger_consumes_real_stratum_snapshot(tmp_path: Path) -> None:
    event = datetime(2026, 1, 2, tzinfo=UTC)
    knowledge = datetime(2026, 1, 2, 21, tzinfo=UTC)
    observation = Observation(
        observation_id="01LEDGERHANDOFF0000000001",
        signal_type="market.bar",
        run_id="stratum-contract",
        source_id="fixture",
        adapter_id="fixture",
        security_id="SEC-TEST",
        native_entity="TEST",
        event_time=EventTime.at(event),
        knowledge_time=KnowledgeTime.at(knowledge),
        data_class=DataClass.PUBLIC_AGG,
        license_tag="fixture",
        payload=MarketBar(open=10.0, high=11.0, low=9.0, close=10.5, volume=1000.0),
    )
    validated = LeakageGuard(now=lambda: datetime(2026, 2, 1, tzinfo=UTC)).validate(
        observation,
        manifest=make_manifest(basis=KnowledgeTimeBasis.PUBLICATION),
        mode=IngestMode.BACKFILL,
    )
    store = DuckDBSignalStore(tmp_path / "stratum.duckdb")
    try:
        store.write([validated], run_id="stratum-contract")
        snapshot = store.snapshot(
            as_of=AsOf.at(datetime(2026, 2, 1, tzinfo=UTC)),
            target_dir=tmp_path / "snapshot",
        )
    finally:
        store.close()

    adapter = StratumAdapter()
    asyncio.run(
        adapter.configure(
            {
                "snapshot_dir": str(snapshot.path),
                "expected_snapshot_id": snapshot.content_hash,
            },
            AdapterContext(
                adapter_id="stratum",
                run_id="ledger-contract",
                data_dir=tmp_path,
                logger=logging.getLogger("test.ledger_handoff"),
            ),
        )
    )

    async def collect() -> list[object]:
        return [
            item
            async for item in adapter.backfill(
                TimeWindow(
                    start=datetime(2026, 1, 1, tzinfo=UTC),
                    end=datetime(2026, 1, 3, tzinfo=UTC),
                )
            )
        ]

    rows = asyncio.run(collect())
    asyncio.run(adapter.close())

    assert adapter.snapshot_id() == snapshot.content_hash
    assert len(rows) == 1
    row = rows[0]
    assert row.observation_id == observation.observation_id
    assert row.event_time.ns == observation.event_time.ns
    assert row.knowledge_time.ns == observation.knowledge_time.ns
