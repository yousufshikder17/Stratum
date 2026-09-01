"""SEC EDGAR adapter: acceptance-time PIT mapping and normalized facts."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path

import httpx
import pytest

from stratum.adapters.base import TimeWindow
from stratum.adapters.context import AdapterContext
from stratum.adapters.sec_edgar import SecEdgarAdapter, SecEdgarError
from stratum.config import ResearchConfig
from stratum.guard.leakage import IngestMode
from stratum.run.ingest import ingest
from stratum.schema.observation import Observation
from stratum.schema.payloads import FilingEvent, FundamentalFact
from stratum.schema.times import AsOf
from stratum.store.duckdb_store import DuckDBSignalStore
from tests.conftest import write_csv

CIK = "0000320193"

SUBMISSIONS = {
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-24-000001", "0000320193-24-000002", "skip"],
            "filingDate": ["2024-02-01", "2024-02-10", "2024-02-11"],
            "reportDate": ["2023-12-31", "2023-12-31", "2024-02-11"],
            "acceptanceDateTime": [
                "2024-02-01T16:30:00Z",
                "2024-02-10T17:00:00Z",
                "2024-02-11T12:00:00Z",
            ],
            "form": ["10-K", "10-K/A", "4"],
            "items": ["", "", ""],
        },
        "files": [{"name": "CIK0000320193-submissions-001.json"}],
    }
}

HISTORICAL = {
    "accessionNumber": ["0000320193-23-000003"],
    "filingDate": ["2023-08-01"],
    "reportDate": ["2023-06-30"],
    "acceptanceDateTime": ["20230801163000"],
    "form": ["10-Q"],
    "items": [""],
}

COMPANY_FACTS = {
    "facts": {
        "us-gaap": {
            "Assets": {
                "units": {
                    "USD": [
                        {
                            "accn": "0000320193-24-000001",
                            "end": "2023-12-31",
                            "val": 100,
                            "form": "10-K",
                        },
                        {
                            "accn": "0000320193-24-000002",
                            "end": "2023-12-31",
                            "val": 110,
                            "form": "10-K/A",
                        },
                    ]
                }
            },
            "Revenues": {
                "units": {
                    "USD": [
                        {
                            "accn": "0000320193-23-000003",
                            "start": "2023-04-01",
                            "end": "2023-06-30",
                            "val": 25,
                            "form": "10-Q",
                        }
                    ]
                }
            },
            "NotConfigured": {
                "units": {
                    "USD": [
                        {
                            "accn": "0000320193-24-000001",
                            "end": "2023-12-31",
                            "val": 999,
                        }
                    ]
                }
            },
        }
    }
}


class _Sink:
    async def submit(self, observation: Observation) -> None:
        return None


class _Limiter:
    def __init__(self) -> None:
        self.calls = 0

    async def acquire(self, cost: int = 1) -> None:
        self.calls += cost


class _Secrets:
    def get(self, key: str) -> str | None:
        return None


def _transport(request: httpx.Request) -> httpx.Response:
    documents = {
        f"/submissions/CIK{CIK}.json": SUBMISSIONS,
        "/submissions/CIK0000320193-submissions-001.json": HISTORICAL,
        f"/api/xbrl/companyfacts/CIK{CIK}.json": COMPANY_FACTS,
    }
    document = documents.get(request.url.path)
    return httpx.Response(200, json=document) if document else httpx.Response(404)


async def _configured(tmp_path: Path) -> tuple[SecEdgarAdapter, _Limiter]:
    limiter = _Limiter()
    context = AdapterContext(
        logger=logging.getLogger("test.sec"),
        clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        data_dir=tmp_path,
        secrets=_Secrets(),
        rate_limiter=limiter,
        sink=_Sink(),
        run_id="sec-fixture",
    )
    adapter = SecEdgarAdapter(transport=httpx.MockTransport(_transport))
    await adapter.configure({"user_agent": "Stratum test@example.com", "ciks": ["320193"]}, context)
    return adapter, limiter


async def test_backfill_maps_acceptance_vintages_amendments_and_facts(tmp_path: Path) -> None:
    adapter, limiter = await _configured(tmp_path)
    window = TimeWindow(
        start=datetime(2023, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 1, tzinfo=UTC),
    )
    observations = [item async for item in adapter.backfill(window)]
    await adapter.close()

    filings = [item for item in observations if isinstance(item.payload, FilingEvent)]
    facts = [item for item in observations if isinstance(item.payload, FundamentalFact)]
    assert len(filings) == 3
    assert len(facts) == 3
    amendment = next(item for item in filings if item.payload.form_type == "10-K/A")
    assert amendment.payload.amends_ref == "0000320193-24-000001"
    assert amendment.knowledge_time.as_datetime() == datetime(2024, 2, 10, 17, tzinfo=UTC)
    assert amendment.vintage_id == amendment.payload.accession
    assert {fact.payload.value for fact in facts if fact.payload.concept == "Assets"} == {
        100.0,
        110.0,
    }
    assert all(item.native_entity == CIK for item in observations)
    assert limiter.calls == 3


async def test_poll_uses_acceptance_watermark(tmp_path: Path) -> None:
    adapter, _ = await _configured(tmp_path)
    first = [item async for item in adapter.poll()]
    second = [item async for item in adapter.poll()]
    await adapter.close()

    assert len(first) == 6
    assert second == []


async def test_configuration_requires_contact_and_bounded_ciks(tmp_path: Path) -> None:
    adapter = SecEdgarAdapter(transport=httpx.MockTransport(_transport))
    context = AdapterContext(
        logger=logging.getLogger("test.sec"),
        clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        data_dir=tmp_path,
        secrets=_Secrets(),
        rate_limiter=_Limiter(),
        sink=_Sink(),
        run_id="sec-fixture",
    )
    with pytest.raises(SecEdgarError, match="contact email"):
        await adapter.configure({"user_agent": "anonymous", "ciks": [CIK]}, context)
    with pytest.raises(SecEdgarError, match="ciks allowlist"):
        await adapter.configure({"user_agent": "Stratum test@example.com"}, context)


async def test_provider_failure_has_endpoint_context(tmp_path: Path) -> None:
    def unavailable(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    adapter = SecEdgarAdapter(transport=httpx.MockTransport(unavailable))
    context = AdapterContext(
        logger=logging.getLogger("test.sec"),
        clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        data_dir=tmp_path,
        secrets=_Secrets(),
        rate_limiter=_Limiter(),
        sink=_Sink(),
        run_id="sec-fixture",
    )
    await adapter.configure({"user_agent": "Stratum test@example.com", "ciks": [CIK]}, context)
    with pytest.raises(SecEdgarError, match=f"submissions/CIK{CIK}"):
        _ = [item async for item in adapter.poll()]
    await adapter.close()


async def test_guarded_ingest_resolves_and_persists_sec_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entities = write_csv(
        tmp_path / "entities.csv",
        ["kind", "value", "security_id", "valid_from", "known_from", "confidence"],
        [["cik", CIK, "SEC-AAPL", "1980-12-12", "1980-12-12", "1.0"]],
    )
    config = ResearchConfig.parse(
        {
            "store": {"path": "sec.duckdb"},
            "resolver": {"tables": entities.name, "version": "sec-test-v1"},
            "backfill": {"start": "2023-01-01", "end": "2025-01-01"},
            "rate_limits": {"sec_edgar": {"requests_per_second": 8.0, "burst": 8}},
            "adapters": [
                {
                    "id": "sec_edgar",
                    "entity_kind": "cik",
                    "config": {
                        "user_agent": "Stratum test@example.com",
                        "ciks": [CIK],
                    },
                }
            ],
        },
        source=tmp_path / "research.toml",
    )
    monkeypatch.setattr(
        import_module("stratum.run.ingest"),
        "load_adapter",
        lambda _: lambda: SecEdgarAdapter(transport=httpx.MockTransport(_transport)),
    )

    report = await ingest(
        config,
        mode=IngestMode.BACKFILL,
        clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
    )
    store = DuckDBSignalStore(config.store_path)
    try:
        facts = list(
            store.read(
                signal_type="fundamental.fact",
                as_of=AsOf.at(datetime(2025, 1, 1, tzinfo=UTC)),
            )
        )
    finally:
        store.close()

    assert report.clean
    assert report.rows_written == 6
    assert report.results[0].resolved == 6
    assert {item.security_id for item in facts} == {"SEC-AAPL"}
    assert {item.vintage_id for item in facts if item.payload.concept == "Assets"} == {
        "0000320193-24-000002"
    }
