"""SEC EDGAR submissions and Company Facts adapter.

EDGAR's acceptance timestamp is the knowledge axis; report/fact period end is
the event axis. Accessions are immutable vintages, so amendments append rather
than overwrite earlier filings and facts.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from stratum.adapters.base import (
    CapabilityManifest,
    EntityHint,
    SignalValue,
    SourceAdapter,
    TimeWindow,
)
from stratum.adapters.context import AdapterContext
from stratum.adapters.manifest import load_manifest
from stratum.schema.data_class import DataClass
from stratum.schema.ids import deterministic_ulid
from stratum.schema.observation import Observation
from stratum.schema.payloads import FilingEvent, FundamentalFact, Payload
from stratum.schema.times import EventTime, KnowledgeTime

__all__ = ["SecEdgarAdapter", "SecEdgarError"]

_MANIFEST = load_manifest(Path(__file__).with_name("manifest.toml"))
_BASE_URL = "https://data.sec.gov"
_DEFAULT_FORMS = ("10-K", "10-Q", "8-K")
_DEFAULT_CONCEPTS = (
    "Assets",
    "Liabilities",
    "Revenues",
    "NetIncomeLoss",
    "StockholdersEquity",
)


class SecEdgarError(ValueError):
    """EDGAR configuration or response cannot be represented honestly."""


class SecEdgarAdapter(SourceAdapter):
    ADAPTER_ID = "sec_edgar"

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport
        self._ctx: AdapterContext | None = None
        self._client: httpx.AsyncClient | None = None
        self._ciks: tuple[str, ...] = ()
        self._forms: frozenset[str] = frozenset(_DEFAULT_FORMS)
        self._concepts: frozenset[str] = frozenset(_DEFAULT_CONCEPTS)
        self._poll_watermark_ns: int | None = None

    async def configure(self, config: dict[str, Any], ctx: AdapterContext) -> None:
        user_agent = str(config.get("user_agent", "")).strip()
        if "@" not in user_agent or len(user_agent) < 8:
            raise SecEdgarError(
                "sec_edgar user_agent must identify an application and contact email"
            )
        raw_ciks = config.get("ciks")
        if not isinstance(raw_ciks, list) or not raw_ciks:
            raise SecEdgarError("sec_edgar requires a non-empty ciks allowlist")
        self._ciks = tuple(dict.fromkeys(_normalize_cik(value) for value in raw_ciks))
        self._forms = _string_set(config.get("forms", _DEFAULT_FORMS), field="forms")
        self._concepts = _string_set(config.get("concepts", _DEFAULT_CONCEPTS), field="concepts")
        timeout = config.get("timeout_seconds", 30.0)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
            raise SecEdgarError("timeout_seconds must be positive")
        self._ctx = ctx
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=float(timeout),
            transport=self._transport,
        )

    async def backfill(self, window: TimeWindow) -> AsyncIterator[Observation]:
        async for observation in self._observations():
            if _in_window(observation.event_time, window):
                yield observation

    async def poll(self) -> AsyncIterator[Observation]:
        floor = self._poll_watermark_ns
        high_water = floor
        now_ns = KnowledgeTime.at(self._clock()).ns
        async for observation in self._observations():
            stamp = observation.knowledge_time.ns
            if stamp > now_ns or (floor is not None and stamp <= floor):
                continue
            high_water = stamp if high_water is None else max(high_water, stamp)
            yield observation
        self._poll_watermark_ns = high_water

    def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            signal_types=("fundamental.fact", "event.filing"),
            native_frequency="per-filing",
            supports_restatement=True,
            entities=self._ciks or None,
        )

    def entity_hint(self, raw_record: dict[str, Any]) -> list[EntityHint]:
        return [EntityHint(kind="cik", value=_normalize_cik(raw_record.get("cik")))]

    def to_signal(self, raw_record: dict[str, Any]) -> list[SignalValue]:
        kind = raw_record.get("kind")
        cik = _normalize_cik(raw_record.get("cik"))
        accession = _required_text(raw_record, "accession")
        knowledge_time = KnowledgeTime.at(_parse_datetime(raw_record.get("accepted")))
        event_time = EventTime.at(_day_start(_parse_date(raw_record.get("period_end"))))
        if kind == "filing":
            payload: Payload = FilingEvent(
                form_type=_required_text(raw_record, "form"),
                accession=accession,
                items=tuple(raw_record.get("items", ())),
                amends_ref=_optional_text(raw_record.get("amends_ref")),
            )
            signal_type = "event.filing"
        elif kind == "fact":
            payload = FundamentalFact(
                concept=_required_text(raw_record, "concept"),
                value=_number(raw_record.get("value"), field="value"),
                unit=_required_text(raw_record, "unit"),
                period_end=_parse_date(raw_record.get("period_end")),
                period_type="duration" if raw_record.get("period_start") else "instant",
            )
            signal_type = "fundamental.fact"
        else:
            raise SecEdgarError(f"unknown normalized EDGAR record kind {kind!r}")
        return [
            SignalValue(
                signal_type=signal_type,
                native_entity=cik,
                event_time=event_time,
                knowledge_time=knowledge_time,
                payload=payload,
                vintage_id=accession,
            )
        ]

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _observations(self) -> AsyncIterator[Observation]:
        for cik in self._ciks:
            filings = await self._filings(cik)
            for record in filings:
                yield self._observation(record)
            facts = await self._json(f"/api/xbrl/companyfacts/CIK{cik}.json")
            for record in _fact_records(cik, facts, filings, self._concepts):
                yield self._observation(record)

    async def _filings(self, cik: str) -> list[dict[str, Any]]:
        document = await self._json(f"/submissions/CIK{cik}.json")
        filings = document.get("filings")
        if not isinstance(filings, Mapping):
            raise SecEdgarError(f"CIK {cik}: submissions response has no filings object")
        records = list(_filing_records(cik, filings.get("recent"), self._forms))
        files = filings.get("files", [])
        if not isinstance(files, list):
            raise SecEdgarError(f"CIK {cik}: filings.files must be an array")
        for item in files:
            if not isinstance(item, Mapping) or not isinstance(item.get("name"), str):
                raise SecEdgarError(f"CIK {cik}: malformed historical submissions file")
            historical = await self._json(f"/submissions/{item['name']}")
            records.extend(_filing_records(cik, historical, self._forms))
        records.sort(key=lambda record: str(record["accepted"]))
        latest_base: dict[tuple[str, str], str] = {}
        for record in records:
            form = str(record["form"])
            base_form = form.removesuffix("/A")
            key = (base_form, str(record["period_end"]))
            if form.endswith("/A"):
                record["amends_ref"] = latest_base.get(key)
            else:
                latest_base[key] = str(record["accession"])
        return records

    async def _json(self, path: str) -> dict[str, Any]:
        if self._ctx is None or self._client is None:
            raise SecEdgarError("sec_edgar is not configured")
        await self._ctx.rate_limiter.acquire()
        try:
            response = await self._client.get(path)
            response.raise_for_status()
            document = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise SecEdgarError(f"EDGAR request failed for {path}: {exc}") from exc
        if not isinstance(document, dict):
            raise SecEdgarError(f"EDGAR response for {path} must be a JSON object")
        return document

    def _observation(self, raw_record: dict[str, Any]) -> Observation:
        signal = self.to_signal(raw_record)[0]
        if isinstance(signal.payload, FundamentalFact):
            suffix = (
                f"{signal.payload.concept}|{signal.payload.unit}|"
                f"{raw_record.get('period_start', '')}|{signal.payload.period_end}"
            )
        else:
            suffix = "filing"
        series_id = (
            suffix
            if isinstance(signal.payload, FundamentalFact)
            else str(raw_record["form"]).removesuffix("/A")
        )
        key = f"sec_edgar|{signal.native_entity}|{signal.vintage_id}|{suffix}"
        return Observation(
            observation_id=deterministic_ulid(
                timestamp_ms=signal.event_time.ns // 1_000_000, key=key
            ),
            signal_type=signal.signal_type,
            run_id=self._ctx.run_id if self._ctx else "unconfigured",
            source_id="sec_edgar",
            adapter_id=self.ADAPTER_ID,
            native_entity=signal.native_entity,
            series_id=series_id,
            event_time=signal.event_time,
            knowledge_time=signal.knowledge_time,
            vintage_id=signal.vintage_id,
            data_class=DataClass.PUBLIC_AGG,
            license_tag=_MANIFEST.license_tag,
            payload=signal.payload,
        )

    def _clock(self) -> datetime:
        return self._ctx.clock() if self._ctx else datetime.now(UTC)


def _filing_records(cik: str, columns: object, forms: frozenset[str]) -> Iterable[dict[str, Any]]:
    if not isinstance(columns, Mapping):
        raise SecEdgarError(f"CIK {cik}: filing history must be an object")
    required = ("accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "form")
    arrays: dict[str, list[Any]] = {}
    for name in required:
        value = columns.get(name)
        if not isinstance(value, list):
            raise SecEdgarError(f"CIK {cik}: filing history {name} must be an array")
        arrays[name] = value
    count = len(arrays["accessionNumber"])
    if any(len(values) != count for values in arrays.values()):
        raise SecEdgarError(f"CIK {cik}: filing history columns have different lengths")
    items = columns.get("items", [""] * count)
    if not isinstance(items, list) or len(items) != count:
        raise SecEdgarError(f"CIK {cik}: filing history items has the wrong length")
    for index in range(count):
        form = str(arrays["form"][index]).strip().upper()
        if form not in forms and not (form.endswith("/A") and form[:-2] in forms):
            continue
        filing_date = _parse_date(arrays["filingDate"][index])
        report = _optional_text(arrays["reportDate"][index])
        period_end = _parse_date(report) if report else filing_date
        yield {
            "kind": "filing",
            "cik": cik,
            "accession": _required_value(arrays["accessionNumber"][index], "accessionNumber"),
            "form": form,
            "period_end": period_end.isoformat(),
            "accepted": _parse_datetime(arrays["acceptanceDateTime"][index]).isoformat(),
            "items": tuple(part.strip() for part in str(items[index]).split(",") if part.strip()),
        }


def _fact_records(
    cik: str,
    document: Mapping[str, Any],
    filings: Iterable[Mapping[str, Any]],
    concepts: frozenset[str],
) -> Iterable[dict[str, Any]]:
    by_accession = {str(item["accession"]): item for item in filings}
    facts = document.get("facts")
    if not isinstance(facts, Mapping):
        raise SecEdgarError(f"CIK {cik}: Company Facts response has no facts object")
    seen: dict[tuple[str, str, str, str, str], float] = {}
    for taxonomy, taxonomy_facts in facts.items():
        if taxonomy not in {"us-gaap", "ifrs-full", "dei"} or not isinstance(
            taxonomy_facts, Mapping
        ):
            continue
        for concept, definition in taxonomy_facts.items():
            if concept not in concepts or not isinstance(definition, Mapping):
                continue
            units = definition.get("units")
            if not isinstance(units, Mapping):
                raise SecEdgarError(f"CIK {cik}: concept {concept} has no units object")
            for unit, values in units.items():
                if not isinstance(values, list):
                    raise SecEdgarError(f"CIK {cik}: {concept}/{unit} facts must be an array")
                for value in values:
                    if not isinstance(value, Mapping):
                        raise SecEdgarError(f"CIK {cik}: malformed {concept}/{unit} fact")
                    accession = str(value.get("accn", ""))
                    filing = by_accession.get(accession)
                    if filing is None:
                        continue
                    end = _parse_date(value.get("end")).isoformat()
                    start = _optional_text(value.get("start")) or ""
                    number = _number(value.get("val"), field=f"{concept}.val")
                    key = (accession, str(concept), str(unit), start, end)
                    previous = seen.get(key)
                    if previous is not None and previous != number:
                        raise SecEdgarError(f"CIK {cik}: conflicting values for {key}")
                    if previous is not None:
                        continue
                    seen[key] = number
                    yield {
                        "kind": "fact",
                        "cik": cik,
                        "accession": accession,
                        "accepted": filing["accepted"],
                        "period_end": end,
                        "period_start": start,
                        "concept": str(concept),
                        "value": number,
                        "unit": str(unit),
                    }


def _normalize_cik(value: object) -> str:
    digits = str(value or "").strip().removeprefix("CIK")
    if not digits.isdigit() or len(digits) > 10:
        raise SecEdgarError(f"invalid CIK {value!r}")
    return digits.zfill(10)


def _string_set(value: object, *, field: str) -> frozenset[str]:
    if not isinstance(value, (list, tuple)) or not value:
        raise SecEdgarError(f"{field} must be a non-empty array")
    items = frozenset(str(item).strip() for item in value if str(item).strip())
    if not items:
        raise SecEdgarError(f"{field} must contain non-empty strings")
    return items


def _parse_date(value: object) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise SecEdgarError(f"invalid EDGAR date {value!r}") from exc


def _parse_datetime(value: object) -> datetime:
    text = str(value or "").strip()
    try:
        stamp = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        try:
            stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SecEdgarError(f"invalid EDGAR acceptance timestamp {value!r}") from exc
    if stamp.tzinfo is None:
        raise SecEdgarError(f"EDGAR acceptance timestamp {value!r} has no timezone")
    return stamp.astimezone(UTC)


def _day_start(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def _in_window(event_time: EventTime, window: TimeWindow) -> bool:
    return EventTime.at(window.start).ns <= event_time.ns < EventTime.at(window.end).ns


def _number(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SecEdgarError(f"{field} must be numeric")
    return float(value)


def _required_text(record: Mapping[str, Any], field: str) -> str:
    return _required_value(record.get(field), field)


def _required_value(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SecEdgarError(f"EDGAR {field} is required")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
