"""Embedded DuckDB + Parquet bitemporal store — researcher profile (spec §4.8).

Append-only with vintage columns, indexed by ``(security_id, signal_type,
event_time_ns, knowledge_time_ns)``. Columnar, crash-safe, fast ``as_of``
slicing, shippable as a content-addressed snapshot.

The layout mirrors the sibling Ledger store deliberately: identical
``event_time_ns`` / ``knowledge_time_ns`` / ``ingest_time_ns`` / ``vintage_id``
columns (Stratum says ``signal_type`` where Ledger says ``data_type``) — the
shared contract that lets ``ledger.adapters.stratum`` read Stratum stores
directly.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from stratum.guard.leakage import ValidatedObservation
from stratum.schema.data_class import DataClass
from stratum.schema.observation import SCHEMA_VERSION, Observation
from stratum.schema.payloads import GenericPayload, ModelRef, Payload
from stratum.schema.times import AsOf, EventTime, IngestTime, KnowledgeTime
from stratum.store.interface import SignalStore, SnapshotRef, StoreError, WriteReceipt

__all__ = ["OBSERVATIONS_DDL", "DuckDBSignalStore"]

#: Bitemporal layout. ``ingest_time`` is stored for audit but carries no index
#: and appears in no selection predicate (spec §4.2).
OBSERVATIONS_DDL = """
CREATE TABLE IF NOT EXISTS observations (
    observation_id    TEXT PRIMARY KEY,      -- ULID; idempotency key
    schema_version    TEXT NOT NULL,
    signal_type       TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    source_id         TEXT NOT NULL,
    adapter_id        TEXT NOT NULL,
    security_id       TEXT,                  -- resolver output; NULL = unresolved
    native_entity     TEXT NOT NULL,
    event_time_ns     BIGINT NOT NULL,       -- alignment axis
    knowledge_time_ns BIGINT NOT NULL,       -- selection axis (PIT)
    ingest_time_ns    BIGINT NOT NULL,       -- audit only
    vintage_id        TEXT NOT NULL,
    data_class        TINYINT NOT NULL,
    license_tag       TEXT NOT NULL,
    derived_by        TEXT,                  -- "model_id@version" or NULL
    payload           JSON NOT NULL,
    raw               BLOB
);
CREATE INDEX IF NOT EXISTS idx_obs_pit
    ON observations (security_id, signal_type, event_time_ns, knowledge_time_ns);
"""

_COLUMNS = (
    "observation_id, schema_version, signal_type, run_id, source_id, adapter_id, "
    "security_id, native_entity, event_time_ns, knowledge_time_ns, ingest_time_ns, "
    "vintage_id, data_class, license_tag, derived_by, payload, raw"
)

_DATE_FIELDS = ("ex_date", "record_date", "pay_date", "last_trade_date")


def _typed_payload_classes() -> dict[str, type[Payload]]:
    """signal_type -> payload class, discovered from the Payload hierarchy."""
    return {
        cls.SIGNAL_TYPE: cls  # type: ignore[misc]
        for cls in Payload.__subclasses__()
        if getattr(cls, "SIGNAL_TYPE", "") and cls is not GenericPayload
    }


def _payload_to_fields(payload: Payload) -> dict[str, Any]:
    """Flatten a payload to a JSON-serializable dict (store round-trip)."""
    if isinstance(payload, GenericPayload):
        return {"type_name": payload.type_name, "__generic__": dict(payload.fields)}
    if is_dataclass(payload):
        return asdict(payload)
    raise StoreError(f"unsupported payload type {type(payload).__name__}")


def _payload_from_fields(signal_type: str, fields: Mapping[str, Any]) -> Payload:
    """Rehydrate a typed payload; unknown ``signal_type`` yields
    :class:`GenericPayload` — preserved, never dropped (spec §4.1)."""
    cls = _typed_payload_classes().get(signal_type)
    if cls is None:
        return GenericPayload(
            type_name=signal_type, fields=dict(fields.get("__generic__", {}))
        )
    kwargs = dict(fields)
    kwargs.pop("__generic__", None)
    model = kwargs.get("model")
    if isinstance(model, dict):
        kwargs["model"] = ModelRef(**model)
    for key in _DATE_FIELDS:
        if isinstance(kwargs.get(key), str):
            kwargs[key] = date.fromisoformat(kwargs[key])
    try:
        return cls(**kwargs)  # type: ignore[no-any-return]
    except TypeError:
        return GenericPayload(type_name=signal_type, fields=dict(fields))


def _row_from_observation(o: Observation, ingest_ns: int) -> tuple[Any, ...]:
    return (
        o.observation_id,
        SCHEMA_VERSION,
        o.signal_type,
        o.run_id,
        o.source_id,
        o.adapter_id,
        o.security_id,
        o.native_entity,
        o.event_time.ns,
        o.knowledge_time.ns,
        o.ingest_time.ns if o.ingest_time is not None else ingest_ns,
        o.vintage_id,
        int(o.data_class),
        o.license_tag,
        f"{o.derived_by.model_id}@{o.derived_by.version}" if o.derived_by else None,
        json.dumps(_payload_to_fields(o.payload), sort_keys=True),
        o.raw,
    )


def _observation_from_row(row: tuple[Any, ...]) -> Observation:
    return Observation(
        observation_id=row[0],
        signal_type=row[2],
        run_id=row[3],
        source_id=row[4],
        adapter_id=row[5],
        security_id=row[6],
        native_entity=row[7],
        event_time=EventTime(row[8]),
        knowledge_time=KnowledgeTime(row[9]),
        ingest_time=IngestTime(row[10]),
        vintage_id=row[11],
        data_class=DataClass(row[12]),
        license_tag=row[13],
        derived_by=ModelRef.parse(row[14]) if row[14] else None,
        payload=_payload_from_fields(row[2], json.loads(row[15])),
        raw=row[16],
    )


class DuckDBSignalStore(SignalStore):
    """Researcher-profile store. Cloud profiles swap this class behind the
    same ``SignalStore`` interface (spec §2.3)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: Any = None  # duckdb.DuckDBPyConnection, created lazily

    def _connect(self) -> Any:
        if self._conn is None:
            import duckdb

            self._conn = duckdb.connect(str(self._path))
            self._conn.execute(OBSERVATIONS_DDL)
        return self._conn

    # -- write ----------------------------------------------------------------

    def write(self, records: Iterable[ValidatedObservation], *, run_id: str) -> WriteReceipt:
        conn = self._connect()
        ingest_ns = IngestTime.at(datetime.now(UTC)).ns
        written = skipped = 0
        for record in records:
            o = record.observation
            existing = conn.execute(
                "SELECT payload, event_time_ns, knowledge_time_ns FROM observations "
                "WHERE observation_id = ?",
                [o.observation_id],
            ).fetchone()
            if existing is not None:
                # Idempotent re-pull: identical content is silently skipped;
                # different content under the same id is corruption.
                same = (
                    existing[0] == json.dumps(_payload_to_fields(o.payload), sort_keys=True)
                    and existing[1] == o.event_time.ns
                    and existing[2] == o.knowledge_time.ns
                )
                if not same:
                    raise StoreError(
                        f"observation_id {o.observation_id!r} exists with different "
                        "content — ids are immutable (spec §3.2 rule 4)"
                    )
                skipped += 1
                continue
            # Append-only vintages: a second row for the same logical key must
            # carry a new vintage_id, never rewrite the existing one (§4.4 rule 2).
            clash = conn.execute(
                "SELECT observation_id FROM observations WHERE native_entity = ? "
                "AND signal_type = ? AND event_time_ns = ? AND vintage_id = ?",
                [o.native_entity, o.signal_type, o.event_time.ns, o.vintage_id],
            ).fetchone()
            if clash is not None:
                raise StoreError(
                    f"in-place update rejected: ({o.native_entity!r}, {o.signal_type!r}, "
                    f"event_time={o.event_time.ns}, vintage={o.vintage_id!r}) already "
                    "exists — restatements must be new vintages"
                )
            conn.execute(
                f"INSERT INTO observations ({_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                list(_row_from_observation(o, ingest_ns)),
            )
            written += 1
        return WriteReceipt(rows_written=written, rows_skipped=skipped, run_id=run_id)

    # -- read -----------------------------------------------------------------

    def read(
        self,
        *,
        signal_type: str,
        as_of: AsOf,
        security_ids: Sequence[str] | None = None,
        event_start: EventTime | None = None,
        event_end: EventTime | None = None,
    ) -> Iterator[Observation]:
        conn = self._connect()
        predicates = ["signal_type = ?", "knowledge_time_ns <= ?"]
        params: list[Any] = [signal_type, as_of.ns]
        if security_ids is not None:
            placeholders = ",".join("?" for _ in security_ids)
            predicates.append(f"security_id IN ({placeholders})")
            params.extend(security_ids)
        if event_start is not None:
            predicates.append("event_time_ns >= ?")
            params.append(event_start.ns)
        if event_end is not None:
            predicates.append("event_time_ns < ?")
            params.append(event_end.ns)
        # The single PIT selection rule (spec §4.4 rule 1): latest vintage per
        # (entity, event_time) among rows knowable at as_of. Ties on
        # knowledge_time break on vintage_id then observation_id — total,
        # deterministic order.
        sql = (
            f"SELECT {_COLUMNS} FROM observations WHERE {' AND '.join(predicates)} "
            "QUALIFY row_number() OVER ("
            "  PARTITION BY coalesce(security_id, native_entity), event_time_ns"
            "  ORDER BY knowledge_time_ns DESC, vintage_id DESC, observation_id DESC"
            ") = 1 "
            "ORDER BY event_time_ns, coalesce(security_id, native_entity)"
        )
        for row in conn.execute(sql, params).fetchall():
            yield _observation_from_row(row)

    # -- snapshot -------------------------------------------------------------

    def snapshot(self, *, as_of: AsOf, target_dir: Path) -> SnapshotRef:
        """Materialize Parquet at ``as_of`` + a manifest; hash the contents
        for the content-addressed ref (spec §2.3, §4.8). The manifest shape
        (``manifest.json`` with a ``content_hash`` key) is the shared
        sibling contract consumed by ``ledger.adapters.stratum``."""
        conn = self._connect()
        target_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = target_dir / "observations.parquet"
        conn.execute(
            "COPY (SELECT * FROM observations WHERE knowledge_time_ns <= ? "
            "ORDER BY signal_type, coalesce(security_id, native_entity), "
            "event_time_ns, knowledge_time_ns, observation_id) "
            f"TO '{parquet_path.as_posix()}' (FORMAT PARQUET)",
            [as_of.ns],
        )
        row_count = conn.execute(
            "SELECT count(*) FROM observations WHERE knowledge_time_ns <= ?", [as_of.ns]
        ).fetchone()[0]
        content_hash = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
        manifest = {
            "content_hash": content_hash,
            "as_of_ns": as_of.ns,
            "row_count": row_count,
            "schema": "stratum.observations.v1",
        }
        (target_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
        return SnapshotRef(content_hash=content_hash, as_of=as_of, path=target_dir)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
