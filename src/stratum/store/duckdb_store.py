"""Embedded DuckDB + Parquet bitemporal store — researcher profile (spec §4.8).

Append-only with vintage columns, indexed by ``(security_id, signal_type,
series_id, event_time_ns, knowledge_time_ns)``. Columnar, crash-safe, fast ``as_of``
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
from datetime import UTC, date, datetime, timedelta
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
    series_id         TEXT NOT NULL DEFAULT '',
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
-- The append-only conflict key. Unindexed, the in-place-update check scans the
-- whole table once per incoming row, which makes a large backfill quadratic.
CREATE INDEX IF NOT EXISTS idx_obs_vintage_key
    ON observations (native_entity, signal_type, event_time_ns, vintage_id);
"""

OBSERVATIONS_MIGRATION_DDL = """
ALTER TABLE observations ADD COLUMN IF NOT EXISTS series_id TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_obs_pit_series
    ON observations (security_id, signal_type, series_id, event_time_ns, knowledge_time_ns);
CREATE INDEX IF NOT EXISTS idx_obs_vintage_series
    ON observations (native_entity, signal_type, series_id, event_time_ns, vintage_id);
"""

_COLUMNS = (
    "observation_id, schema_version, signal_type, series_id, run_id, source_id, adapter_id, "
    "security_id, native_entity, event_time_ns, knowledge_time_ns, ingest_time_ns, "
    "vintage_id, data_class, license_tag, derived_by, payload, raw"
)

#: Payload fields typed ``date`` — stored as ISO strings, rehydrated on read.
#: Staging table for a batched write. TEMP, so it is connection-local and
#: cannot be mistaken for durable state.
_STAGING = "_stratum_incoming"
_PLACEHOLDERS = ",".join("?" * 18)


def _same_content(left: tuple[Any, ...], right: tuple[Any, ...]) -> bool:
    """Do two staged rows carry the same payload and the same two clocks?"""
    return (left[16], left[9], left[10]) == (right[16], right[9], right[10])


_DATE_FIELDS = ("ex_date", "record_date", "pay_date", "last_trade_date", "period_end")
#: Payload fields typed ``timedelta`` — stored as seconds.
_TIMEDELTA_FIELDS = ("forecast_horizon",)


def _typed_payload_classes() -> dict[str, type[Payload]]:
    """signal_type -> payload class, discovered from the Payload hierarchy.

    ``GenericPayload`` is excluded by name rather than by identity: it is the
    fallback carrier for *unknown* types and declares no ``SIGNAL_TYPE``, so
    registering it would shadow whatever type it happens to be carrying.
    """
    return {
        cls.SIGNAL_TYPE: cls
        for cls in Payload.__subclasses__()
        if cls.SIGNAL_TYPE and cls.__name__ != GenericPayload.__name__
    }


def _encode_value(key: str, value: Any) -> Any:
    """JSON-safe form for one payload field.

    Dates and durations are the two non-primitive types the v1 payload
    vocabulary uses. They are converted explicitly rather than via a
    ``default=`` fallback, so an unexpected type raises at write time instead
    of being silently stringified into a value that will not round-trip.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    return value


def _payload_to_fields(payload: Payload) -> dict[str, Any]:
    """Flatten a payload to a JSON-serializable dict (store round-trip)."""
    if isinstance(payload, GenericPayload):
        return {
            "type_name": payload.type_name,
            "__generic__": {k: _encode_value(k, v) for k, v in payload.fields.items()},
        }
    if is_dataclass(payload):
        return {k: _encode_value(k, v) for k, v in asdict(payload).items()}
    raise StoreError(f"unsupported payload type {type(payload).__name__}")


def _payload_from_fields(signal_type: str, fields: Mapping[str, Any]) -> Payload:
    """Rehydrate a typed payload; unknown ``signal_type`` yields
    :class:`GenericPayload` — preserved, never dropped (spec §4.1)."""
    cls = _typed_payload_classes().get(signal_type)
    if cls is None:
        return GenericPayload(type_name=signal_type, fields=dict(fields.get("__generic__", {})))
    kwargs = dict(fields)
    kwargs.pop("__generic__", None)
    model = kwargs.get("model")
    if isinstance(model, dict):
        kwargs["model"] = ModelRef(**model)
    for key in _DATE_FIELDS:
        if isinstance(kwargs.get(key), str):
            kwargs[key] = date.fromisoformat(kwargs[key])
    for key in _TIMEDELTA_FIELDS:
        seconds = kwargs.get(key)
        if isinstance(seconds, (int, float)) and not isinstance(seconds, bool):
            kwargs[key] = timedelta(seconds=seconds)
    try:
        return cls(**kwargs)
    except TypeError:
        # The stored shape no longer matches the class (a payload gained or
        # lost a field). Preserve the row as generic rather than dropping it.
        return GenericPayload(type_name=signal_type, fields=dict(fields))


def _row_from_observation(o: Observation, ingest_ns: int) -> tuple[Any, ...]:
    return (
        o.observation_id,
        SCHEMA_VERSION,
        o.signal_type,
        o.series_id,
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
        str(o.derived_by) if o.derived_by else None,
        json.dumps(_payload_to_fields(o.payload), sort_keys=True),
        o.raw,
    )


def _observation_from_row(row: tuple[Any, ...]) -> Observation:
    return Observation(
        observation_id=row[0],
        signal_type=row[2],
        series_id=row[3],
        run_id=row[4],
        source_id=row[5],
        adapter_id=row[6],
        security_id=row[7],
        native_entity=row[8],
        event_time=EventTime(row[9]),
        knowledge_time=KnowledgeTime(row[10]),
        ingest_time=IngestTime(row[11]),
        vintage_id=row[12],
        data_class=DataClass(row[13]),
        license_tag=row[14],
        derived_by=ModelRef.parse(row[15]) if row[15] else None,
        payload=_payload_from_fields(row[2], json.loads(row[16])),
        raw=row[17],
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
            self._conn.execute(OBSERVATIONS_MIGRATION_DDL)
        return self._conn

    # -- write ----------------------------------------------------------------

    def write(self, records: Iterable[ValidatedObservation], *, run_id: str) -> WriteReceipt:
        """Append a batch in one transaction.

        The two invariants — idempotency by ``observation_id`` and no in-place
        update of a logical key — are checked set-wise against a staging table
        rather than row by row. A per-row loop issued two queries and an insert
        per observation, which is fine for a CSV fixture and hopeless for a
        multi-year backfill; the checks themselves are unchanged.

        Duplicates *within* the batch are caught here too, which the per-row
        path could not see: batching makes it possible to submit the same id
        twice before either reaches the table.
        """
        conn = self._connect()
        ingest_ns = IngestTime.at(datetime.now(UTC)).ns
        rows: list[tuple[Any, ...]] = []
        by_id: dict[str, tuple[Any, ...]] = {}
        by_key: dict[tuple[str, str, str, int, str], str] = {}
        skipped = 0

        for record in records:
            o = record.observation
            row = _row_from_observation(o, ingest_ns)
            seen = by_id.get(o.observation_id)
            if seen is not None:
                # Same id twice in one batch: identical content is a re-pull,
                # different content is corruption — same rule as across batches.
                if _same_content(seen, row):
                    skipped += 1
                    continue
                raise StoreError(
                    f"observation_id {o.observation_id!r} appears twice in one batch "
                    "with different content — ids are immutable (spec §3.2 rule 4)"
                )
            key = (o.native_entity, o.signal_type, o.series_id, o.event_time.ns, o.vintage_id)
            clashing = by_key.get(key)
            if clashing is not None:
                raise StoreError(
                    f"in-place update rejected: ({o.native_entity!r}, {o.signal_type!r}, "
                    f"series={o.series_id!r}, event_time={o.event_time.ns}, "
                    f"vintage={o.vintage_id!r}) appears "
                    f"twice in one batch ({clashing!r} and {o.observation_id!r}) — "
                    "restatements must be new vintages"
                )
            by_id[o.observation_id] = row
            by_key[key] = o.observation_id
            rows.append(row)

        if not rows:
            return WriteReceipt(rows_written=0, rows_skipped=skipped, run_id=run_id)

        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                f"CREATE OR REPLACE TEMP TABLE {_STAGING} AS SELECT * FROM observations WHERE false"
            )
            conn.executemany(
                f"INSERT INTO {_STAGING} ({_COLUMNS}) VALUES ({_PLACEHOLDERS})",
                [list(row) for row in rows],
            )

            # Already present: identical content is skipped, different content
            # under the same id is corruption.
            for observation_id, matches in conn.execute(
                f"""
                SELECT i.observation_id,
                       o.payload::VARCHAR = i.payload::VARCHAR
                       AND o.event_time_ns = i.event_time_ns
                       AND o.knowledge_time_ns = i.knowledge_time_ns
                FROM {_STAGING} i JOIN observations o USING (observation_id)
                """
            ).fetchall():
                if not matches:
                    raise StoreError(
                        f"observation_id {observation_id!r} exists with different "
                        "content — ids are immutable (spec §3.2 rule 4)"
                    )
                skipped += 1
            conn.execute(
                f"DELETE FROM {_STAGING} WHERE observation_id IN "
                "(SELECT observation_id FROM observations)"
            )

            # Append-only vintages: a second row for the same logical key must
            # carry a new vintage_id, never rewrite the existing one (§4.4 rule 2).
            clash = conn.execute(
                f"""
                SELECT i.native_entity, i.signal_type, i.series_id,
                       i.event_time_ns, i.vintage_id
                FROM {_STAGING} i JOIN observations o
                  ON o.native_entity = i.native_entity
                 AND o.signal_type = i.signal_type
                 AND o.series_id = i.series_id
                 AND o.event_time_ns = i.event_time_ns
                 AND o.vintage_id = i.vintage_id
                LIMIT 1
                """
            ).fetchone()
            if clash is not None:
                raise StoreError(
                    f"in-place update rejected: ({clash[0]!r}, {clash[1]!r}, "
                    f"series={clash[2]!r}, event_time={clash[3]}, "
                    f"vintage={clash[4]!r}) already "
                    "exists — restatements must be new vintages"
                )

            written = conn.execute(f"SELECT count(*) FROM {_STAGING}").fetchone()[0]
            conn.execute(f"INSERT INTO observations ({_COLUMNS}) SELECT {_COLUMNS} FROM {_STAGING}")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
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
        # (entity, series, event_time) among rows knowable at as_of. Ties on
        # knowledge_time break on vintage_id then observation_id — total,
        # deterministic order.
        sql = (
            f"SELECT {_COLUMNS} FROM observations WHERE {' AND '.join(predicates)} "
            "QUALIFY row_number() OVER ("
            "  PARTITION BY coalesce(security_id, native_entity), series_id, event_time_ns"
            "  ORDER BY knowledge_time_ns DESC, vintage_id DESC, observation_id DESC"
            ") = 1 "
            "ORDER BY event_time_ns, coalesce(security_id, native_entity), series_id"
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
            "series_id, event_time_ns, knowledge_time_ns, observation_id) "
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
