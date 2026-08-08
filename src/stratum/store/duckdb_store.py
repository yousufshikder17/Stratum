"""Embedded DuckDB + Parquet bitemporal store — researcher profile (spec §4.8).

The planned layout has vintage columns and a point-in-time index. Only schema
initialization is implemented; writes, reads, conflict handling, and snapshots
are not. ``duckdb`` is imported lazily.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from stratum.guard.leakage import ValidatedObservation
from stratum.schema.observation import Observation
from stratum.schema.times import AsOf, EventTime
from stratum.store.interface import SignalStore, SnapshotRef, WriteReceipt

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
    extensions        JSON,
    raw               BLOB
);
CREATE INDEX IF NOT EXISTS idx_obs_pit
    ON observations (security_id, signal_type, event_time_ns, knowledge_time_ns);
"""


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

    def write(self, records: Iterable[ValidatedObservation], *, run_id: str) -> WriteReceipt:
        raise NotImplementedError(
            "not implemented: idempotent append-only insert; a conflicting non-identical row "
            "for an existing observation_id, or an in-place update to an existing "
            "(entity, event_time, signal_type, vintage_id), raises StoreError"
        )

    def read(
        self,
        *,
        signal_type: str,
        as_of: AsOf,
        security_ids: Sequence[str] | None = None,
        event_start: EventTime | None = None,
        event_end: EventTime | None = None,
    ) -> Iterator[Observation]:
        raise NotImplementedError(
            "not implemented: SELECT latest vintage per (security_id, event_time) where "
            "knowledge_time_ns <= as_of.ns — the single PIT selection rule (spec §4.4)"
        )

    def snapshot(self, *, as_of: AsOf, target_dir: Path) -> SnapshotRef:
        raise NotImplementedError(
            "not implemented: materialize Parquet partitions at as_of + a manifest of source "
            "vintages; hash contents for the content-addressed SnapshotRef"
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
