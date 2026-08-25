"""Read-only store view over a materialized snapshot (spec §2.3).

``DuckDBSignalStore.snapshot()`` writes ``observations.parquet`` plus a
``manifest.json`` carrying a content hash. :class:`SnapshotStore` opens such
a directory read-only and serves the same PIT ``read`` contract as the live
store — this makes "research reads only the immutable snapshot" structural:
backtests bind a snapshot hash and consume exactly those bytes.

The manifest layout is the shared sibling contract consumed by
``ledger.adapters.stratum``.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from stratum.guard.leakage import ValidatedObservation
from stratum.schema.observation import Observation
from stratum.schema.times import AsOf, EventTime
from stratum.store.duckdb_store import _COLUMNS, _observation_from_row
from stratum.store.interface import SignalStore, SnapshotRef, StoreError, WriteReceipt

__all__ = ["SnapshotStore"]


class SnapshotStore(SignalStore):
    """Read-only ``SignalStore`` over one immutable snapshot directory."""

    def __init__(self, path: Path, *, expected_content_hash: str | None = None) -> None:
        self._path = path
        manifest_path = path / "manifest.json"
        if not manifest_path.exists():
            raise StoreError(f"snapshot manifest not found: {manifest_path}")
        manifest = json.loads(manifest_path.read_text())
        self._content_hash: str | None = manifest.get("content_hash")
        if not self._content_hash:
            raise StoreError(f"snapshot manifest at {manifest_path} has no content_hash")
        if expected_content_hash is not None and expected_content_hash != self._content_hash:
            raise StoreError(
                f"snapshot content_hash {self._content_hash!r} does not match "
                f"expected_content_hash {expected_content_hash!r} — refusing to read "
                "a different dataset than the pinned hash"
            )
        parquet_path = path / "observations.parquet"
        if not parquet_path.exists():
            raise StoreError(f"snapshot parquet not found: {parquet_path}")
        self._relation = f"read_parquet('{parquet_path.as_posix()}')"
        self._as_of_ns: int | None = manifest.get("as_of_ns")
        self._conn: Any = None

    @property
    def content_hash(self) -> str:
        """The snapshot's content hash — belongs in backtest manifests."""
        assert self._content_hash is not None
        return self._content_hash

    def as_snapshot_ref(self) -> SnapshotRef:
        return SnapshotRef(
            content_hash=self.content_hash,
            as_of=AsOf(self._as_of_ns) if self._as_of_ns is not None else AsOf(0),
            path=self._path,
        )

    def _connect(self) -> Any:
        if self._conn is None:
            import duckdb

            self._conn = duckdb.connect()  # in-memory; reads the Parquet file
        return self._conn

    # -- rejected mutations ---------------------------------------------------

    def write(self, records: Iterable[ValidatedObservation], *, run_id: str) -> WriteReceipt:
        raise StoreError(
            "snapshots are immutable: write via the live DuckDBSignalStore, "
            "then materialize a new snapshot"
        )

    def snapshot(self, *, as_of: AsOf, target_dir: Path) -> SnapshotRef:
        raise StoreError("snapshots are immutable: already a fixed as_of")

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
        # Identical PIT selection rule as the live store: latest vintage per
        # key among rows knowable at as_of, total deterministic tiebreak.
        sql = (
            f"SELECT {_COLUMNS} FROM {self._relation} WHERE {' AND '.join(predicates)} "
            "QUALIFY row_number() OVER ("
            "  PARTITION BY coalesce(security_id, native_entity), event_time_ns"
            "  ORDER BY knowledge_time_ns DESC, vintage_id DESC, observation_id DESC"
            ") = 1 "
            "ORDER BY event_time_ns, coalesce(security_id, native_entity)"
        )
        for row in conn.execute(sql, params).fetchall():
            yield _observation_from_row(row)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
