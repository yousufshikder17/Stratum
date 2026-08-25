"""The PIT signal store (spec §4.8): bitemporal, append-only, as_of-scoped."""

from stratum.store.duckdb_store import DuckDBSignalStore
from stratum.store.interface import SignalStore, SnapshotRef, StoreError, WriteReceipt
from stratum.store.snapshot_store import SnapshotStore

__all__ = [
    "DuckDBSignalStore",
    "SignalStore",
    "SnapshotRef",
    "SnapshotStore",
    "StoreError",
    "WriteReceipt",
]
