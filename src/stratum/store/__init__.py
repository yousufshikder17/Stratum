"""The PIT signal store (spec §4.8): bitemporal, append-only, as_of-scoped."""

from stratum.store.duckdb_store import DuckDBSignalStore
from stratum.store.interface import SignalStore, SnapshotRef, StoreError, WriteReceipt

__all__ = ["DuckDBSignalStore", "SignalStore", "SnapshotRef", "StoreError", "WriteReceipt"]
