"""Dataset snapshots (spec §2.3, §4.8).

A snapshot is a content-addressed, immutable materialization of the store at a
fixed ``as_of``: Parquet data plus a manifest of the source vintages used.
Snapshot upload is not implemented yet.
"""

from __future__ import annotations

from pathlib import Path

from stratum.schema.times import AsOf
from stratum.store.interface import SignalStore, SnapshotRef

__all__ = ["create_snapshot", "push_snapshot"]


def create_snapshot(store: SignalStore, *, as_of: AsOf, target_dir: Path) -> SnapshotRef:
    """Materialize + hash. Delegates to the store (spec §4.8)."""
    return store.snapshot(as_of=as_of, target_dir=target_dir)


def push_snapshot(ref: SnapshotRef, *, endpoint: str) -> None:
    """Upload is unavailable until transport and trust policy exist."""
    raise NotImplementedError("snapshot upload is not implemented")
