"""The bitemporal PIT signal store interface (spec §2.2 step 3, §4.8).

All components talk only via this interface (and the schema) — no
shared-memory assumptions — so the researcher-profile DuckDB store can swap to
a warehouse in the cloud profile without touching consumers (spec §2.3).

Type-level guarantees:

- ``write`` accepts only :class:`ValidatedObservation` — the leakage guard's
  proof type. There is no write path for unvalidated adapter output.
- ``read`` requires an :class:`AsOf` keyword with no default — there is no
  unscoped read. The store returns, per ``(security_id, event_time,
  signal_type)``, the latest vintage with ``knowledge_time <= as_of``;
  nothing with ``knowledge_time > as_of`` is ever visible (spec §4.4 rule 1).
- The store is append-only: restatements and backfills create new vintages,
  never overwrites (spec §4.4 rule 2).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from stratum.guard.leakage import ValidatedObservation
from stratum.schema.observation import Observation
from stratum.schema.times import AsOf, EventTime

__all__ = ["SignalStore", "SnapshotRef", "StoreError", "WriteReceipt"]


class StoreError(RuntimeError):
    """Raised for storage failures, including attempted in-place updates."""


@dataclass(frozen=True, kw_only=True, slots=True)
class WriteReceipt:
    rows_written: int
    run_id: str


@dataclass(frozen=True, kw_only=True, slots=True)
class SnapshotRef:
    """A content-addressed, immutable materialization of the store at a fixed
    ``as_of`` (spec §2.3, Appendix A). Backtests reference snapshots by hash
    for reproducibility."""

    content_hash: str
    as_of: AsOf
    path: Path | None = None


class SignalStore(ABC):
    """Bitemporal store: every value lives on both the event-time axis and
    the knowledge-time axis; queries are always ``as_of``-scoped."""

    @abstractmethod
    def write(self, records: Iterable[ValidatedObservation], *, run_id: str) -> WriteReceipt:
        """Append validated observations. Idempotent per observation_id;
        re-pulling a window must not duplicate (spec §3.2 rule 4). New
        information about an existing ``(entity, event_time)`` must arrive as
        a new vintage — an in-place update raises :class:`StoreError`."""

    @abstractmethod
    def read(
        self,
        *,
        signal_type: str,
        as_of: AsOf,
        security_ids: Sequence[str] | None = None,
        event_start: EventTime | None = None,
        event_end: EventTime | None = None,
    ) -> Iterator[Observation]:
        """The value of each signal as it was known on ``as_of``: the latest
        vintage with ``knowledge_time <= as_of`` per key. There is no overload
        without ``as_of``."""

    @abstractmethod
    def snapshot(self, *, as_of: AsOf, target_dir: Path) -> SnapshotRef:
        """Materialize a content-addressed snapshot (Parquet + a manifest of
        source vintages) for reproducible backtests and local->cloud handoff
        (spec §2.3)."""

    @abstractmethod
    def close(self) -> None: ...
