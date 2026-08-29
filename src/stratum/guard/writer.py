"""The guarded write path.

``GuardedWriter`` is the core's only implementation of the
:class:`~stratum.adapters.context.ObservationSink` protocol handed to adapters
via ``AdapterContext.sink``. It stamps the core-owned ``ingest_time``,
attaches canonical identity, runs the leakage guard, and forwards the
resulting :class:`ValidatedObservation` to ``SignalStore.write`` — adapters
physically cannot skip the guard because the store's write signature does not
accept raw observations.

Identity resolution lives here rather than in adapters on purpose (spec §3.2
rule 2): **the adapter owns provenance, the core owns identity.** An adapter
reports the raw key it saw; the core decides what that key referred to at the
moment the value became knowable. Resolving inside the core also means every
adapter — including third-party ones — gets point-in-time-correct identity
without implementing it, and cannot quietly opt out of it.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from stratum.adapters.manifest import AdapterManifest
from stratum.guard.leakage import IngestMode, LeakageGuard, ValidatedObservation
from stratum.schema.observation import Observation
from stratum.schema.times import AsOf, IngestTime

if TYPE_CHECKING:
    from stratum.resolver.entity import EntityResolver
    from stratum.store.interface import SignalStore

__all__ = ["DEFAULT_BATCH_SIZE", "GuardedWriter"]

#: Rows buffered before a flush. Large enough that a multi-year backfill is not
#: one transaction per observation, small enough that a crash loses little and
#: memory stays flat.
DEFAULT_BATCH_SIZE = 1000


class GuardedWriter:
    """Resolve -> stamp -> guard -> buffer -> store. Satisfies ``ObservationSink``.

    Validation stays per observation — the guard sees every row on its own, and
    a rejection names exactly one record. Only the *write* is batched, so a
    backfill costs one transaction per ``batch_size`` rows instead of one per
    row. Callers must :meth:`flush` when the source is exhausted; the ingest
    runner does this in a ``finally``.
    """

    def __init__(
        self,
        *,
        store: SignalStore,
        guard: LeakageGuard,
        manifest: AdapterManifest,
        mode: IngestMode,
        run_id: str,
        clock: Callable[[], datetime] | None = None,
        resolver: EntityResolver | None = None,
        entity_kind: str = "ticker",
        resolution_floor: float = 0.0,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._store = store
        self._guard = guard
        self._manifest = manifest
        self._mode = mode
        self._run_id = run_id
        self._clock = clock or (lambda: datetime.now(UTC))
        self._resolver = resolver
        self._entity_kind = entity_kind
        self._resolution_floor = resolution_floor
        self._batch_size = max(1, batch_size)
        self._pending: list[ValidatedObservation] = []
        self.resolved_count = 0
        self.unresolved_count = 0
        self.rows_written = 0
        self.rows_skipped = 0

    def _resolve(self, observation: Observation) -> Observation:
        """Attach ``security_id`` as of the moment the value became knowable.

        Resolving at ``knowledge_time`` — not at ingest time, and not at
        event time — is the only choice that cannot rewrite history: it asks
        what the key meant to someone reading it then. Unresolved rows are
        stored with a NULL ``security_id`` rather than dropped or guessed;
        the factor engine falls back to ``native_entity`` and the count is
        reported so thin resolution coverage is visible, not silent.
        """
        if self._resolver is None or observation.security_id is not None:
            return observation
        security_id = self._resolver.resolve_native(
            self._entity_kind,
            observation.native_entity,
            as_of=AsOf.from_knowledge_time(observation.knowledge_time),
            floor=self._resolution_floor,
        )
        if security_id is None:
            self.unresolved_count += 1
            return observation
        self.resolved_count += 1
        return observation.resolved(security_id)

    async def submit(self, observation: Observation) -> None:
        resolved = self._resolve(observation)
        stamped = resolved.stamped(IngestTime.at(self._clock()))
        # Validate now, not at flush: a LeakageViolation must point at the row
        # that caused it, and the caller must be able to catch it per record.
        validated = self._guard.validate(stamped, manifest=self._manifest, mode=self._mode)
        self._pending.append(validated)
        if len(self._pending) >= self._batch_size:
            await self.flush()

    async def flush(self) -> None:
        """Write everything buffered so far. Safe to call on an empty buffer."""
        if not self._pending:
            return
        batch, self._pending = self._pending, []
        receipt = self._store.write(batch, run_id=self._run_id)
        self.rows_written += receipt.rows_written
        self.rows_skipped += receipt.rows_skipped

    @property
    def pending(self) -> int:
        """Rows validated but not yet written. Non-zero means a flush is owed."""
        return len(self._pending)
