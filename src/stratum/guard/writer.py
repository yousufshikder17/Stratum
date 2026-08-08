"""The guarded write path.

``GuardedWriter`` is the core's only implementation of the
:class:`~stratum.adapters.context.ObservationSink` protocol handed to adapters
via ``AdapterContext.sink``. It stamps the core-owned ``ingest_time``, runs
the leakage guard, and forwards the resulting :class:`ValidatedObservation` to
``SignalStore.write`` — adapters physically cannot skip the guard because the
store's write signature does not accept raw observations.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from stratum.adapters.manifest import AdapterManifest
from stratum.guard.leakage import IngestMode, LeakageGuard
from stratum.schema.observation import Observation
from stratum.schema.times import IngestTime

if TYPE_CHECKING:
    from stratum.store.interface import SignalStore

__all__ = ["GuardedWriter"]


class GuardedWriter:
    """Guard -> stamp -> store. Satisfies ``ObservationSink`` structurally."""

    def __init__(
        self,
        *,
        store: SignalStore,
        guard: LeakageGuard,
        manifest: AdapterManifest,
        mode: IngestMode,
        run_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._guard = guard
        self._manifest = manifest
        self._mode = mode
        self._run_id = run_id
        self._clock = clock or (lambda: datetime.now(UTC))

    async def submit(self, observation: Observation) -> None:
        stamped = observation.stamped(IngestTime.at(self._clock()))
        validated = self._guard.validate(stamped, manifest=self._manifest, mode=self._mode)
        self._store.write([validated], run_id=self._run_id)
