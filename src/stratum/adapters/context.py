"""AdapterContext (spec §3.2): what the core hands an adapter at configure().

The context is the *only* channel through which an adapter touches the outside
of its own source: logging, clock, data dir, secrets, the shared rate-limiter,
and — crucially — the write handle into the PIT store. That write handle is a
:class:`ObservationSink`, and the core's only implementation of it is the
guarded writer (``stratum.guard.writer.GuardedWriter``), so every adapter
observation passes the leakage guard before it can reach the store.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from stratum.schema.observation import Observation

__all__ = ["AdapterContext", "ObservationSink", "RateLimiter", "SecretsProvider"]


class RateLimiter(Protocol):
    """Core-supplied, shared per provider (spec §3.2 rule 5)."""

    async def acquire(self, cost: int = 1) -> None: ...


class SecretsProvider(Protocol):
    def get(self, key: str) -> str | None: ...


class ObservationSink(Protocol):
    """Write handle into the PIT store.

    Structurally satisfied only by the guarded writer in core: submissions are
    validated by the leakage guard and stamped with core-owned ``ingest_time``
    before hitting ``SignalStore.write``.
    """

    async def submit(self, observation: Observation) -> None: ...


@dataclass(frozen=True, kw_only=True)
class AdapterContext:
    logger: logging.Logger
    #: Injected clock — adapters must not call datetime.now() directly, so
    #: runs are testable and the guard's clock and the adapter's clock agree.
    clock: Callable[[], datetime]
    data_dir: Path
    secrets: SecretsProvider
    rate_limiter: RateLimiter
    sink: ObservationSink
    #: Ingestion run id, threaded into every observation (spec §4.3).
    run_id: str
    #: Extra core-supplied settings (cache dir, proxies, ...).
    settings: Mapping[str, str] | None = None
