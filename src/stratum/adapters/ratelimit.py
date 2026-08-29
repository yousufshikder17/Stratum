"""Core-supplied rate limiting (spec §3.2 rule 5).

Adapters declare what they need; **the core enforces it**. The limiter is
handed to an adapter through :class:`~stratum.adapters.context.AdapterContext`
and is shared *per provider*, not per adapter — two adapters pulling from the
same host are one client as far as that host's terms are concerned, and a
per-adapter limiter would quietly double the request rate.

:class:`TokenBucketRateLimiter` refills continuously at ``requests_per_second``
up to ``burst``. Continuous refill rather than fixed windows, because a window
boundary lets a caller spend a whole window's budget at the end of one window
and the next at the start of the following one — briefly double the rate a
provider agreed to.

:class:`Unlimited` is the honest no-op for file-backed adapters. It is not the
default for network adapters: the ingest runner refuses to run an adapter whose
manifest declares ``network = true`` without a configured limit, so "we forgot
to rate-limit EDGAR" fails at startup rather than at the provider's discretion.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

__all__ = ["RateLimitConfig", "TokenBucketRateLimiter", "Unlimited"]


class Unlimited:
    """No throttling. Correct for local files; never for a network source."""

    async def acquire(self, cost: int = 1) -> None:
        return None


@dataclass(frozen=True, kw_only=True, slots=True)
class RateLimitConfig:
    """A provider's declared budget."""

    requests_per_second: float
    #: Largest instantaneous burst. Defaults to one second's worth, rounded up
    #: to at least 1 — a bucket that cannot hold one token never releases.
    burst: int = 0

    def __post_init__(self) -> None:
        if self.requests_per_second <= 0:
            raise ValueError(
                f"requests_per_second must be positive, got {self.requests_per_second} "
                "(use no rate_limits entry, not zero, to mean unlimited)"
            )
        if self.burst < 0:
            raise ValueError(f"burst cannot be negative, got {self.burst}")

    @property
    def capacity(self) -> int:
        return self.burst or max(1, int(self.requests_per_second))


class TokenBucketRateLimiter:
    """Continuous-refill token bucket, safe for concurrent adapters.

    The lock is held across the wait deliberately. Releasing it would let every
    waiter compute a delay against the same empty bucket and wake together —
    a thundering herd that spends the whole refilled budget in one instant,
    which is exactly what the limiter exists to prevent.
    """

    def __init__(
        self,
        config: RateLimitConfig,
        *,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], object] | None = None,
    ) -> None:
        self._config = config
        self._clock = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._tokens = float(config.capacity)
        self._updated = self._clock()
        self._lock = asyncio.Lock()
        #: Cumulative seconds spent waiting — surfaced in the run report so a
        #: slow ingest is visibly throttling rather than mysteriously slow.
        self.waited_seconds = 0.0

    @property
    def config(self) -> RateLimitConfig:
        return self._config

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        self._tokens = min(
            float(self._config.capacity),
            self._tokens + elapsed * self._config.requests_per_second,
        )

    async def acquire(self, cost: int = 1) -> None:
        if cost <= 0:
            return
        if cost > self._config.capacity:
            raise ValueError(
                f"cost {cost} exceeds burst capacity {self._config.capacity}; "
                "this request could never be admitted"
            )
        async with self._lock:
            self._refill()
            if self._tokens < cost:
                delay = (cost - self._tokens) / self._config.requests_per_second
                self.waited_seconds += delay
                await self._sleep(delay)  # type: ignore[misc]
                self._refill()
            self._tokens -= cost
