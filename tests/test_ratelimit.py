"""Core-supplied rate limiting (spec §3.2 rule 5).

The limiter is driven with an injected clock and sleep, so these assert the
*budget arithmetic* rather than wall-clock timing — a test that really slept
would be slow and flaky and would still not prove the refill maths.
"""

from __future__ import annotations

import asyncio

import pytest

from stratum.adapters.ratelimit import RateLimitConfig, TokenBucketRateLimiter, Unlimited


class FakeClock:
    """A monotonic clock that only advances when something sleeps."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def limiter(rate: float = 2.0, burst: int = 2) -> tuple[TokenBucketRateLimiter, FakeClock]:
    clock = FakeClock()
    return (
        TokenBucketRateLimiter(
            RateLimitConfig(requests_per_second=rate, burst=burst),
            clock=clock,
            sleep=clock.sleep,
        ),
        clock,
    )


async def test_burst_is_free_then_the_rate_binds() -> None:
    bucket, clock = limiter(rate=2.0, burst=2)
    await bucket.acquire()
    await bucket.acquire()
    assert clock.slept == []  # the burst budget
    await bucket.acquire()
    assert clock.slept == [0.5]  # 1 token at 2/s
    assert bucket.waited_seconds == pytest.approx(0.5)


async def test_tokens_refill_over_time() -> None:
    bucket, clock = limiter(rate=2.0, burst=2)
    await bucket.acquire()
    await bucket.acquire()
    clock.now += 1.0  # 2 tokens' worth of elapsed time
    await bucket.acquire()
    await bucket.acquire()
    assert clock.slept == []


async def test_refill_is_capped_at_burst() -> None:
    """An idle hour must not buy an hour's worth of instantaneous requests."""
    bucket, clock = limiter(rate=2.0, burst=2)
    clock.now += 3600.0
    await bucket.acquire()
    await bucket.acquire()
    await bucket.acquire()
    assert clock.slept == [0.5]


async def test_cost_is_honored() -> None:
    bucket, clock = limiter(rate=2.0, burst=4)
    await bucket.acquire(cost=4)
    await bucket.acquire(cost=2)
    assert clock.slept == [1.0]


async def test_zero_cost_is_free() -> None:
    bucket, clock = limiter(rate=1.0, burst=1)
    await bucket.acquire(cost=0)
    assert clock.slept == []


async def test_impossible_cost_is_refused_not_hung() -> None:
    """A request larger than the bucket could never be admitted; saying so
    beats waiting forever."""
    bucket, _ = limiter(rate=2.0, burst=2)
    with pytest.raises(ValueError, match="could never be admitted"):
        await bucket.acquire(cost=3)


async def test_concurrent_callers_share_one_budget() -> None:
    """Two adapters on one provider are one client to that provider. Serialize
    them, or the refilled budget gets spent twice in the same instant."""
    bucket, clock = limiter(rate=1.0, burst=1)
    await asyncio.gather(*(bucket.acquire() for _ in range(4)))
    # One free, then three waits of a second each — not four simultaneous ones.
    assert clock.slept == [1.0, 1.0, 1.0]
    assert bucket.waited_seconds == pytest.approx(3.0)


async def test_unlimited_never_waits() -> None:
    unlimited = Unlimited()
    for _ in range(1000):
        await unlimited.acquire()


def test_capacity_defaults_to_one_seconds_worth() -> None:
    assert RateLimitConfig(requests_per_second=5.0).capacity == 5
    assert RateLimitConfig(requests_per_second=0.2).capacity == 1  # never zero
    assert RateLimitConfig(requests_per_second=5.0, burst=20).capacity == 20


def test_nonpositive_rate_is_rejected() -> None:
    """Zero would mean "never admit anything", which is never what someone
    means — they mean unlimited, which is the absence of a config entry."""
    with pytest.raises(ValueError, match="must be positive"):
        RateLimitConfig(requests_per_second=0.0)
    with pytest.raises(ValueError, match="must be positive"):
        RateLimitConfig(requests_per_second=-1.0)


def test_negative_burst_is_rejected() -> None:
    with pytest.raises(ValueError, match="burst"):
        RateLimitConfig(requests_per_second=1.0, burst=-1)
