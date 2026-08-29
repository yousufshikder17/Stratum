"""The guarded ingestion run (spec §2.2, MVP item 1).

This is the pipeline the whole architecture exists to protect:

    adapter -> resolve identity -> stamp ingest_time -> leakage guard -> store

The runner drives each configured adapter's ``backfill(window)`` or ``poll()``
and hands every observation to that adapter's :class:`GuardedWriter`. It never
touches ``SignalStore.write`` itself — it *cannot*, because the store accepts
only the guard's proof type. There is no fast path, no bulk loader, and no
"trusted adapter" bypass; if there were, it would be the first thing a
deadline would reach for.

**Rejections are surfaced, not swallowed.** A row that fails the leakage guard
is dropped from the store and recorded in the report with its rule and reason,
and the CLI exits non-zero when any run rejected anything. A guard that
silently discarded rows would be indistinguishable from a source with gaps.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from stratum.adapters.base import Adapter, TimeWindow
from stratum.adapters.context import AdapterContext, RateLimiter
from stratum.adapters.discovery import discover, load_adapter
from stratum.adapters.manifest import AdapterManifest
from stratum.adapters.ratelimit import RateLimitConfig, TokenBucketRateLimiter, Unlimited
from stratum.config import AdapterConfig, ConfigError, ResearchConfig
from stratum.guard.leakage import IngestMode, LeakageGuard, LeakageViolation
from stratum.guard.writer import GuardedWriter
from stratum.resolver.entity import EntityResolver
from stratum.schema.observation import Observation
from stratum.store.duckdb_store import DuckDBSignalStore

if TYPE_CHECKING:
    from stratum.store.interface import SignalStore

__all__ = [
    "AdapterIngestResult",
    "IngestReport",
    "Rejection",
    "ingest",
    "run_ingest",
]

_LOGGER = logging.getLogger("stratum.ingest")


class _EnvSecrets:
    """Secrets from ``STRATUM_SECRET_<KEY>`` environment variables.

    Deliberately minimal, and deliberately not a file: a pre-alpha tool should
    not invent a credential store, and secrets should never land in the
    research config next to the data paths.
    """

    def get(self, key: str) -> str | None:
        return os.environ.get(f"STRATUM_SECRET_{key.upper()}")


@dataclass(frozen=True, kw_only=True, slots=True)
class Rejection:
    """One observation the guard refused."""

    adapter_id: str
    observation_id: str
    rule: str
    detail: str


@dataclass(kw_only=True)
class AdapterIngestResult:
    adapter_id: str
    mode: str
    rows_written: int = 0
    rows_skipped: int = 0
    resolved: int = 0
    unresolved: int = 0
    #: Seconds this adapter spent waiting on its provider's rate limit, so a
    #: slow ingest reads as throttled rather than mysteriously slow.
    throttled_seconds: float = 0.0
    rejections: list[Rejection] = field(default_factory=list)

    @property
    def rows_seen(self) -> int:
        return self.rows_written + self.rows_skipped + len(self.rejections)


@dataclass(kw_only=True)
class IngestReport:
    run_id: str
    store_path: str
    resolver_version: str | None
    results: list[AdapterIngestResult] = field(default_factory=list)

    @property
    def rows_written(self) -> int:
        return sum(r.rows_written for r in self.results)

    @property
    def rows_skipped(self) -> int:
        return sum(r.rows_skipped for r in self.results)

    @property
    def rejections(self) -> list[Rejection]:
        return [rejection for result in self.results for rejection in result.rejections]

    @property
    def clean(self) -> bool:
        """True when nothing was rejected — the CLI's exit-code condition."""
        return not self.rejections


def _rate_limiter(
    adapter_config: AdapterConfig,
    manifest: AdapterManifest,
    limits: Mapping[str, RateLimitConfig],
    cache: dict[str, TokenBucketRateLimiter],
) -> RateLimiter:
    """One bucket per provider, shared across the adapters that use it.

    A network adapter with no declared budget is refused rather than run
    unthrottled: an unlimited limiter against a real provider is how you get
    a source's terms broken and an IP blocked, and the failure would land on
    whoever runs the ingest, not on whoever forgot the config line.
    """
    provider = adapter_config.provider_key
    config = limits.get(provider)
    if config is None:
        if manifest.network:
            raise ConfigError(
                "\n".join(
                    [
                        f"adapter {adapter_config.id!r} declares network = true in "
                        f"its manifest but provider {provider!r} has no rate limit.",
                        "Add:",
                        f"    [rate_limits.{provider}]",
                        "    requests_per_second = 5.0",
                        "Rate limiting is core-supplied on purpose (spec §3.2 rule 5); "
                        "running a remote source unthrottled is not a default.",
                    ]
                )
            )
        return Unlimited()
    if provider not in cache:
        cache[provider] = TokenBucketRateLimiter(config)
    return cache[provider]


def _manifests() -> dict[str, AdapterManifest]:
    return {found.name: found.manifest for found in discover() if found.manifest is not None}


def _default_run_id(clock: datetime) -> str:
    return f"ingest-{clock.strftime('%Y%m%dT%H%M%SZ')}"


async def _drive(
    adapter: Adapter, *, mode: IngestMode, window: TimeWindow | None
) -> AsyncIterator[Observation]:
    if mode is IngestMode.BACKFILL:
        if window is None:
            raise ConfigError(
                "backfill needs a [backfill] window in the config — an unbounded "
                "historical pull has no defined end and cannot be reproduced"
            )
        async for observation in adapter.backfill(window):
            yield observation
        return
    async for observation in adapter.poll():
        yield observation


async def ingest(
    config: ResearchConfig,
    *,
    mode: IngestMode = IngestMode.BACKFILL,
    only: str | None = None,
    store: SignalStore | None = None,
    clock: Any = None,
    guard: LeakageGuard | None = None,
) -> IngestReport:
    """Run every configured adapter through the guarded path.

    ``store`` and ``clock`` are injectable so tests drive the real pipeline
    with a fixed clock rather than a mock of it.
    """
    now = clock or (lambda: datetime.now(UTC))
    run_id = config.run_id or _default_run_id(now())
    owns_store = store is None
    active_store: SignalStore = store or DuckDBSignalStore(config.store_path)
    active_guard = guard or LeakageGuard(now=now)

    resolver: EntityResolver | None = None
    if config.resolver.tables is not None:
        resolver = EntityResolver.from_csv(
            config.resolver.tables, tables_version=config.resolver.version
        )

    report = IngestReport(
        run_id=run_id,
        store_path=str(config.store_path),
        resolver_version=resolver.tables_version if resolver else None,
    )
    manifests = _manifests()
    #: Shared across adapters within this run, so two adapters on one provider
    #: draw from one budget rather than two.
    limiters: dict[str, TokenBucketRateLimiter] = {}
    selected = [a for a in config.adapters if only is None or a.id == only]
    if only is not None and not selected:
        raise ConfigError(f"adapter {only!r} is not configured in {config.source.name}")

    try:
        for adapter_config in selected:
            report.results.append(
                await _ingest_one(
                    adapter_config,
                    manifests=manifests,
                    store=active_store,
                    guard=active_guard,
                    resolver=resolver,
                    resolution_floor=config.resolver.floor,
                    mode=mode,
                    window=_window(config),
                    run_id=run_id,
                    clock=now,
                    data_dir=config.source.parent,
                    rate_limits=config.rate_limits,
                    limiters=limiters,
                )
            )
    finally:
        if owns_store:
            active_store.close()
    return report


def _window(config: ResearchConfig) -> TimeWindow | None:
    if config.backfill is None:
        return None
    start, end = config.backfill.as_datetimes()
    return TimeWindow(start=start, end=end)


async def _ingest_one(
    adapter_config: AdapterConfig,
    *,
    manifests: dict[str, AdapterManifest],
    store: SignalStore,
    guard: LeakageGuard,
    resolver: EntityResolver | None,
    resolution_floor: float,
    mode: IngestMode,
    window: TimeWindow | None,
    run_id: str,
    clock: Any,
    data_dir: Any,
    rate_limits: Mapping[str, RateLimitConfig],
    limiters: dict[str, TokenBucketRateLimiter],
) -> AdapterIngestResult:
    manifest = manifests.get(adapter_config.id)
    if manifest is None:
        raise ConfigError(
            f"adapter {adapter_config.id!r} is not installed — `stratum adapters` "
            "lists what is available"
        )
    limiter = _rate_limiter(adapter_config, manifest, rate_limits, limiters)
    adapter = load_adapter(adapter_config.id)()
    writer = GuardedWriter(
        store=store,
        guard=guard,
        manifest=manifest,
        mode=mode,
        run_id=run_id,
        clock=clock,
        resolver=resolver,
        entity_kind=adapter_config.entity_kind,
        resolution_floor=resolution_floor,
    )
    ctx = AdapterContext(
        logger=_LOGGER.getChild(adapter_config.id),
        clock=clock,
        data_dir=data_dir,
        secrets=_EnvSecrets(),
        rate_limiter=limiter,
        sink=writer,
        run_id=run_id,
        settings=None,
    )
    result = AdapterIngestResult(adapter_id=adapter_config.id, mode=mode.value)
    await adapter.configure(dict(adapter_config.settings), ctx)
    try:
        async for observation in _drive(adapter, mode=mode, window=window):
            try:
                await writer.submit(observation)
            except LeakageViolation as violation:
                result.rejections.append(
                    Rejection(
                        adapter_id=adapter_config.id,
                        observation_id=violation.observation.observation_id,
                        rule=violation.rule,
                        detail=str(violation),
                    )
                )
    finally:
        # The writer buffers, so the tail of the run is unwritten until this
        # runs. In a `finally` because a source that dies mid-stream should
        # still persist what it did produce.
        await writer.flush()
        await adapter.close()
    result.rows_written = writer.rows_written
    result.rows_skipped = writer.rows_skipped
    result.resolved = writer.resolved_count
    result.unresolved = writer.unresolved_count
    if isinstance(limiter, TokenBucketRateLimiter):
        result.throttled_seconds = limiter.waited_seconds
    return result


def run_ingest(
    config: ResearchConfig,
    *,
    mode: IngestMode = IngestMode.BACKFILL,
    only: str | None = None,
) -> IngestReport:
    """Synchronous entry point for the CLI."""
    return asyncio.run(ingest(config, mode=mode, only=only))


def format_report(report: IngestReport) -> Sequence[str]:
    """Human-readable run summary; one line per adapter, then the totals."""
    lines = [f"run {report.run_id} -> {report.store_path}"]
    if report.resolver_version:
        lines.append(f"resolver tables: {report.resolver_version}")
    for result in report.results:
        lines.append(
            f"  {result.adapter_id:<16} {result.mode:<8} "
            f"written={result.rows_written} skipped={result.rows_skipped} "
            f"resolved={result.resolved} unresolved={result.unresolved} "
            f"rejected={len(result.rejections)}"
            + (f" throttled={result.throttled_seconds:.1f}s" if result.throttled_seconds else "")
        )
    for rejection in report.rejections:
        lines.append(f"  REJECTED [{rejection.rule}] {rejection.adapter_id}: {rejection.detail}")
    lines.append(
        f"total: {report.rows_written} written, {report.rows_skipped} skipped, "
        f"{len(report.rejections)} rejected"
    )
    return lines
