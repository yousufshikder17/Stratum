"""The single research TOML config (spec §8 MVP item 1).

One file describes a research environment: where the point-in-time store
lives, which adapters to run with what settings, which resolver tables give
canonical identity, and the backfill window. ``stratum ingest --config
research.toml`` needs nothing else.

**Relative paths resolve against the config file's own directory**, not the
process working directory, so a config plus its data files is a movable
bundle and a run does not silently read a different dataset because it was
launched from elsewhere.
"""

from __future__ import annotations

import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from stratum.adapters.ratelimit import RateLimitConfig

__all__ = [
    "AdapterConfig",
    "BackfillWindow",
    "ConfigError",
    "ResearchConfig",
    "ResolverConfig",
]


class ConfigError(ValueError):
    """The research config is missing something, or asks for the impossible."""


def _resolve(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base / path)


def _require_date(raw: object, *, field_name: str) -> date:
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise ConfigError(f"{field_name} {raw!r} is not an ISO date") from exc
    raise ConfigError(f"{field_name} must be an ISO date, got {type(raw).__name__}")


@dataclass(frozen=True, kw_only=True, slots=True)
class BackfillWindow:
    """Half-open ``[start, end)`` on the **event** axis."""

    start: date
    end: date

    def __post_init__(self) -> None:
        if not self.start < self.end:
            raise ConfigError(f"backfill window needs start < end, got {self.start}..{self.end}")

    def as_datetimes(self) -> tuple[datetime, datetime]:
        return (
            datetime(self.start.year, self.start.month, self.start.day, tzinfo=UTC),
            datetime(self.end.year, self.end.month, self.end.day, tzinfo=UTC),
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class ResolverConfig:
    """Versioned identity tables (spec §4.5)."""

    tables: Path | None = None
    version: str | None = None
    #: Minimum confidence before a match is accepted. 0.0 takes any match;
    #: raise it to keep low-confidence social mentions out of a cross-section.
    floor: float = 0.0

    def __post_init__(self) -> None:
        if not 0.0 <= self.floor <= 1.0:
            raise ConfigError(f"resolver.floor {self.floor} is outside 0..1")


@dataclass(frozen=True, kw_only=True, slots=True)
class AdapterConfig:
    """One adapter to run, and the settings handed to its ``configure()``."""

    id: str
    #: What kind of native key this adapter's ``native_entity`` holds, so the
    #: resolver knows how to read it ("ticker", "cashtag", "cik", ...).
    entity_kind: str = "ticker"
    #: Which rate-limit budget this adapter draws on. Adapters sharing a
    #: provider share one bucket — two adapters against the same host are one
    #: client as far as that host's terms are concerned. Defaults to the
    #: adapter id, i.e. its own budget.
    provider: str = ""
    settings: Mapping[str, Any] = field(default_factory=dict)

    @property
    def provider_key(self) -> str:
        return self.provider or self.id


@dataclass(frozen=True, kw_only=True, slots=True)
class ResearchConfig:
    """A whole research environment, parsed and path-resolved."""

    source: Path
    store_path: Path
    adapters: Sequence[AdapterConfig]
    resolver: ResolverConfig = field(default_factory=ResolverConfig)
    backfill: BackfillWindow | None = None
    #: provider -> declared budget. A provider absent here is unlimited, which
    #: is correct for local files and refused for adapters whose manifest
    #: declares ``network = true``.
    rate_limits: Mapping[str, RateLimitConfig] = field(default_factory=dict)
    run_id: str | None = None
    seed: int = 0

    @classmethod
    def load(cls, path: Path) -> ResearchConfig:
        if not path.exists():
            raise ConfigError(f"config not found: {path}")
        with path.open("rb") as fh:
            data = tomllib.load(fh)
        return cls.parse(data, source=path)

    @classmethod
    def parse(cls, data: Mapping[str, Any], *, source: Path) -> ResearchConfig:
        base = source.parent

        store = data.get("store", {})
        store_path = store.get("path")
        if not store_path:
            raise ConfigError('config needs [store] path = "..." (the PIT store file)')

        raw_adapters = data.get("adapters", [])
        if not isinstance(raw_adapters, list) or not raw_adapters:
            raise ConfigError(
                "config needs at least one [[adapters]] block — an ingest run "
                "with no adapters would silently write nothing"
            )
        adapters = []
        for index, raw in enumerate(raw_adapters):
            adapter_id = raw.get("id")
            if not adapter_id:
                raise ConfigError(f"[[adapters]] #{index + 1} is missing 'id'")
            settings = dict(raw.get("config", {}))
            # Path-valued settings are resolved relative to the config file so
            # a bundle stays movable.
            for key, value in settings.items():
                if key.endswith("_path") and isinstance(value, str):
                    settings[key] = str(_resolve(base, value))
            adapters.append(
                AdapterConfig(
                    id=str(adapter_id),
                    entity_kind=str(raw.get("entity_kind", "ticker")),
                    provider=str(raw.get("provider", "")),
                    settings=settings,
                )
            )

        rate_limits: dict[str, RateLimitConfig] = {}
        for provider, raw_limit in data.get("rate_limits", {}).items():
            if "requests_per_second" not in raw_limit:
                raise ConfigError(f"[rate_limits.{provider}] needs 'requests_per_second'")
            try:
                rate_limits[str(provider)] = RateLimitConfig(
                    requests_per_second=float(raw_limit["requests_per_second"]),
                    burst=int(raw_limit.get("burst", 0)),
                )
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"[rate_limits.{provider}]: {exc}") from exc

        raw_resolver = data.get("resolver", {})
        tables = raw_resolver.get("tables")
        resolver = ResolverConfig(
            tables=_resolve(base, str(tables)) if tables else None,
            version=str(raw_resolver["version"]) if raw_resolver.get("version") else None,
            floor=float(raw_resolver.get("floor", 0.0)),
        )

        raw_backfill = data.get("backfill")
        backfill = None
        if raw_backfill:
            if "start" not in raw_backfill or "end" not in raw_backfill:
                raise ConfigError("[backfill] needs both 'start' and 'end'")
            backfill = BackfillWindow(
                start=_require_date(raw_backfill["start"], field_name="backfill.start"),
                end=_require_date(raw_backfill["end"], field_name="backfill.end"),
            )

        run = data.get("run", {})
        return cls(
            source=source,
            store_path=_resolve(base, str(store_path)),
            adapters=tuple(adapters),
            resolver=resolver,
            backfill=backfill,
            rate_limits=rate_limits,
            run_id=str(run["id"]) if run.get("id") else None,
            seed=int(run.get("seed", 0)),
        )
