"""Declarative factor definitions (spec §5.2).

A factor is a reproducible, point-in-time function from signals + market data
to a cross-sectional exposure, plus metadata describing what it claims to
capture and how to evaluate it. Definitions are plain YAML, versioned so the
community library is comparable, auditable, and forkable.

PIT policy is enforced structurally: ``pit.as_of_rule`` has exactly one legal
value — ``knowledge_time``. A definition selecting on any other axis fails to
load; there is no configuration that reads the future.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

__all__ = [
    "EvaluationSpec",
    "FactorDefinition",
    "FactorFamily",
    "FactorInput",
    "InvalidFactorDefinition",
    "PitPolicy",
    "TransformStep",
    "load_factor",
]


class InvalidFactorDefinition(ValueError):
    """The YAML does not describe a valid, PIT-safe factor."""


class FactorFamily(StrEnum):
    """v1 vocabulary (spec §5.3). Every factor declares a family and a
    baseline so incremental information is framed honestly."""

    MOMENTUM = "momentum"
    VALUE = "value"
    QUALITY = "quality"
    SENTIMENT = "sentiment"
    CUSTOM = "custom"


@dataclass(frozen=True, kw_only=True, slots=True)
class FactorInput:
    """One input: an alt-data signal (``signal``) or market data (``market``)."""

    signal: str | None = None  # e.g. "social.attention"
    market: str | None = None  # e.g. "market.bar"
    series_id: str | None = None  # e.g. "Assets|USD||2025-12-31"
    field: str | None = None
    window: str | None = None  # e.g. "5d"
    #: Coverage floor (spec §5.4): thin observations are excluded or
    #: downweighted, surfaced in diagnostics — never silently overstated.
    min_coverage: int | None = None

    def __post_init__(self) -> None:
        if (self.signal is None) == (self.market is None):
            raise InvalidFactorDefinition(
                "each input must name exactly one of 'signal' or 'market'"
            )
        if self.min_coverage is not None and self.min_coverage <= 0:
            raise InvalidFactorDefinition("input min_coverage must be positive")


@dataclass(frozen=True, kw_only=True, slots=True)
class PitPolicy:
    """Selection axis + embargo (spec §5.2 ``pit:`` block)."""

    as_of_rule: str = "knowledge_time"
    #: Don't use a signal until this long after it became knowable; the
    #: engine evaluates at ``as_of = t - embargo`` (spec §5.4, §6.1).
    embargo: str = "1d"

    def __post_init__(self) -> None:
        if self.as_of_rule != "knowledge_time":
            raise InvalidFactorDefinition(
                f"pit.as_of_rule={self.as_of_rule!r}: the only legal selection axis is "
                "'knowledge_time' (spec §5.2) — selecting on event_time or ingest_time "
                "is lookahead by construction"
            )


@dataclass(frozen=True, kw_only=True, slots=True)
class TransformStep:
    op: str  # must name a registered PIT-safe op (transforms.py)
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True, slots=True)
class EvaluationSpec:
    rebalance: str = "weekly"
    horizon: Sequence[str] = ("1d", "5d", "21d")
    #: Diagnostics vocabulary (spec §6.5): IC/turnover/decay/coverage —
    #: never "alpha" or promised return.
    metrics: Sequence[str] = ("ic", "rank_ic", "turnover", "decay", "coverage")


@dataclass(frozen=True, kw_only=True, slots=True)
class FactorDefinition:
    id: str
    version: str
    family: FactorFamily
    description: str
    author: str = ""
    license: str | None = None
    #: Declared comparison baseline (spec §5.3), e.g. "price_momentum_12_1".
    baseline: str | None = None
    inputs: Sequence[FactorInput] = ()
    pit: PitPolicy = field(default_factory=PitPolicy)
    transform: Sequence[TransformStep] = ()
    evaluation: EvaluationSpec = field(default_factory=EvaluationSpec)


def _parse(data: Mapping[str, Any], *, origin: str) -> FactorDefinition:
    try:
        factor = data["factor"]
        inputs = [
            FactorInput(
                signal=item.get("signal"),
                market=item.get("market"),
                series_id=item.get("series_id"),
                field=item.get("field"),
                window=item.get("window"),
                min_coverage=item.get("min_coverage"),
            )
            for item in data.get("inputs", [])
        ]
        pit_raw = data.get("pit", {})
        transform = [
            TransformStep(op=step["op"], params={k: v for k, v in step.items() if k != "op"})
            for step in data.get("transform", [])
        ]
        eval_raw = data.get("evaluation", {})
        return FactorDefinition(
            id=factor["id"],
            version=str(factor["version"]),
            family=FactorFamily(factor["family"]),
            description=factor.get("description", "").strip(),
            author=factor.get("author", ""),
            license=factor.get("license"),
            baseline=factor.get("baseline"),
            inputs=inputs,
            pit=PitPolicy(
                as_of_rule=pit_raw.get("as_of_rule", "knowledge_time"),
                embargo=pit_raw.get("embargo", "1d"),
            ),
            transform=transform,
            evaluation=EvaluationSpec(
                rebalance=eval_raw.get("rebalance", "weekly"),
                horizon=tuple(eval_raw.get("horizon", ("1d", "5d", "21d"))),
                metrics=tuple(
                    eval_raw.get("metrics", ("ic", "rank_ic", "turnover", "decay", "coverage"))
                ),
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, InvalidFactorDefinition):
            raise
        raise InvalidFactorDefinition(f"invalid factor definition at {origin}: {exc}") from exc


def load_factor(path: Path) -> FactorDefinition:
    import yaml  # lazy: keep the schema/guard core importable without PyYAML

    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise InvalidFactorDefinition(f"{path}: expected a YAML mapping")
    return _parse(data, origin=str(path))
