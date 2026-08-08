"""Transform registry and unimplemented standard transform placeholders.

The registry accepts a single cross-section per call. Standard operation names
are registered, but their bodies are not implemented.

Ops are deterministic and seedable (spec §5.4); registration is the extension
point for factor packs that ship Python transform plugins (spec §5.5).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol

__all__ = ["CrossSection", "TransformOp", "get_op", "register_op", "registered_ops"]


class CrossSection(Protocol):
    """One rebalance date's exposure vector: entity -> value.

    Deliberately narrow: an op sees one date's cross-section (plus explicit
    per-entity history already filtered to ``<= as_of`` by the engine), so a
    whole-sample fit is unrepresentable, not merely discouraged.
    """

    def values(self) -> Mapping[str, float]: ...


TransformOp = Callable[[Mapping[str, float], Mapping[str, Any]], Mapping[str, float]]

_REGISTRY: dict[str, TransformOp] = {}


def register_op(name: str) -> Callable[[TransformOp], TransformOp]:
    def deco(fn: TransformOp) -> TransformOp:
        if name in _REGISTRY:
            raise ValueError(f"transform op {name!r} already registered")
        _REGISTRY[name] = fn
        return fn

    return deco


def get_op(name: str) -> TransformOp:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"unknown transform op {name!r}; registered: {sorted(_REGISTRY)}") from None


def registered_ops() -> list[str]:
    return sorted(_REGISTRY)


# --- Standard operation placeholders ---


@register_op("pct_change")
def pct_change(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    raise NotImplementedError("pct_change is not implemented")


@register_op("winsorize")
def winsorize(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    raise NotImplementedError("winsorize is not implemented")


@register_op("sector_neutralize")
def sector_neutralize(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    raise NotImplementedError("sector_neutralize is not implemented")


@register_op("cross_sectional_rank")
def cross_sectional_rank(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    raise NotImplementedError("cross_sectional_rank is not implemented")


@register_op("zscore")
def zscore(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    raise NotImplementedError("zscore is not implemented")
