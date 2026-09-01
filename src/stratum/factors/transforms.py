"""Standard PIT-safe cross-sectional transforms (spec §5.4).

The registry accepts a single cross-section per call. Standard operations are
implemented below.

Ops are deterministic and seedable (spec §5.4); registration is the extension
point for factor packs that ship Python transform plugins (spec §5.5).

Every op here sees ONE rebalance date's exposure vector — there is no
whole-sample entry point. ``pct_change`` is the only op that needs history:
its baseline arrives via ``params["base"]`` as explicit per-entity values
already filtered to ``<= as_of`` by the engine, so a full-sample fit remains
unrepresentable rather than merely discouraged.
"""

from __future__ import annotations

import math
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


def _quantile(ordered: list[float], q: float) -> float:
    pos = q * (len(ordered) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


# --- Standard operations ---


@register_op("pct_change")
def pct_change(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    """Change relative to a prior value per entity: ``(x - base) / |base|``.

    ``params["base"]`` maps entity -> the value one declared window earlier;
    the engine supplies it from knowledge-time-filtered history only. Entities
    without a base are excluded (missing data is absent, never zero).
    """
    base: Mapping[str, float] = params["base"]
    out: dict[str, float] = {}
    for entity, value in xs.items():
        if entity not in base:
            continue
        prior = base[entity]
        if prior == 0.0:
            raise ValueError(f"pct_change: zero baseline for {entity!r}")
        out[entity] = (value - prior) / abs(prior)
    return out


@register_op("winsorize")
def winsorize(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    """Clamp to the [lower, upper] empirical quantiles of THIS cross-section."""
    lo_q, hi_q = params.get("limits", (0.01, 0.99))
    entities = sorted(xs)
    values = [xs[e] for e in entities]
    ordered = sorted(values)
    lo = _quantile(ordered, float(lo_q))
    hi = _quantile(ordered, float(hi_q))
    return {e: min(max(v, lo), hi) for e, v in zip(entities, values, strict=True)}


@register_op("sector_neutralize")
def sector_neutralize(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    """Subtract each sector's mean within this cross-section (residuals of a
    sector-dummy regression fit inside ``t`` only). ``params["sectors"]``
    maps entity -> sector label; unlabeled entities are excluded."""
    sectors: Mapping[str, str] = params["sectors"]
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    for entity, value in xs.items():
        if entity in sectors:
            s = sectors[entity]
            sums[s] = sums.get(s, 0.0) + value
            counts[s] = counts.get(s, 0) + 1
    means = {s: sums[s] / counts[s] for s in sums}
    return {
        entity: value - means[sectors[entity]] for entity, value in xs.items() if entity in sectors
    }


@register_op("cross_sectional_rank")
def cross_sectional_rank(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    """Fractional ranks in [0, 1] within this cross-section (average ties)."""
    entities = sorted(xs)
    n = len(entities)
    if n == 0:
        return {}
    if n == 1:
        return {entities[0]: 0.5}
    values = [xs[e] for e in entities]
    order = sorted(range(n), key=lambda i: values[i])
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg = (i + j) / 2.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg / (n - 1)
        i = j + 1
    return dict(zip(entities, ranks, strict=True))


@register_op("zscore")
def zscore(xs: Mapping[str, float], params: Mapping[str, Any]) -> Mapping[str, float]:
    """Z-score within THIS cross-section (sample std, n-1). A degenerate
    (zero-variance) cross-section maps to all-zeros rather than NaNs."""
    entities = sorted(xs)
    n = len(entities)
    if n < 2:
        raise ValueError("z-score needs at least 2 entities in the cross-section")
    values = [xs[e] for e in entities]
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    if var == 0.0:
        return dict.fromkeys(entities, 0.0)
    std = math.sqrt(var)
    return {e: (v - mean) / std for e, v in zip(entities, values, strict=True)}
