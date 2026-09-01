"""Descriptive diagnostics for point-in-time factor exposures."""

from __future__ import annotations

from statistics import fmean, pstdev
from typing import Any

from stratum.factors.engine import ExposurePanel

__all__ = ["summarize_exposures"]


def summarize_exposures(panel: ExposurePanel) -> dict[str, dict[str, Any]]:
    """Summarize each cross-section without making return claims."""
    summary: dict[str, dict[str, Any]] = {}
    for day, cross_section in sorted(panel.exposures.items()):
        values = list(cross_section.values())
        summary[day.isoformat()] = {
            "coverage": panel.coverage[day],
            "mean": fmean(values) if values else None,
            "population_stddev": pstdev(values) if len(values) > 1 else 0.0 if values else None,
            "minimum": min(values) if values else None,
            "maximum": max(values) if values else None,
        }
    return summary
