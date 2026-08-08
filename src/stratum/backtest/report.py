"""Diagnostics reporting — the honesty layer (spec §6.5).

Every backtest emits a diagnostics report, not a P&L headline. The report
speaks in IC / rank-IC / decay / coverage / turnover against a declared
baseline. **No "alpha," no "expected return," no Sharpe presented as a
promise** — enforced here in the template and by
:func:`assert_honest_framing`, not left to the user.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib.resources import files
from typing import Any

__all__ = [
    "BiasScorecard",
    "DiagnosticsReport",
    "HonestFramingError",
    "ReproStamp",
    "assert_honest_framing",
    "render_markdown",
]

#: Phrases the report surface may never present as claims (spec §1, §6.5).
FORBIDDEN_CLAIM_TERMS: tuple[str, ...] = (
    "alpha",
    "expected return",
    "guaranteed",
    "edge over the market",
)


class HonestFramingError(ValueError):
    """A rendered report contains promise-language the platform forbids."""


@dataclass(frozen=True, kw_only=True, slots=True)
class BiasScorecard:
    """Required report section (spec §6.5): explicit pass/fail per guard."""

    pit_universe_used: bool
    costs_enabled: bool
    no_whole_sample_normalization: bool
    delisting_handling_on: bool
    leakage_suite_passed: bool
    #: True when costs were disabled and outputs are labeled accordingly.
    gross_illustrative: bool = False


@dataclass(frozen=True, kw_only=True, slots=True)
class ReproStamp:
    """Records the inputs intended to identify a reproducible run."""

    snapshot_hash: str
    factor_def_hash: str
    universe_id: str
    cost_model_id: str
    seed: int
    code_version: str


@dataclass(frozen=True, kw_only=True, slots=True)
class DiagnosticsReport:
    factor_id: str
    factor_version: str
    baseline: str | None
    #: ic / rank_ic / turnover / decay / coverage per horizon, sub-period
    #: stability, embargo-sensitivity — diagnostics vocabulary only.
    metrics: Mapping[str, Any]
    scorecard: BiasScorecard
    stamp: ReproStamp
    caveats: tuple[str, ...] = field(
        default=(
            "Outputs are research hypotheses and diagnostics, not investment "
            "advice, and measure nothing about future returns.",
        )
    )


def assert_honest_framing(rendered: str) -> None:
    """Reject rendered output containing forbidden promise-language.

    Deliberately blunt: template authors work around a false positive by
    rephrasing, which is exactly the pressure the spec intends (§6.5).
    """
    lowered = rendered.lower()
    for term in FORBIDDEN_CLAIM_TERMS:
        if term in lowered:
            raise HonestFramingError(
                f"report contains forbidden claim language: {term!r} (spec §6.5)"
            )


def render_markdown(report: DiagnosticsReport) -> str:
    """Render the diagnostics report through the enforced template."""
    import jinja2  # lazy: keep the core importable without jinja2 installed

    template_text = (
        files("stratum.backtest").joinpath("templates/report.md.j2").read_text(encoding="utf-8")
    )
    env = jinja2.Environment(autoescape=False, undefined=jinja2.StrictUndefined)
    rendered = env.from_string(template_text).render(report=report)
    assert_honest_framing(rendered)
    return rendered
