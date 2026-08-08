"""Factor definitions are declarative and PIT-locked (spec §5.2)."""

from __future__ import annotations

from pathlib import Path

import pytest

from stratum.factors.definition import (
    FactorFamily,
    FactorInput,
    InvalidFactorDefinition,
    PitPolicy,
    load_factor,
)

REPO = Path(__file__).resolve().parents[1]
REFERENCE_YAML = (
    REPO / "src" / "stratum" / "factors" / "reference" / "reddit_attention_momentum.yaml"
)
WORKSPACE_YAML = REPO / "factors" / "reddit_attention_momentum.yaml"


@pytest.mark.parametrize("path", [REFERENCE_YAML, WORKSPACE_YAML], ids=["packaged", "workspace"])
def test_reference_factor_loads(path: Path) -> None:
    fd = load_factor(path)
    assert fd.id == "reddit_attention_momentum"
    assert fd.family is FactorFamily.SENTIMENT
    assert fd.baseline == "price_momentum_12_1"
    assert fd.pit.as_of_rule == "knowledge_time"
    assert fd.pit.embargo == "1d"
    assert [step.op for step in fd.transform] == [
        "pct_change",
        "winsorize",
        "sector_neutralize",
        "cross_sectional_rank",
        "zscore",
    ]
    assert fd.inputs[0].min_coverage == 50


def test_selection_axis_other_than_knowledge_time_is_illegal() -> None:
    with pytest.raises(InvalidFactorDefinition, match="knowledge_time"):
        PitPolicy(as_of_rule="event_time")
    with pytest.raises(InvalidFactorDefinition, match="knowledge_time"):
        PitPolicy(as_of_rule="ingest_time")


def test_input_names_exactly_one_source() -> None:
    with pytest.raises(InvalidFactorDefinition):
        FactorInput()  # neither signal nor market
    with pytest.raises(InvalidFactorDefinition):
        FactorInput(signal="social.attention", market="market.bar")  # both
