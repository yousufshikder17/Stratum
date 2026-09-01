"""The factor framework (spec §5): declarative, versioned, PIT-enforced."""

from stratum.factors.definition import (
    EvaluationSpec,
    FactorDefinition,
    FactorFamily,
    FactorInput,
    InvalidFactorDefinition,
    PitPolicy,
    TransformStep,
    load_factor,
)
from stratum.factors.diagnostics import summarize_exposures
from stratum.factors.engine import ExposurePanel, FactorEngine

__all__ = [
    "EvaluationSpec",
    "ExposurePanel",
    "FactorDefinition",
    "FactorEngine",
    "FactorFamily",
    "FactorInput",
    "InvalidFactorDefinition",
    "PitPolicy",
    "TransformStep",
    "load_factor",
    "summarize_exposures",
]
