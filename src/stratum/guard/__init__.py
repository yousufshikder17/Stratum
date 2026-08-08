"""Bias-guard hooks (spec §2.1): the leakage guard and the guarded write path.

The guard sits between the adapter layer and the PIT store; the store's write
signature accepts only :class:`ValidatedObservation`, which only
:class:`LeakageGuard` can produce. No adapter output crosses this boundary
without validation.
"""

from stratum.guard.leakage import (
    GuardPolicy,
    IngestMode,
    LeakageGuard,
    LeakageViolation,
    ValidatedObservation,
)
from stratum.guard.writer import GuardedWriter

__all__ = [
    "GuardPolicy",
    "GuardedWriter",
    "IngestMode",
    "LeakageGuard",
    "LeakageViolation",
    "ValidatedObservation",
]
