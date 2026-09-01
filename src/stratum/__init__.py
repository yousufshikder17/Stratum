"""Stratum — point-in-time-correct alt-data research infrastructure.

Research tooling, not a prediction or trading product. Storage, guarded
ingestion, snapshots, and point-in-time factor computation are operational;
simulation belongs to the sibling Ledger project.
"""

from stratum.schema import (
    AsOf,
    DataClass,
    EventTime,
    IngestTime,
    KnowledgeTime,
    Observation,
    Payload,
    PointInTimeRecord,
    publication_lag,
)

__version__ = "0.1.0.dev0"

__all__ = [
    "AsOf",
    "DataClass",
    "EventTime",
    "IngestTime",
    "KnowledgeTime",
    "Observation",
    "Payload",
    "PointInTimeRecord",
    "__version__",
    "publication_lag",
]
