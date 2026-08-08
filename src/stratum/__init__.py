"""Stratum — point-in-time-correct alt-data research infrastructure.

Pre-alpha research tooling, not a prediction or trading product. Operational
storage, ingestion, factor computation, and backtesting are not implemented.
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
