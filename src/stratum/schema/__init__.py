"""The common signal schema (spec §4) — Stratum's architectural waist.

Import surface for the envelope (``Observation`` == ``PointInTimeRecord``),
the two clocks, typed payloads, data classification, and the registry.
"""

from stratum.schema.data_class import DataClass
from stratum.schema.observation import SCHEMA_VERSION, Observation, PointInTimeRecord
from stratum.schema.payloads import (
    STANDARD_PAYLOAD_TYPES,
    FilingEvent,
    FundamentalFact,
    GenericPayload,
    GeoActivityIndex,
    HiringAttention,
    MarketBar,
    MarketCorporateAction,
    MarketDelisting,
    MetaCoverage,
    ModelRef,
    Payload,
    SearchInterest,
    SocialAttention,
    SocialSentiment,
    WeatherAnomaly,
)
from stratum.schema.registry import SchemaRegistry, SchemaViolation
from stratum.schema.times import AsOf, EventTime, IngestTime, KnowledgeTime, publication_lag

__all__ = [
    "SCHEMA_VERSION",
    "STANDARD_PAYLOAD_TYPES",
    "AsOf",
    "DataClass",
    "EventTime",
    "FilingEvent",
    "FundamentalFact",
    "GenericPayload",
    "GeoActivityIndex",
    "HiringAttention",
    "IngestTime",
    "KnowledgeTime",
    "MarketBar",
    "MarketCorporateAction",
    "MarketDelisting",
    "MetaCoverage",
    "ModelRef",
    "Observation",
    "Payload",
    "PointInTimeRecord",
    "SchemaRegistry",
    "SchemaViolation",
    "SearchInterest",
    "SocialAttention",
    "SocialSentiment",
    "WeatherAnomaly",
    "publication_lag",
]
