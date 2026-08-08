"""The observation envelope (spec §4.3) — the architectural waist.

Every adapter produces :class:`Observation`; every consumer (normalizer,
store, factor engine, backtest engine, exporters) reads it. Nothing else
crosses the adapter boundary.

The class is also exported as :data:`PointInTimeRecord` — the two names refer
to the same type; ``Observation`` is the spec's protobuf message name, and
``PointInTimeRecord`` names what it *is*.

Type-level guarantees:

- ``knowledge_time`` is a required, default-less :class:`KnowledgeTime`
  keyword field. Runtime validation rejects the wrong clock type.
- The dataclass is frozen: once constructed, times cannot be swapped or
  edited. Restatements are new vintages, never mutations (spec §4.4).
- ``ingest_time`` is core-owned: adapters leave it ``None``; the guarded
  write path stamps it (spec §4.2).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import final

from stratum.schema.data_class import DataClass
from stratum.schema.payloads import ModelRef, Payload
from stratum.schema.times import EventTime, IngestTime, KnowledgeTime, publication_lag

__all__ = ["SCHEMA_VERSION", "Observation", "PointInTimeRecord"]

#: Major.minor of the common signal schema this codebase speaks (spec §4.1).
SCHEMA_VERSION = "1.0"

_REQUIRED_NONEMPTY = (
    "observation_id",
    "signal_type",
    "run_id",
    "source_id",
    "adapter_id",
    "native_entity",
    "license_tag",
)


@final
@dataclass(frozen=True, kw_only=True, slots=True)
class Observation:
    """One schema-valid envelope + typed payload (spec §4.3)."""

    observation_id: str  # ULID
    signal_type: str  # e.g. "social.sentiment", "fundamental.fact"
    run_id: str  # ingestion run / dataset build
    source_id: str  # provider id ("edgar", "reddit")
    adapter_id: str

    #: Adapter's raw entity key (CIK, cashtag, search term). The adapter owns
    #: provenance; the core's resolver owns identity (spec §3.2 rule 2).
    native_entity: str
    #: Canonical entity id — resolver output; None until resolved (spec §4.5).
    security_id: str | None = None

    #: The two clocks (spec §4.2). Both required, both set by the adapter,
    #: mutually non-interchangeable at the type level.
    event_time: EventTime
    knowledge_time: KnowledgeTime
    #: Set by the core at write time. Audit only; never used for selection.
    ingest_time: IngestTime | None = None
    #: Restatement/backfill vintage key. Required (non-empty) for adapters
    #: whose manifest declares restatement support or a provider-vintage
    #: knowledge basis — enforced by the leakage guard (spec §3.6).
    vintage_id: str = ""

    data_class: DataClass
    license_tag: str  # provenance/terms (spec §4.7)
    #: Model + version when the value is derived (e.g. NLP sentiment).
    derived_by: ModelRef | None = None

    payload: Payload
    extensions: Mapping[str, bytes] = field(default_factory=dict)
    #: Optional opaque native record for audit/replay. Core logic never reads
    #: it (spec §3.2 rule 1).
    raw: bytes | None = None

    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.event_time, EventTime):
            raise TypeError("Observation.event_time must be an EventTime")
        if not isinstance(self.knowledge_time, KnowledgeTime):
            raise TypeError("Observation.knowledge_time must be a KnowledgeTime")
        if self.ingest_time is not None and not isinstance(self.ingest_time, IngestTime):
            raise TypeError("Observation.ingest_time must be an IngestTime or None")
        for name in _REQUIRED_NONEMPTY:
            if not getattr(self, name):
                raise ValueError(f"Observation.{name} must be a non-empty string")
        declared = self.payload.signal_type()
        if declared != self.signal_type:
            raise ValueError(
                f"payload declares signal_type {declared!r} but the envelope says "
                f"{self.signal_type!r} — typed payloads may not be relabeled"
            )

    def publication_lag(self) -> timedelta:
        """``knowledge_time - event_time`` — lag is data, not loss (spec §4.4)."""
        return publication_lag(self.event_time, self.knowledge_time)

    # -- Core-side helpers (adapters never call these) ----------------------

    def stamped(self, ingest_time: IngestTime) -> Observation:
        """Return a copy with the core-owned ``ingest_time`` set."""
        return replace(self, ingest_time=ingest_time)

    def resolved(self, security_id: str) -> Observation:
        """Return a copy with the resolver's canonical ``security_id`` attached."""
        return replace(self, security_id=security_id)


#: The same type under the name that says what it is. Every adapter produces
#: it; every consumer reads it (spec §4.1: the load-bearing invariant).
PointInTimeRecord = Observation
