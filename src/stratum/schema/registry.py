"""Schema registry (spec §2.2, §4.1).

Validates observations against the common signal schema, knows the standard
signal-type vocabulary, and accepts extension registrations (the promotion
path: extension -> experimental field -> standard field).

The Python classes and this registry are currently authoritative. No
Protobuf/Arrow schema or code-generation pipeline is included.
"""

from __future__ import annotations

from collections.abc import Iterable

from stratum.schema.observation import SCHEMA_VERSION, Observation
from stratum.schema.payloads import STANDARD_PAYLOAD_TYPES, GenericPayload, Payload

__all__ = ["SchemaRegistry", "SchemaViolation"]


class SchemaViolation(ValueError):
    """An observation does not conform to the registered schema."""


class SchemaRegistry:
    """Registry of signal types -> payload classes, plus envelope validation."""

    version = SCHEMA_VERSION

    def __init__(self) -> None:
        self._payload_types: dict[str, type[Payload]] = dict(STANDARD_PAYLOAD_TYPES)

    def signal_types(self) -> Iterable[str]:
        return sorted(self._payload_types)

    def register_extension(self, signal_type: str, payload_type: type[Payload]) -> None:
        """Register an adapter-specific extension signal type (spec §4.1)."""
        if signal_type in self._payload_types:
            raise SchemaViolation(f"signal type {signal_type!r} is already registered")
        self._payload_types[signal_type] = payload_type

    def validate(self, observation: Observation) -> Observation:
        """Check the envelope against the registry.

        Known signal types must carry their registered payload class; unknown
        types are preserved as :class:`GenericPayload`, never dropped.
        """
        expected = self._payload_types.get(observation.signal_type)
        if expected is not None and not isinstance(observation.payload, expected):
            raise SchemaViolation(
                f"signal type {observation.signal_type!r} requires payload "
                f"{expected.__name__}, got {type(observation.payload).__name__}"
            )
        if expected is None and not isinstance(observation.payload, GenericPayload):
            raise SchemaViolation(
                f"unknown signal type {observation.signal_type!r} must use GenericPayload "
                "(unknown types are preserved, not repurposed)"
            )
        return observation
