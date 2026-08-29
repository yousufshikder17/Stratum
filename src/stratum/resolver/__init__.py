"""Point-in-time entity resolution (spec §4.5)."""

from stratum.resolver.entity import (
    Candidate,
    EntityResolver,
    MappingRow,
    Resolution,
    normalize,
)

__all__ = ["Candidate", "EntityResolver", "MappingRow", "Resolution", "normalize"]
