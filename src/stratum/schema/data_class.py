"""Data classification & licensing (spec §4.7).

``DataClass`` + ``license_tag`` travel on every observation so exporters can
enforce "don't redistribute licensed raw data" and "strip/anonymize public
text PII" as a policy filter at export time, not a schema migration.
Classification rides every record; export-policy enforcement is not
implemented yet.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["DataClass"]


class DataClass(IntEnum):
    """Mirrors the ``DataClass`` protobuf enum (spec §4.7)."""

    #: Aggregated public statistics (Trends interest, mention counts).
    PUBLIC_AGG = 0
    #: Public user content (posts) — may carry PII; export policy may strip.
    PUBLIC_TEXT = 1
    #: Provider-licensed data (satellite, paid feeds) — no resale, ever.
    LICENSED = 2
