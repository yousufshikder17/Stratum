"""Adapter manifests (spec §3.1).

Each adapter ships a static TOML manifest declaring identity, family, schema
version spoken, config schema, isolation needs, the data license/terms tag,
and — critically — the ``[pit]`` section: how the adapter establishes
``knowledge_time``. The leakage guard (spec §3.6) enforces ingest-time rules
directly from this declaration, so an adapter cannot quietly claim PIT
semantics it does not deliver.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

__all__ = [
    "AdapterFamily",
    "AdapterManifest",
    "Isolation",
    "KnowledgeTimeBasis",
    "ManifestError",
    "PitDeclaration",
    "load_manifest",
    "parse_manifest",
]


class ManifestError(ValueError):
    """A manifest is missing, malformed, or declares impossible semantics."""


class AdapterFamily(StrEnum):
    SOURCE = "source"
    MARKET_DATA = "market_data"
    UNIVERSE = "universe"
    COST_MODEL = "cost_model"
    OUTPUT = "output"


class Isolation(StrEnum):
    INPROCESS = "inprocess"
    SUBPROCESS = "subprocess"  # e.g. heavy geospatial/ML wheels (spec §2.4)


class KnowledgeTimeBasis(StrEnum):
    """How an adapter establishes ``knowledge_time`` (spec §3.1, §3.6).

    - ``INGESTION``: knowledge_time = when we pulled it. Honest for forward
      ``poll()``; for historical ``backfill()`` the guard rejects rows that
      claim a 2019 value was knowable the moment it was pulled years later.
    - ``PUBLICATION``: a source-provided publication stamp (EDGAR acceptance
      datetime, post created_utc).
    - ``PROVIDER_VINTAGE``: the provider supplies its own observation or
      processing vintage (Trends request vintage, satellite processing run);
      each row must carry that stamp (``vintage_id``), or ingest fails.
    """

    INGESTION = "ingestion"
    PUBLICATION = "publication"
    PROVIDER_VINTAGE = "provider_vintage"


@dataclass(frozen=True, kw_only=True, slots=True)
class PitDeclaration:
    knowledge_time_basis: KnowledgeTimeBasis
    supports_restatement: bool = False
    #: Documented typical publication lag, for honest defaults (spec §3.1).
    publication_lag_p50_s: int = 0


@dataclass(frozen=True, kw_only=True, slots=True)
class AdapterManifest:
    id: str
    family: AdapterFamily
    version: str
    #: Major version of the common signal schema this adapter speaks.
    schema_version: str
    isolation: Isolation
    config_schema: str  # relative path to a JSON Schema file
    license_tag: str  # provenance/terms tracked per observation (spec §4.7)
    frequency: str  # native cadence, e.g. "intraday->daily"
    #: Does this adapter reach a remote provider? Declared so the core can
    #: refuse to run it without a configured rate limit (spec §3.2 rule 5) —
    #: forgetting to throttle EDGAR should fail at startup, not at the
    #: provider's discretion. Defaults false, so file-backed adapters and
    #: existing manifests need no change.
    network: bool = False
    pit: PitDeclaration


def parse_manifest(data: dict[str, Any], *, origin: str = "<memory>") -> AdapterManifest:
    try:
        adapter = data["adapter"]
        pit = data["pit"]
        return AdapterManifest(
            id=adapter["id"],
            family=AdapterFamily(adapter["family"]),
            version=adapter["version"],
            schema_version=str(adapter["schema_version"]),
            isolation=Isolation(adapter.get("isolation", "inprocess")),
            config_schema=adapter["config_schema"],
            license_tag=adapter["license_tag"],
            frequency=adapter.get("frequency", ""),
            network=bool(adapter.get("network", False)),
            pit=PitDeclaration(
                knowledge_time_basis=KnowledgeTimeBasis(pit["knowledge_time_basis"]),
                supports_restatement=bool(pit.get("supports_restatement", False)),
                publication_lag_p50_s=int(pit.get("publication_lag_p50_s", 0)),
            ),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise ManifestError(f"invalid adapter manifest at {origin}: {exc}") from exc


def load_manifest(path: Path) -> AdapterManifest:
    """Load and validate a ``manifest.toml``. Importing the adapter's code is
    not required — the core can list/validate adapters statically (spec §3.1)."""
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return parse_manifest(data, origin=str(path))
