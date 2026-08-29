"""Point-in-time entity resolution (spec §4.5) — resolver v0, MVP item 6.

Maps native keys (ticker, cashtag, CIK, search term) to a canonical internal
``security_id``. The mapping is itself **bitemporal**, because identity
changes: tickers get reused, symbols change, companies merge. The question is
never "what is ``$X``?" but "what did ``$X`` refer to *as of* that date?" — so
:meth:`EntityResolver.resolve` requires an ``as_of`` with no default, exactly
like every store read.

Each mapping row carries both axes:

===============  ==========================================================
``valid_from``   event axis — when the mapping became true in the world
``valid_to``     event axis — when it stopped (``None`` = still true)
``known_from``   knowledge axis — when *we* learned it
===============  ==========================================================

A row is a candidate only when ``known_from <= as_of`` **and**
``valid_from <= as_of < valid_to``. The knowledge axis matters as much here as
in the store: back-dating a symbol change we only learned about last week
would silently re-attribute years of history.

**Ambiguity is preserved, not forced.** ``$AVGO`` on a message board may be
two different issuers across a merger; the resolver returns every candidate
with a confidence rather than picking one and hiding the doubt. Callers
decide: :attr:`Resolution.best` takes the highest-confidence candidate,
:meth:`Resolution.confident` applies a floor, and low-confidence social
mentions can be filtered downstream (spec §4.5).

Tables are **versioned data, shipped and updatable, never hardcoded**
(:meth:`EntityResolver.from_csv`). ``tables_version`` travels into run
manifests so a resolution is reproducible.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from stratum.adapters.base import EntityHint
from stratum.adapters.csv_common import (
    CsvFormatError,
    optional_date,
    optional_float,
    read_csv,
    require_date,
)
from stratum.schema.times import AsOf

__all__ = [
    "Candidate",
    "EntityResolver",
    "MappingRow",
    "Resolution",
    "normalize",
]


def normalize(kind: str, value: str) -> str:
    """Canonical lookup form for a native key.

    Only the mechanical differences are normalized — a cashtag's ``$``, case,
    surrounding space, a CIK's leading zeros. Nothing here guesses at
    identity; that is what the tables are for.
    """
    text = value.strip()
    kind = kind.strip().lower()
    if kind == "cashtag":
        text = text.lstrip("$")
    if kind in {"cashtag", "ticker"}:
        return text.upper()
    if kind == "cik":
        digits = text.lstrip("0")
        return digits.zfill(10) if digits else text
    if kind in {"company_name", "search_term"}:
        return " ".join(text.lower().split())
    return text


@dataclass(frozen=True, kw_only=True, slots=True)
class Candidate:
    security_id: str
    confidence: float  # 0..1


@dataclass(frozen=True, kw_only=True, slots=True)
class Resolution:
    hint: EntityHint
    candidates: Sequence[Candidate]

    @property
    def best(self) -> Candidate | None:
        return max(self.candidates, key=lambda c: c.confidence, default=None)

    @property
    def ambiguous(self) -> bool:
        """More than one candidate was in force — the doubt is real, not noise."""
        return len(self.candidates) > 1

    def confident(self, floor: float) -> Candidate | None:
        """The best candidate, but only if it clears ``floor``.

        The filter for thin social mentions: a 0.3-confidence cashtag match
        should not silently become a position (spec §4.5).
        """
        best = self.best
        return best if best is not None and best.confidence >= floor else None


@dataclass(frozen=True, kw_only=True, slots=True)
class MappingRow:
    """One bitemporal ``native key -> security_id`` assertion."""

    kind: str
    value: str
    security_id: str
    valid_from: date
    valid_to: date | None = None
    known_from: date | None = None
    confidence: float = 1.0

    def __post_init__(self) -> None:
        if not self.security_id:
            raise CsvFormatError("mapping row needs a non-empty security_id")
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise CsvFormatError(
                f"mapping {self.kind}:{self.value} -> {self.security_id} has valid_to "
                f"{self.valid_to} on or before valid_from {self.valid_from}"
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise CsvFormatError(f"confidence {self.confidence} is outside 0..1")

    @property
    def key(self) -> tuple[str, str]:
        return (self.kind.strip().lower(), normalize(self.kind, self.value))

    @property
    def effective_known_from(self) -> date:
        """When we learned the mapping. Defaults to when it became true —
        the honest reading for a mapping published as it happened."""
        return self.known_from if self.known_from is not None else self.valid_from

    def in_force(self, day: date) -> bool:
        """Was the mapping true on ``day``, and did we know it by then?"""
        if self.effective_known_from > day:
            return False
        if day < self.valid_from:
            return False
        return self.valid_to is None or day < self.valid_to


class EntityResolver:
    """Bitemporal lookup over versioned mapping tables.

    ``as_of`` is required because identity itself changes over time.
    """

    def __init__(self, rows: Iterable[MappingRow] = (), *, tables_version: str = "v0") -> None:
        self.tables_version = tables_version
        self._by_key: dict[tuple[str, str], list[MappingRow]] = {}
        for row in rows:
            self._by_key.setdefault(row.key, []).append(row)

    def __len__(self) -> int:
        return sum(len(rows) for rows in self._by_key.values())

    @classmethod
    def from_csv(cls, path: Path, *, tables_version: str | None = None) -> EntityResolver:
        """Load a mapping table.

        Columns: ``kind, value, security_id, valid_from`` required;
        ``valid_to, known_from, confidence`` optional. ``tables_version``
        defaults to the file's stem so the run manifest records *which* table
        produced a resolution.
        """
        rows = list(_read_mapping_rows(path))
        return cls(rows, tables_version=tables_version or path.stem)

    def resolve(self, hint: EntityHint, *, as_of: AsOf) -> Resolution:
        """Every mapping in force at ``as_of``, highest confidence first."""
        day = as_of.as_datetime().date()
        key = (hint.kind.strip().lower(), normalize(hint.kind, hint.value))
        matches = [row for row in self._by_key.get(key, ()) if row.in_force(day)]
        # Deterministic order: confidence desc, then security_id, so equal
        # confidences never resolve differently between runs.
        matches.sort(key=lambda row: (-row.confidence, row.security_id))
        return Resolution(
            hint=hint,
            candidates=tuple(
                Candidate(security_id=row.security_id, confidence=row.confidence) for row in matches
            ),
        )

    def resolve_native(
        self, kind: str, value: str, *, as_of: AsOf, floor: float = 0.0
    ) -> str | None:
        """Convenience for the ingest path: the confident ``security_id``, or
        ``None`` when nothing is in force or the match is too weak.

        ``None`` means *unresolved*, which the store records as a NULL
        ``security_id`` — an honest "we don't know who this is" rather than a
        guess that quietly pollutes a cross-section.
        """
        resolution = self.resolve(EntityHint(kind=kind, value=value), as_of=as_of)
        candidate = resolution.confident(floor)
        return candidate.security_id if candidate is not None else None


def _read_mapping_rows(path: Path) -> Iterator[MappingRow]:
    for row in read_csv(path, required=["kind", "value", "security_id", "valid_from"]):
        origin = f"{path.name}[{row.get('value', '?')}]"
        confidence = optional_float(row, "confidence", origin=origin)
        yield MappingRow(
            kind=row["kind"],
            value=row["value"],
            security_id=row["security_id"],
            valid_from=require_date(row, "valid_from", origin=origin),
            valid_to=optional_date(row, "valid_to", origin=origin),
            known_from=optional_date(row, "known_from", origin=origin),
            confidence=1.0 if confidence is None else confidence,
        )
