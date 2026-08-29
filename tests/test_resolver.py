"""Point-in-time entity resolution (spec §4.5): identity changes over time."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from stratum.adapters.base import EntityHint
from stratum.adapters.csv_common import CsvFormatError
from stratum.resolver import Candidate, EntityResolver, MappingRow, Resolution, normalize
from stratum.schema.times import AsOf
from tests.conftest import write_csv


def as_of(day: str) -> AsOf:
    return AsOf.at(datetime.fromisoformat(day).replace(tzinfo=UTC))


#: The ZZZ ticker belongs to one issuer until it delists, then is reused by an
#: unrelated company. Resolving it without an as_of merges two histories.
REUSED_TICKER = [
    MappingRow(
        kind="ticker",
        value="ZZZ",
        security_id="SEC-OLD",
        valid_from=date(2019, 6, 3),
        valid_to=date(2024, 2, 16),
        known_from=date(2019, 5, 28),
    ),
    MappingRow(
        kind="ticker",
        value="ZZZ",
        security_id="SEC-NEW",
        valid_from=date(2024, 9, 2),
        known_from=date(2024, 8, 19),
    ),
]


# -- the bitemporal lookup ----------------------------------------------------


def test_ticker_reuse_resolves_by_date() -> None:
    resolver = EntityResolver(REUSED_TICKER)
    assert resolver.resolve_native("ticker", "ZZZ", as_of=as_of("2020-01-01")) == "SEC-OLD"
    assert resolver.resolve_native("ticker", "ZZZ", as_of=as_of("2025-01-01")) == "SEC-NEW"


def test_gap_between_issuers_resolves_to_nobody() -> None:
    """Between the delisting and the reuse the ticker referred to nothing.
    Saying so beats picking whichever row sorts first."""
    resolver = EntityResolver(REUSED_TICKER)
    assert resolver.resolve_native("ticker", "ZZZ", as_of=as_of("2024-05-01")) is None


def test_mapping_is_invisible_before_we_learned_it() -> None:
    """known_from is the knowledge axis: back-dating a symbol change we only
    learned last week would re-attribute years of history."""
    resolver = EntityResolver(
        [
            MappingRow(
                kind="ticker",
                value="AAA",
                security_id="SEC-1",
                valid_from=date(2020, 1, 1),
                known_from=date(2024, 1, 1),
            )
        ]
    )
    assert resolver.resolve_native("ticker", "AAA", as_of=as_of("2022-06-01")) is None
    assert resolver.resolve_native("ticker", "AAA", as_of=as_of("2024-06-01")) == "SEC-1"


def test_known_from_defaults_to_valid_from() -> None:
    row = MappingRow(kind="ticker", value="AAA", security_id="SEC-1", valid_from=date(2020, 1, 1))
    assert row.effective_known_from == date(2020, 1, 1)


def test_validity_is_half_open() -> None:
    resolver = EntityResolver(REUSED_TICKER)
    assert resolver.resolve_native("ticker", "ZZZ", as_of=as_of("2024-02-15")) == "SEC-OLD"
    assert resolver.resolve_native("ticker", "ZZZ", as_of=as_of("2024-02-16")) is None


# -- ambiguity ----------------------------------------------------------------


def test_ambiguity_is_preserved_not_forced() -> None:
    """Two issuers plausibly match a cashtag. The resolver reports both."""
    resolver = EntityResolver(
        [
            MappingRow(
                kind="cashtag",
                value="$X",
                security_id="SEC-1",
                valid_from=date(2020, 1, 1),
                confidence=0.6,
            ),
            MappingRow(
                kind="cashtag",
                value="$X",
                security_id="SEC-2",
                valid_from=date(2020, 1, 1),
                confidence=0.4,
            ),
        ]
    )
    resolution = resolver.resolve(EntityHint(kind="cashtag", value="$X"), as_of=as_of("2024-01-01"))
    assert resolution.ambiguous
    assert [c.security_id for c in resolution.candidates] == ["SEC-1", "SEC-2"]
    assert resolution.best == Candidate(security_id="SEC-1", confidence=0.6)


def test_confidence_floor_filters_thin_matches() -> None:
    resolver = EntityResolver(
        [
            MappingRow(
                kind="cashtag",
                value="$X",
                security_id="SEC-1",
                valid_from=date(2020, 1, 1),
                confidence=0.3,
            )
        ]
    )
    scope = as_of("2024-01-01")
    assert resolver.resolve_native("cashtag", "$X", as_of=scope) == "SEC-1"
    assert resolver.resolve_native("cashtag", "$X", as_of=scope, floor=0.5) is None


def test_equal_confidence_resolves_deterministically() -> None:
    rows = [
        MappingRow(
            kind="ticker", value="Q", security_id=sid, valid_from=date(2020, 1, 1), confidence=0.5
        )
        for sid in ("SEC-B", "SEC-A")
    ]
    resolver = EntityResolver(rows)
    for _ in range(3):
        resolution = resolver.resolve(
            EntityHint(kind="ticker", value="Q"), as_of=as_of("2024-01-01")
        )
        assert [c.security_id for c in resolution.candidates] == ["SEC-A", "SEC-B"]


def test_unknown_key_resolves_to_nothing() -> None:
    resolver = EntityResolver(REUSED_TICKER)
    resolution = resolver.resolve(
        EntityHint(kind="ticker", value="NOPE"), as_of=as_of("2024-01-01")
    )
    assert resolution.candidates == ()
    assert resolution.best is None
    assert not resolution.ambiguous


# -- normalization ------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "value", "expected"),
    [
        ("cashtag", "$aapl", "AAPL"),
        ("cashtag", " $AAPL ", "AAPL"),
        ("ticker", "aapl", "AAPL"),
        ("cik", "320193", "0000320193"),
        ("cik", "0000320193", "0000320193"),
        ("company_name", "  Apple   Inc. ", "apple inc."),
        ("figi", "BBG000B9XRY4", "BBG000B9XRY4"),
    ],
)
def test_normalize(kind: str, value: str, expected: str) -> None:
    assert normalize(kind, value) == expected


def test_cashtag_and_ticker_are_separate_namespaces() -> None:
    """A cashtag is noisier than a ticker; they must not silently share rows."""
    resolver = EntityResolver(
        [MappingRow(kind="ticker", value="AAA", security_id="SEC-1", valid_from=date(2020, 1, 1))]
    )
    scope = as_of("2024-01-01")
    assert resolver.resolve_native("ticker", "AAA", as_of=scope) == "SEC-1"
    assert resolver.resolve_native("cashtag", "$AAA", as_of=scope) is None


# -- tables -------------------------------------------------------------------


def test_load_from_csv(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path / "entities.csv",
        ["kind", "value", "security_id", "valid_from", "valid_to", "known_from", "confidence"],
        [
            ["ticker", "AAA", "SEC-1", "2015-01-02", "", "2015-01-02", "1.0"],
            ["cashtag", "AAA", "SEC-1", "2015-01-02", "", "2015-01-02", "0.8"],
        ],
    )
    resolver = EntityResolver.from_csv(path)
    assert len(resolver) == 2
    assert resolver.tables_version == "entities"
    assert resolver.resolve_native("cashtag", "$aaa", as_of=as_of("2024-01-01")) == "SEC-1"


def test_tables_version_is_overridable(tmp_path: Path) -> None:
    """The version travels into run manifests, so a resolution is reproducible."""
    path = write_csv(
        tmp_path / "entities.csv",
        ["kind", "value", "security_id", "valid_from"],
        [["ticker", "AAA", "SEC-1", "2015-01-02"]],
    )
    assert EntityResolver.from_csv(path, tables_version="v3").tables_version == "v3"


def test_empty_security_id_is_rejected() -> None:
    with pytest.raises(CsvFormatError, match="security_id"):
        MappingRow(kind="ticker", value="AAA", security_id="", valid_from=date(2020, 1, 1))


def test_inverted_validity_is_rejected() -> None:
    with pytest.raises(CsvFormatError, match="valid_to"):
        MappingRow(
            kind="ticker",
            value="AAA",
            security_id="SEC-1",
            valid_from=date(2020, 1, 1),
            valid_to=date(2019, 1, 1),
        )


def test_confidence_out_of_range_is_rejected() -> None:
    with pytest.raises(CsvFormatError, match="confidence"):
        MappingRow(
            kind="ticker",
            value="AAA",
            security_id="SEC-1",
            valid_from=date(2020, 1, 1),
            confidence=1.5,
        )


def test_resolution_keeps_the_hint() -> None:
    hint = EntityHint(kind="ticker", value="AAA", context="from a filing")
    resolution = Resolution(hint=hint, candidates=())
    assert resolution.hint.context == "from a filing"
