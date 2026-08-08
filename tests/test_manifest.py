"""Every bundled adapter ships a valid static manifest (spec §3.1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from stratum.adapters.manifest import (
    AdapterFamily,
    KnowledgeTimeBasis,
    ManifestError,
    load_manifest,
    parse_manifest,
)

ADAPTERS_DIR = Path(__file__).resolve().parents[1] / "src" / "stratum" / "adapters"
BUNDLED = sorted(p for p in ADAPTERS_DIR.iterdir() if (p / "manifest.toml").is_file())


def test_all_bundled_adapters_have_manifests() -> None:
    names = {p.name for p in BUNDLED}
    assert {
        "reddit_sentiment",
        "sec_edgar",
        "google_trends",
        "market_csv",
        "universe_csv",
        "cost_default",
        "parquet_panel",
    } <= names


@pytest.mark.parametrize("pkg", BUNDLED, ids=lambda p: p.name)
def test_bundled_manifest_parses(pkg: Path) -> None:
    manifest = load_manifest(pkg / "manifest.toml")
    assert manifest.id == pkg.name
    assert manifest.schema_version == "1"
    assert (pkg / manifest.config_schema).is_file()


def test_edgar_is_the_reference_restatement_source() -> None:
    manifest = load_manifest(ADAPTERS_DIR / "sec_edgar" / "manifest.toml")
    assert manifest.family is AdapterFamily.SOURCE
    assert manifest.pit.knowledge_time_basis is KnowledgeTimeBasis.PUBLICATION
    assert manifest.pit.supports_restatement is True


def test_trends_requires_provider_vintage() -> None:
    manifest = load_manifest(ADAPTERS_DIR / "google_trends" / "manifest.toml")
    assert manifest.pit.knowledge_time_basis is KnowledgeTimeBasis.PROVIDER_VINTAGE
    assert manifest.pit.supports_restatement is True


def test_malformed_manifest_is_a_manifest_error() -> None:
    with pytest.raises(ManifestError):
        parse_manifest({"adapter": {"id": "x"}}, origin="<test>")
    with pytest.raises(ManifestError):
        parse_manifest(
            {
                "adapter": {
                    "id": "x",
                    "family": "not_a_family",
                    "version": "0",
                    "schema_version": "1",
                    "config_schema": "c.json",
                    "license_tag": "t",
                },
                "pit": {"knowledge_time_basis": "ingestion"},
            },
            origin="<test>",
        )
