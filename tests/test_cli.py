"""The research CLI end to end (spec §2.4).

The CLI is the acceptance surface: if `ingest` cannot fill a store that
`store`, `universe`, and `snapshot` can then read, the pipeline is not real.
These tests run the actual Typer app against real files.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

from typer.testing import CliRunner

from stratum.cli import app
from tests.conftest import write_csv

runner = CliRunner()

BARS_HEADER = ["security", "date", "open", "high", "low", "close", "volume"]
BARS = [
    ["AAA", "2024-01-02", "10.0", "10.5", "9.8", "10.2", "1000"],
    ["ZZZ", "2024-01-02", "3.0", "3.1", "2.7", "2.8", "5000"],
]
MEMBERSHIP_HEADER = ["security", "added_on", "announced_on", "removed_on", "removed_announced_on"]
MEMBERSHIP = [
    ["AAA", "2020-01-02", "2019-12-20", "", ""],
    ["ZZZ", "2019-06-03", "2019-05-28", "2024-02-16", "2024-02-09"],
]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    write_csv(tmp_path / "bars.csv", BARS_HEADER, BARS)
    write_csv(tmp_path / "membership.csv", MEMBERSHIP_HEADER, MEMBERSHIP)
    write_csv(
        tmp_path / "entities.csv",
        ["kind", "value", "security_id", "valid_from"],
        [["ticker", "AAA", "SEC-1", "2015-01-02"], ["ticker", "ZZZ", "SEC-3", "2019-06-03"]],
    )
    config = tmp_path / "research.toml"
    config.write_text(
        """
[run]
id = "cli-test"

[store]
path = "stratum.duckdb"

[resolver]
tables = "entities.csv"

[backfill]
start = "2024-01-01"
end = "2024-03-01"

[[adapters]]
id = "market_csv"
  [adapters.config]
  bars_path = "bars.csv"

[[adapters]]
id = "universe_csv"
  [adapters.config]
  membership_path = "membership.csv"
  universe_id = "sp1500_pit"
""",
        encoding="utf-8",
    )
    return config


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout.startswith("stratum ")


def test_adapters_lists_bundled_plugins() -> None:
    """Every bundled adapter is a plugin discovered through entry points —
    no specials (spec §8 MVP item 10)."""
    result = runner.invoke(app, ["adapters"])
    assert result.exit_code == 0
    assert "market_csv" in result.stdout
    assert "universe_csv" in result.stdout
    assert "pit=publication" in result.stdout


def test_ingest_then_store(project: Path) -> None:
    ingested = runner.invoke(app, ["ingest", "--config", str(project), "--backfill"])
    assert ingested.exit_code == 0, ingested.stdout
    assert "rejected=0" in ingested.stdout

    described = runner.invoke(app, ["store", "--config", str(project)])
    assert described.exit_code == 0
    assert "market.bar" in described.stdout
    assert "resolved=100%" in described.stdout


def test_ingest_is_idempotent_across_invocations(project: Path) -> None:
    runner.invoke(app, ["ingest", "--config", str(project), "--backfill"])
    second = runner.invoke(app, ["ingest", "--config", str(project), "--backfill"])
    assert second.exit_code == 0
    assert "total: 0 written" in second.stdout


def test_store_on_an_empty_database(project: Path) -> None:
    result = runner.invoke(app, ["store", "--config", str(project)])
    assert result.exit_code == 0
    assert "empty store" in result.stdout


def test_universe_is_survivorship_correct(project: Path) -> None:
    """The check a reviewer can run by hand: a name that dies later is still
    in the index before it dies."""
    before = runner.invoke(
        app,
        ["universe", "--config", str(project), "--universe", "sp1500_pit", "--as-of", "2024-02-01"],
    )
    assert before.exit_code == 0
    assert "2 members" in before.stdout
    assert "ZZZ" in before.stdout

    after = runner.invoke(
        app,
        ["universe", "--config", str(project), "--universe", "sp1500_pit", "--as-of", "2024-02-20"],
    )
    assert after.exit_code == 0
    assert "1 members" in after.stdout
    assert "ZZZ" not in after.stdout


def test_universe_rejects_an_unconfigured_id(project: Path) -> None:
    result = runner.invoke(
        app,
        ["universe", "--config", str(project), "--universe", "nope", "--as-of", "2024-02-01"],
    )
    assert result.exit_code == 2


def test_snapshot_is_content_addressed(project: Path) -> None:
    runner.invoke(app, ["ingest", "--config", str(project), "--backfill"])
    out = project.parent / "snap"
    result = runner.invoke(
        app,
        [
            "snapshot",
            "--config",
            str(project),
            "--as-of",
            "2024-03-01T00:00:00Z",
            "--out",
            str(out),
        ],
    )
    assert result.exit_code == 0
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["content_hash"] in result.stdout
    assert (out / "observations.parquet").exists()
    assert manifest["row_count"] > 0


def test_snapshot_push_is_honest_about_being_unavailable(project: Path) -> None:
    runner.invoke(app, ["ingest", "--config", str(project), "--backfill"])
    result = runner.invoke(
        app,
        [
            "snapshot",
            "--config",
            str(project),
            "--as-of",
            "2024-03-01T00:00:00Z",
            "--out",
            str(project.parent / "snap2"),
            "--push",
            "https://example.invalid",
        ],
    )
    assert result.exit_code == 2
    assert "push unavailable" in result.stdout


def test_backtest_is_still_a_scaffold() -> None:
    """Exiting 2 beats printing a plausible number the engine cannot back up."""
    result = runner.invoke(app, ["backtest", "--factor", "x", "--universe", "y"])
    assert result.exit_code == 2
    assert "not implemented" in result.stdout


def test_naive_as_of_is_read_as_utc(project: Path) -> None:
    runner.invoke(app, ["ingest", "--config", str(project), "--backfill"])
    result = runner.invoke(
        app,
        [
            "snapshot",
            "--config",
            str(project),
            "--as-of",
            "2024-03-01T00:00:00",
            "--out",
            str(project.parent / "snap3"),
        ],
    )
    assert result.exit_code == 0
    assert "2024-03-01T00:00:00+00:00" in result.stdout
