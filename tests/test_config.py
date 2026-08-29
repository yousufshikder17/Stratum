"""The single research TOML config (spec §8 MVP item 1)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from stratum.config import ConfigError, ResearchConfig

MINIMAL = """
[store]
path = "data/stratum.duckdb"

[[adapters]]
id = "market_csv"
  [adapters.config]
  bars_path = "data/bars.csv"
"""


def write(tmp_path: Path, text: str, *, name: str = "research.toml") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_paths_resolve_against_the_config_file(tmp_path: Path) -> None:
    """A config plus its data is a movable bundle; launching from elsewhere
    must not silently read a different dataset."""
    nested = tmp_path / "project"
    nested.mkdir()
    config = ResearchConfig.load(write(nested, MINIMAL))
    assert config.store_path == nested / "data" / "stratum.duckdb"
    assert config.adapters[0].settings["bars_path"] == str(nested / "data" / "bars.csv")


def test_absolute_paths_are_left_alone(tmp_path: Path) -> None:
    absolute = (tmp_path / "elsewhere" / "bars.csv").as_posix()
    config = ResearchConfig.load(
        write(
            tmp_path,
            f"""
[store]
path = "{(tmp_path / "db.duckdb").as_posix()}"

[[adapters]]
id = "market_csv"
  [adapters.config]
  bars_path = "{absolute}"
""",
        )
    )
    assert config.adapters[0].settings["bars_path"] == str(Path(absolute))


def test_non_path_settings_are_untouched(tmp_path: Path) -> None:
    config = ResearchConfig.load(
        write(
            tmp_path,
            MINIMAL + '  session_close_utc = "16:30"\n  data_class = "public_agg"\n',
        )
    )
    settings = config.adapters[0].settings
    assert settings["session_close_utc"] == "16:30"
    assert settings["data_class"] == "public_agg"


def test_defaults(tmp_path: Path) -> None:
    config = ResearchConfig.load(write(tmp_path, MINIMAL))
    assert config.run_id is None
    assert config.seed == 0
    assert config.backfill is None
    assert config.resolver.tables is None
    assert config.resolver.floor == 0.0
    assert config.adapters[0].entity_kind == "ticker"


def test_backfill_window(tmp_path: Path) -> None:
    config = ResearchConfig.load(
        write(tmp_path, MINIMAL + '\n[backfill]\nstart = "2024-01-01"\nend = "2024-03-01"\n')
    )
    assert config.backfill is not None
    assert config.backfill.start == date(2024, 1, 1)
    start, end = config.backfill.as_datetimes()
    assert start.tzinfo is not None and end.tzinfo is not None


def test_inverted_window_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="start < end"):
        ResearchConfig.load(
            write(tmp_path, MINIMAL + '\n[backfill]\nstart = "2024-03-01"\nend = "2024-01-01"\n')
        )


def test_half_specified_window_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="both"):
        ResearchConfig.load(write(tmp_path, MINIMAL + '\n[backfill]\nstart = "2024-01-01"\n'))


def test_resolver_section(tmp_path: Path) -> None:
    config = ResearchConfig.load(
        write(
            tmp_path,
            MINIMAL + '\n[resolver]\ntables = "data/entities.csv"\nversion = "v2"\nfloor = 0.5\n',
        )
    )
    assert config.resolver.tables == tmp_path / "data" / "entities.csv"
    assert config.resolver.version == "v2"
    assert config.resolver.floor == 0.5


def test_out_of_range_floor_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="floor"):
        ResearchConfig.load(write(tmp_path, MINIMAL + "\n[resolver]\nfloor = 1.5\n"))


def test_missing_store_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="store"):
        ResearchConfig.load(write(tmp_path, '[[adapters]]\nid = "market_csv"\n'))


def test_no_adapters_is_rejected(tmp_path: Path) -> None:
    """A run with no adapters writes nothing; failing beats a clean no-op."""
    with pytest.raises(ConfigError, match="at least one"):
        ResearchConfig.load(write(tmp_path, '[store]\npath = "s.duckdb"\n'))


def test_adapter_without_id_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="missing 'id'"):
        ResearchConfig.load(
            write(tmp_path, '[store]\npath = "s.duckdb"\n\n[[adapters]]\nentity_kind = "ticker"\n')
        )


def test_missing_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        ResearchConfig.load(tmp_path / "nope.toml")


def test_rate_limits(tmp_path: Path) -> None:
    config = ResearchConfig.load(
        write(
            tmp_path,
            MINIMAL + "\n[rate_limits.edgar]\nrequests_per_second = 8.0\nburst = 16\n",
        )
    )
    limit = config.rate_limits["edgar"]
    assert limit.requests_per_second == 8.0
    assert limit.capacity == 16


def test_rate_limit_without_a_rate_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="requests_per_second"):
        ResearchConfig.load(write(tmp_path, MINIMAL + "\n[rate_limits.edgar]\nburst = 4\n"))


def test_zero_rate_is_rejected(tmp_path: Path) -> None:
    """Zero would mean "admit nothing"; unlimited is the absence of an entry."""
    with pytest.raises(ConfigError, match="must be positive"):
        ResearchConfig.load(
            write(tmp_path, MINIMAL + "\n[rate_limits.edgar]\nrequests_per_second = 0\n")
        )


def test_provider_defaults_to_the_adapter_id(tmp_path: Path) -> None:
    config = ResearchConfig.load(write(tmp_path, MINIMAL))
    assert config.adapters[0].provider_key == "market_csv"


def test_adapters_can_share_a_provider(tmp_path: Path) -> None:
    config = ResearchConfig.load(
        write(
            tmp_path,
            MINIMAL + '\n[[adapters]]\nid = "universe_csv"\nprovider = "market_csv"\n',
        )
    )
    assert {a.provider_key for a in config.adapters} == {"market_csv"}
