"""The pre-alpha research CLI.

Subcommands map to pipeline stages: ``stratum ingest / store / factor /
backtest / report / snapshot``. Ingestion may touch the network; research
commands read only from the immutable PIT store snapshot named in the run
manifest — the structural guarantee that a backtest cannot accidentally fetch
future data (spec §2.4).
"""

from __future__ import annotations

from pathlib import Path

import typer

from stratum import __version__

app = typer.Typer(
    name="stratum",
    help=(
        "Pre-alpha point-in-time data and signal research tooling. "
        "Research tooling — outputs are hypotheses and diagnostics, not "
        "investment advice."
    ),
    no_args_is_help=True,
)

_NOT_IMPLEMENTED = "Scaffold: this command is not implemented."


@app.callback()
def _main(
    version: bool = typer.Option(False, "--version", help="Print version and exit."),
) -> None:
    if version:
        typer.echo(f"stratum {__version__}")
        raise typer.Exit()


@app.command()
def ingest(
    config: Path = typer.Option(..., "--config", help="Research TOML config file."),
    adapter: str | None = typer.Option(None, help="Restrict to one adapter id."),
    backfill: bool = typer.Option(False, help="Historical backfill instead of forward poll."),
) -> None:
    """Scaffold for planned guarded ingestion; not implemented."""
    typer.echo(_NOT_IMPLEMENTED)
    raise typer.Exit(code=2)


@app.command()
def store(
    config: Path = typer.Option(..., "--config"),
) -> None:
    """Scaffold for planned store inspection; not implemented."""
    typer.echo(_NOT_IMPLEMENTED)
    raise typer.Exit(code=2)


@app.command()
def factor(
    definition: Path = typer.Argument(..., help="Path to a factor YAML definition."),
    validate_only: bool = typer.Option(False, help="Parse + PIT-check without building."),
) -> None:
    """Validate a factor definition; factor computation is not implemented."""
    from stratum.factors.definition import load_factor

    fd = load_factor(definition)
    typer.echo(f"OK: {fd.id} v{fd.version} family={fd.family.value} embargo={fd.pit.embargo}")
    if not validate_only:
        typer.echo(_NOT_IMPLEMENTED)
        raise typer.Exit(code=2)


@app.command()
def backtest(
    factor: str = typer.Option(..., "--factor", help="Factor id (from the library)."),
    universe: str = typer.Option(..., "--universe", help="PIT universe id, e.g. sp1500_pit."),
    costs: str = typer.Option("default", "--costs", help="Cost model id."),
    as_of_embargo: str = typer.Option("1d", "--as-of-embargo", help="Decision lag."),
    snapshot: str | None = typer.Option(None, "--snapshot", help="Snapshot hash to pin."),
    seed: int = typer.Option(0, "--seed"),
) -> None:
    """Scaffold for planned bias-controlled backtesting; not implemented."""
    typer.echo(_NOT_IMPLEMENTED)
    raise typer.Exit(code=2)


@app.command()
def report(
    run_id: str = typer.Argument(..., help="Run id or manifest hash."),
    fmt: str = typer.Option("markdown", "--format", help="markdown | html"),
) -> None:
    """Scaffold for planned diagnostics rendering; not implemented."""
    typer.echo(_NOT_IMPLEMENTED)
    raise typer.Exit(code=2)


@app.command()
def snapshot(
    config: Path = typer.Option(..., "--config"),
    as_of: str = typer.Option(..., "--as-of", help="Knowledge-axis timestamp (ISO, UTC)."),
    push: str | None = typer.Option(None, "--push", help="Endpoint to push the snapshot to."),
) -> None:
    """Scaffold for planned snapshot materialization; not implemented."""
    typer.echo(_NOT_IMPLEMENTED)
    raise typer.Exit(code=2)


@app.command()
def adapters() -> None:
    """List installed adapter entry points and validated manifests."""
    from stratum.adapters.discovery import discover

    for found in discover():
        if found.manifest is None:
            typer.echo(f"{found.name:<20} INVALID: {found.error}")
        else:
            m = found.manifest
            typer.echo(
                f"{found.name:<20} family={m.family.value:<12} v{m.version} "
                f"pit={m.pit.knowledge_time_basis.value} "
                f"restatement={'yes' if m.pit.supports_restatement else 'no'}"
            )


def main() -> None:
    app()
