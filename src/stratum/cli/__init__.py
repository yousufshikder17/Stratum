"""The pre-alpha research CLI.

Subcommands map to pipeline stages: ``stratum ingest / store / factor /
backtest / report / snapshot``. Ingestion may touch the network; research
commands read only from the immutable PIT store snapshot named in the run
manifest — the structural guarantee that a backtest cannot accidentally fetch
future data (spec §2.4).

``ingest``, ``store``, ``snapshot``, ``factor``, and ``adapters`` are
implemented. ``backtest`` and ``report`` are still scaffolds:
the bias-controlled simulation loop (spec §6) belongs in Ledger, and
exiting 2 is a more honest answer than a plausible-looking number.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import typer

from stratum import __version__

if TYPE_CHECKING:
    from stratum.adapters.context import AdapterContext
    from stratum.config import ResearchConfig

app = typer.Typer(
    name="stratum",
    help=(
        "Pre-alpha point-in-time data and signal research tooling. "
        "Research tooling — outputs are hypotheses and diagnostics, not "
        "investment advice."
    ),
    no_args_is_help=True,
)

_LEDGER_BOUNDARY = "This workflow belongs to Ledger; pass it a pinned Stratum snapshot."


def _parse_as_of(text: str) -> datetime:
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError as exc:
        raise typer.BadParameter(f"{text!r} is not an ISO timestamp") from exc
    # A naive as_of is ambiguous, and an ambiguous knowledge cut is a silent
    # off-by-hours leak. Assume UTC explicitly and say so, rather than guess.
    return stamp if stamp.tzinfo is not None else stamp.replace(tzinfo=UTC)


def _version_callback(value: bool) -> None:
    """Eager, so ``stratum --version`` answers before Click's group insists on
    a subcommand (``no_args_is_help`` would otherwise turn it into an error)."""
    if value:
        typer.echo(f"stratum {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        False,
        "--version",
        help="Print version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    return None


@app.command()
def ingest(
    config: Path = typer.Option(..., "--config", help="Research TOML config file."),
    adapter: str | None = typer.Option(None, help="Restrict to one adapter id."),
    backfill: bool = typer.Option(False, help="Historical backfill instead of forward poll."),
) -> None:
    """Run configured adapters through the guarded path into the PIT store.

    Exits non-zero if the leakage guard rejected anything, so a CI job cannot
    treat a partially-rejected ingest as a success.
    """
    from stratum.config import ResearchConfig
    from stratum.guard.leakage import IngestMode
    from stratum.run.ingest import format_report, run_ingest

    research = ResearchConfig.load(config)
    mode = IngestMode.BACKFILL if backfill else IngestMode.POLL
    report = run_ingest(research, mode=mode, only=adapter)
    for line in format_report(report):
        typer.echo(line)
    if not report.clean:
        raise typer.Exit(code=1)


@app.command()
def store(
    config: Path = typer.Option(..., "--config"),
) -> None:
    """Summarize the PIT store: rows, entities, axis spans, resolution coverage."""
    from stratum.config import ResearchConfig
    from stratum.store.duckdb_store import DuckDBSignalStore
    from stratum.store.inspect import summarize

    research = ResearchConfig.load(config)
    signal_store = DuckDBSignalStore(research.store_path)
    try:
        for line in summarize(signal_store).lines():
            typer.echo(line)
    finally:
        signal_store.close()


@app.command()
def factor(
    definition: Path = typer.Argument(..., help="Path to a factor YAML definition."),
    validate_only: bool = typer.Option(False, help="Parse + PIT-check without building."),
    snapshot_dir: Path | None = typer.Option(None, "--snapshot", help="Pinned snapshot directory."),
    dates: str | None = typer.Option(None, "--dates", help="Comma-separated rebalance dates."),
    out: Path | None = typer.Option(None, "--out", help="Write JSON result to this path."),
) -> None:
    """Validate or build point-in-time factor exposures from a snapshot."""
    import json

    from stratum.factors.definition import load_factor
    from stratum.factors.diagnostics import summarize_exposures
    from stratum.factors.engine import FactorEngine
    from stratum.store.snapshot_store import SnapshotStore

    fd = load_factor(definition)
    typer.echo(f"OK: {fd.id} v{fd.version} family={fd.family.value} embargo={fd.pit.embargo}")
    if validate_only:
        return
    if snapshot_dir is None or not dates:
        raise typer.BadParameter(
            "--snapshot and --dates are required unless --validate-only is used"
        )
    try:
        rebalance_dates = sorted(
            {datetime.fromisoformat(item.strip()).date() for item in dates.split(",")}
        )
    except ValueError as exc:
        raise typer.BadParameter("--dates must contain ISO dates") from exc
    snapshot_store = SnapshotStore(snapshot_dir)
    try:
        panel = FactorEngine(
            store=snapshot_store,
            snapshot=snapshot_store.as_snapshot_ref(),
        ).build(fd, rebalance_dates=rebalance_dates)
    finally:
        snapshot_store.close()
    result = {
        "factor_id": panel.factor_id,
        "factor_version": panel.factor_version,
        "snapshot_hash": snapshot_store.content_hash,
        "build_hash": panel.build_hash,
        "diagnostics": summarize_exposures(panel),
        "exposures": {
            day.isoformat(): dict(sorted(values.items()))
            for day, values in sorted(panel.exposures.items())
        },
    }
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if out is None:
        typer.echo(rendered)
    else:
        out.write_text(rendered + "\n", encoding="utf-8")
        typer.echo(f"wrote {out}")


@app.command()
def backtest(
    factor: str = typer.Option(..., "--factor", help="Factor id (from the library)."),
    universe: str = typer.Option(..., "--universe", help="PIT universe id, e.g. sp1500_pit."),
    costs: str = typer.Option("default", "--costs", help="Cost model id."),
    as_of_embargo: str = typer.Option("1d", "--as-of-embargo", help="Decision lag."),
    snapshot: str | None = typer.Option(None, "--snapshot", help="Snapshot hash to pin."),
    seed: int = typer.Option(0, "--seed"),
) -> None:
    """Direct simulation work to the sibling Ledger project."""
    typer.echo(_LEDGER_BOUNDARY)
    raise typer.Exit(code=2)


@app.command()
def report(
    run_id: str = typer.Argument(..., help="Run id or manifest hash."),
    fmt: str = typer.Option("markdown", "--format", help="markdown | html"),
) -> None:
    """Direct return-linked reporting to the sibling Ledger project."""
    typer.echo(_LEDGER_BOUNDARY)
    raise typer.Exit(code=2)


@app.command()
def snapshot(
    config: Path = typer.Option(..., "--config"),
    as_of: str = typer.Option(..., "--as-of", help="Knowledge-axis timestamp (ISO, UTC)."),
    out: Path | None = typer.Option(None, "--out", help="Snapshot directory."),
    push: str | None = typer.Option(None, "--push", help="Endpoint to push the snapshot to."),
) -> None:
    """Materialize a content-addressed snapshot of the store at ``--as-of``."""
    from stratum.config import ResearchConfig
    from stratum.run.snapshot import create_snapshot, push_snapshot
    from stratum.schema.times import AsOf
    from stratum.store.duckdb_store import DuckDBSignalStore

    research = ResearchConfig.load(config)
    scope = AsOf.at(_parse_as_of(as_of))
    target = out or research.store_path.parent / "snapshots" / as_of.replace(":", "")
    signal_store = DuckDBSignalStore(research.store_path)
    try:
        ref = create_snapshot(signal_store, as_of=scope, target_dir=target)
    finally:
        signal_store.close()
    typer.echo(f"snapshot {ref.content_hash}")
    typer.echo(f"  as_of {scope.as_datetime().isoformat()}")
    typer.echo(f"  path  {ref.path}")
    if push is not None:
        try:
            push_snapshot(ref, endpoint=push)
        except NotImplementedError as exc:
            typer.echo(f"push unavailable: {exc}")
            raise typer.Exit(code=2) from exc


@app.command()
def universe(
    config: Path = typer.Option(..., "--config"),
    as_of: str = typer.Option(..., "--as-of", help="Membership date (ISO, UTC)."),
    universe_id: str = typer.Option(..., "--universe", help="Universe id, e.g. sp1500_pit."),
) -> None:
    """Print point-in-time index membership known as of a date.

    The survivorship check you can run by hand: constituents include names
    later delisted and exclude changes not yet announced (spec §6.3).
    """
    import asyncio

    from stratum.adapters.base import UniverseSpec
    from stratum.adapters.discovery import load_adapter
    from stratum.config import ResearchConfig

    research = ResearchConfig.load(config)
    matching = [a for a in research.adapters if a.settings.get("universe_id") == universe_id]
    if not matching:
        typer.echo(f"no configured adapter serves universe {universe_id!r}")
        raise typer.Exit(code=2)

    adapter_config = matching[0]

    async def _members() -> list[str]:
        adapter = load_adapter(adapter_config.id)()
        await adapter.configure(dict(adapter_config.settings), _read_only_context(research))
        members = await adapter.members_as_of(  # type: ignore[attr-defined]
            _parse_as_of(as_of).date(), UniverseSpec(id=universe_id)
        )
        await adapter.close()
        return list(members)

    members = asyncio.run(_members())
    typer.echo(f"{universe_id} as of {as_of}: {len(members)} members")
    for member in members:
        typer.echo(f"  {member}")


def _read_only_context(research: ResearchConfig) -> AdapterContext:
    """Minimal context for a lookup that writes nothing.

    ``members_as_of`` never reaches the sink, so this context deliberately
    carries a sink that raises: a lookup path that started writing would be a
    bug, and it should fail loudly rather than quietly bypass the guarded
    writer.
    """
    import logging

    from stratum.adapters.context import AdapterContext
    from stratum.schema.observation import Observation

    class _RefusingSink:
        async def submit(self, observation: Observation) -> None:
            raise RuntimeError("this context is read-only; ingestion must go through GuardedWriter")

    class _NoLimit:
        async def acquire(self, cost: int = 1) -> None:
            return None

    class _NoSecrets:
        def get(self, key: str) -> str | None:
            return None

    return AdapterContext(
        logger=logging.getLogger("stratum.cli.universe"),
        clock=lambda: datetime.now(UTC),
        data_dir=research.source.parent,
        secrets=_NoSecrets(),
        rate_limiter=_NoLimit(),
        sink=_RefusingSink(),
        run_id="cli-universe",
    )


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
