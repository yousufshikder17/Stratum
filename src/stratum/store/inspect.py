"""Store inspection — aggregates only, never rows (spec §4.8).

``stratum store`` answers "what is actually in here?": row counts, entity
counts, the span of each axis, and how much of the store the resolver managed
to canonicalize.

This is the one place that queries the physical table without an ``as_of``,
and it is safe precisely because **it returns no observations**. Every value
here is an aggregate. If this module ever grows a function that hands back a
row, that function has become an unscoped read and belongs behind
:meth:`~stratum.store.interface.SignalStore.read` instead.

Resolution coverage is reported per signal type on purpose: a store that looks
healthy overall can be 100% resolved on market data and 20% resolved on social
mentions, and the cross-section built from it will be quietly thin.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from stratum.store.duckdb_store import DuckDBSignalStore

__all__ = ["SignalSummary", "StoreSummary", "summarize"]


def _stamp(ns: int | None) -> datetime | None:
    return None if ns is None else datetime.fromtimestamp(ns / 1e9, tz=UTC)


@dataclass(frozen=True, kw_only=True, slots=True)
class SignalSummary:
    signal_type: str
    rows: int
    entities: int
    resolved_rows: int
    vintages: int
    event_start: datetime | None
    event_end: datetime | None
    knowledge_end: datetime | None

    @property
    def resolution_coverage(self) -> float:
        """Share of rows carrying a canonical ``security_id`` (0..1)."""
        return self.resolved_rows / self.rows if self.rows else 0.0


@dataclass(frozen=True, kw_only=True, slots=True)
class StoreSummary:
    path: str
    rows: int
    signals: Sequence[SignalSummary]
    runs: int
    last_ingest: datetime | None

    def lines(self) -> list[str]:
        """Human-readable summary for the CLI."""
        if not self.rows:
            return [f"{self.path}: empty store"]
        out = [f"{self.path}: {self.rows} rows across {len(self.signals)} signal types"]
        for signal in self.signals:
            span = "—"
            if signal.event_start and signal.event_end:
                span = (
                    f"{signal.event_start.date().isoformat()}"
                    f"..{signal.event_end.date().isoformat()}"
                )
            out.append(
                f"  {signal.signal_type:<26} rows={signal.rows:<7} "
                f"entities={signal.entities:<5} vintages={signal.vintages:<4} "
                f"resolved={signal.resolution_coverage:.0%}  events {span}"
            )
        if self.last_ingest is not None:
            out.append(f"  {self.runs} run(s); last ingest {self.last_ingest.isoformat()}")
        return out


def summarize(store: DuckDBSignalStore) -> StoreSummary:
    """Aggregate the physical table. Returns counts and ranges, never rows."""
    # An audit view over the store it was handed; not a second read path.
    conn = store._connect()
    total, runs, last_ingest_ns = conn.execute(
        "SELECT count(*), count(DISTINCT run_id), max(ingest_time_ns) FROM observations"
    ).fetchone()
    rows = conn.execute(
        """
        SELECT signal_type,
               count(*),
               count(DISTINCT coalesce(security_id, native_entity)),
               count(security_id),
               count(DISTINCT vintage_id),
               min(event_time_ns),
               max(event_time_ns),
               max(knowledge_time_ns)
        FROM observations
        GROUP BY signal_type
        ORDER BY signal_type
        """
    ).fetchall()
    signals = [
        SignalSummary(
            signal_type=row[0],
            rows=row[1],
            entities=row[2],
            resolved_rows=row[3],
            vintages=row[4],
            event_start=_stamp(row[5]),
            event_end=_stamp(row[6]),
            knowledge_end=_stamp(row[7]),
        )
        for row in rows
    ]
    return StoreSummary(
        path=str(store._path),
        rows=total or 0,
        signals=signals,
        runs=runs or 0,
        last_ingest=_stamp(last_ingest_ns),
    )
