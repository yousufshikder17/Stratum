"""Research/run manager: reproducibility as a first-class component (§2.1)."""

from stratum.run.ingest import (
    AdapterIngestResult,
    IngestReport,
    Rejection,
    format_report,
    ingest,
    run_ingest,
)
from stratum.run.manifest import RunManifest, content_hash
from stratum.run.snapshot import create_snapshot, push_snapshot

__all__ = [
    "AdapterIngestResult",
    "IngestReport",
    "Rejection",
    "RunManifest",
    "content_hash",
    "create_snapshot",
    "format_report",
    "ingest",
    "push_snapshot",
    "run_ingest",
]
