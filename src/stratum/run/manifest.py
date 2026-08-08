"""Run manifests (spec §2.3, §6.5, §6.6).

A run manifest records the inputs intended to identify a reproducible run.
End-to-end snapshot and backtest reproduction are not implemented yet.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

__all__ = ["RunManifest", "content_hash"]


def content_hash(payload: object) -> str:
    """Deterministic sha256 over canonical JSON."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, kw_only=True, slots=True)
class RunManifest:
    run_id: str
    snapshot_hash: str
    factor_def_hash: str
    universe_id: str
    cost_model_id: str
    seed: int
    code_version: str

    def hash(self) -> str:
        return content_hash(asdict(self))
