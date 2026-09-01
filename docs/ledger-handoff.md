# The Ledger handoff

Stratum and Ledger are sibling projects with a deliberate
split: **Stratum ingests and validates at origin; Ledger backtests and
scores.** They share no code. What they share is a data contract.

## The contract

Both envelopes carry the same two clocks under the same column names, and both
stores are append-only bitemporal tables:

| Stratum column | Ledger field | Note |
|---|---|---|
| `observation_id` | `observation_id` | verbatim — idempotent re-ingest |
| `signal_type` | `data_type` | verbatim; both use `family.kind` |
| `event_time_ns` | `event_time` | bit-for-bit |
| `knowledge_time_ns` | `knowledge_time` | bit-for-bit — **the shared axis** |
| `vintage_id` | `vintage_id` | `stratum:<id>` when empty, so Ledger's vintage-keyed append stays total |
| `source_id` | `source_id` | upstream provenance preserved |
| `license_tag` | `license_tag` | the data-terms firewall carries across |
| `payload` (JSON) | typed payload | falls back to `GenericPayload` when shapes differ |
| `run_id`, `adapter_id`, `data_class` | `extensions["stratum.*"]` | preserved, not dropped |

> **Do not rename `event_time_ns` or `knowledge_time_ns` in one project without
> renaming them in the other.** They are the boundary.

## Which side owns what

Building the same thing twice is the failure mode this split exists to
prevent, so the line is worth stating plainly:

| Stratum | Ledger |
|---|---|
| Source adapters, ingest, the leakage guard | Factor evaluation and the simulation loop |
| The PIT store and its vintages | Validity scorecards, PSR/DSR, multiple-testing corrections |
| Entity resolution | Walk-forward consistency, attribution |
| Survivorship-correct universe | Cost models applied to fills |
| Content-addressed snapshots | Backtest manifests that pin those snapshots |

Stratum's `backtest/` package holds configuration guards and a scaffold. It is
not the next thing to build here — Ledger already has the simulation and
validity mathematics, and duplicating it would mean maintaining two of them.

## Defense in depth, not delegated trust

Every row in a Stratum store has already passed Stratum's leakage guard at
origin. Ledger re-validates all of it through its own lookahead guard anyway,
and its scorecard records the upstream controls
(`ledger.validity.scorecard.STRATUM_UPSTREAM_CONTROLS`) rather than silently
assuming either layer. Two guards that both run and both get credited beats
one guard and an assumption.

## Verifying the handoff locally

With both repositories checked out and Stratum's example bundle ingested:

```bash
# In Stratum
uv run stratum ingest   --config examples/research.toml --backfill
uv run stratum snapshot --config examples/research.toml --as-of 2024-03-01T00:00:00Z
```

Then, from Ledger, point its `StratumAdapter` at either the live store or the
pinned snapshot:

```python
from ledger.adapters.stratum import StratumAdapter

adapter = StratumAdapter()
await adapter.configure({"store_path": ".../examples/data/stratum.duckdb"}, ctx)
# or, pinned for reproducibility:
await adapter.configure(
    {"snapshot_dir": ".../snapshots/2024-03-01T000000Z", "expected_snapshot_id": "<content-hash>"},
    ctx,
)
```

Both paths should yield the same 124 observations across `market.bar`,
`market.corporate_action`, `market.delisting`, and `meta.coverage`, with the
delisting arriving on Ledger's side as a typed `DelistingPayload` carrying a
real `date`, `security_id=SEC-0000000003`, and `knowledge_time`
2024-02-09T21:00Z — six days before the last trade, because that is when the
bankruptcy was announced.

## Reproducibility

`StratumAdapter.snapshot_id()` returns the snapshot's content hash. It belongs
in `BacktestManifest.stratum_snapshot_id`, and the scorecard's reproducibility
stamp surfaces it — so a run mixing Ledger market data with Stratum signals is
pinned end to end. Reading a snapshot with `expected_snapshot_id` set to a
different hash is refused rather than warned about.

## Automated local contract check

With both repositories checked out as siblings, run:

```bash
pytest tests/integration/test_ledger_handoff.py
```

The test creates a real current-schema Stratum snapshot and makes Ledger's
adapter consume it, checking the pinned hash and both clocks. Set
`LEDGER_REPO` when Ledger is not in the sibling directory. The test skips when
Ledger is absent, so either repository remains independently installable; a
hosted cross-repository CI job still needs the repository URL and access policy.
