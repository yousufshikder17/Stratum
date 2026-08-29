# Stratum

Pre-alpha research tooling for a common **point-in-time signal schema**,
adapter contracts, leakage validation, and declarative factors. Guarded
ingestion, the bitemporal store, entity resolution, survivorship-correct
universes, factor construction, and content-addressed snapshots work today.
The backtest engine, the leakage suite, and the three network source adapters
are still scaffolds.

!!! warning "Scope (binding, not boilerplate)"
    Stratum is *research infrastructure*, not a prediction or trading
    product. It does not claim, imply, or measure "alpha generation."
    Outputs are hypotheses and diagnostics, not investment advice. This
    constraint shapes the schema, the engine, and the API surface.

## The load-bearing invariant

Every observation carries two clocks:

| Clock | Meaning | Engine rule |
|---|---|---|
| `event_time` | when the thing happened | the axis you align factors on |
| `knowledge_time` | when it became knowable | **the engine reads only `knowledge_time <= as_of`** |

The two are distinct Python types that cannot be compared directly;
`Observation` (a.k.a. `PointInTimeRecord`) cannot be constructed without a
`knowledge_time`. The store accepts only the leakage guard's proof type on
write and requires an `as_of` on every read — there is no unscoped read and no
unguarded write.

## Ledger

Ledger is an intentional sibling project: Stratum ingests and validates at
origin, Ledger backtests and scores. They share no code — only a column
contract on the two clocks — and neither needs a checkout of the other to
build or test. See [the Ledger handoff](ledger-handoff.md).

## Start here

- [Authoring an adapter](adapter-authoring.md) — connect a new source.
- [Authoring a factor](factor-authoring.md) — define a factor in YAML.
- [The Ledger handoff](ledger-handoff.md) — the sibling data contract.

Or run the pipeline over the synthetic `examples/` bundle:

```bash
uv run stratum ingest   --config examples/research.toml --backfill
uv run stratum universe --config examples/research.toml     --universe sp1500_pit --as-of 2024-02-01
```
