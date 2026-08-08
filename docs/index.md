# Stratum

Pre-alpha research tooling for a common **point-in-time signal schema**,
adapter contracts, leakage validation, and declarative factor definitions.
Storage, ingestion, factor computation, backtesting, snapshots, and the
leakage suite are currently interfaces or scaffolds rather than working
pipelines.

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
`knowledge_time`. The store interface requires guarded writes and an `as_of`
scope for reads. The DuckDB implementation is not complete, so persistence
and as-of query guarantees are not yet available.

## Ledger

Ledger is an intentional sibling project. Stratum is the point-in-time data
and signal foundation that Ledger can consume through adapter interfaces.
No local Ledger checkout or private repository is required by this project,
and no public Ledger URL is assumed.

## Start here

- [Authoring an adapter](adapter-authoring.md) — connect a new source.
- [Authoring a factor](factor-authoring.md) — define a factor in YAML.
