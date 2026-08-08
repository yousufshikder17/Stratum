# Stratum

**A pre-alpha toolkit for point-in-time data and signal research.**

Stratum currently provides typed bitemporal observation models, adapter
contracts and manifests, a leakage-validation boundary, declarative factor
validation, and bias-control configuration types. It is intended to grow into
source-agnostic ingestion, point-in-time storage, factor construction, and
bias-aware backtesting.

> **Scope disclaimer (binding, not boilerplate):** Stratum is *research
> infrastructure*, not a prediction or trading product. It is intended to help
> researchers construct point-in-time-correct datasets, define factors, and backtest them
> with explicit bias controls. It does **not** claim, imply, or measure
> "alpha generation," and no component, doc, or marketing surface may.
> Outputs are hypotheses and diagnostics, not investment advice. This
> disclaimer is a design constraint that shapes the schema, the engine, and
> the API surface.

**Status: pre-alpha scaffold, not an operational research pipeline.** The
schema types, manifest and factor-definition parsers, selected leakage checks,
and configuration guards have tests. The DuckDB read/write path, snapshots,
source ingestion, entity resolution, factor transforms, factor engine,
backtest engine, and leakage suite are not implemented. Most CLI commands
therefore exit with a scaffold message.

## The architectural waist

The central interface type is `Observation`, also exported as
**`PointInTimeRecord`** (`stratum.schema`). Adapter contracts produce it and
consumer interfaces accept it. Its envelope carries two clocks, and the
distinction is enforced with distinct Python types, runtime validation, and
static type annotations:

| Clock | Set by | Meaning |
|---|---|---|
| `event_time: EventTime` | adapter | when the real-world event occurred |
| `knowledge_time: KnowledgeTime` | adapter, honestly | the earliest moment the value could have been acted on |
| `ingest_time: IngestTime` | core | when Stratum stored it — audit only, never selection |

`EventTime` and `KnowledgeTime` are distinct, frozen, non-interchangeable
types: comparing one against the other raises `TypeError`, and an
`Observation` cannot be constructed without an explicit `knowledge_time`.
The store interface requires an `as_of` parameter for reads. The DuckDB query
implementation that will enforce `knowledge_time <= as_of` is not yet built.

## The leakage guard

The guard (`stratum.guard`) is designed to sit between adapters and storage.
`SignalStore.write` is typed to accept `ValidatedObservation`, obtained from
`LeakageGuard.validate()`. The implemented guard currently checks:

- historical **backfill** rows stamped with a "now" knowledge time (the
  classic backfill bug) are rejected;
- provider-vintage sources (e.g. Google Trends) must stamp every row with a
  `vintage_id`;
- restatement-capable sources must provide a non-empty `vintage_id`;
- knowledge stamps beyond the configured clock-skew tolerance are rejected.

Append-only conflict handling, idempotency, vintage selection, and persistence
remain unimplemented in the DuckDB store.

## Ledger integration

Ledger is an intentional sibling project. Stratum defines the point-in-time
data and signal boundary that Ledger can consume through adapter interfaces.
The repositories do not require a local sibling checkout to build or test,
and this repository does not assume that Ledger has a public URL. A concrete
Ledger adapter and end-to-end integration tests are not included yet.

## Layout

```
src/stratum/
├── schema/          # common Observation/PointInTimeRecord schema,
│                    #   EventTime/KnowledgeTime/IngestTime/AsOf, payloads, registry
├── adapters/        # lifecycle ABCs + manifests + entry-point discovery
│   ├── reddit_sentiment/   # scaffold: social.sentiment / social.attention
│   ├── sec_edgar/          # scaffold: publication/restatement semantics
│   ├── google_trends/      # scaffold: provider-vintage semantics
│   ├── market_csv/         # scaffold: bars, actions, delistings
│   ├── universe_csv/       # scaffold: historical membership
│   ├── cost_default/       # default commission/spread/sqrt-impact cost model
│   └── parquet_panel/      # output adapter: as_of-stamped Parquet panels
├── guard/           # leakage checks + guarded writer
├── store/           # SignalStore interface + incomplete DuckDB scaffold
├── resolver/        # point-in-time entity-resolution scaffold
├── factors/         # YAML validation + transform/engine scaffolds
├── backtest/        # configuration guards + engine/suite scaffolds
├── run/             # run manifests + snapshot scaffolds
└── cli/             # stratum ingest / store / factor / backtest / report / snapshot
```

## Install from source

Requires Python 3.12+. With [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --locked             # reproducible dev environment
uv run stratum --help
uv run stratum adapters      # list installed adapter plugins + manifests
uv run stratum factor factors/reddit_attention_momentum.yaml --validate-only
```

There is no supported PyPI installation command for this project. The
distribution name `stratum` is already used on PyPI; a publication name has
not been selected. Source-specific SDK declarations are optional extras, but
their adapters are currently scaffolds.

## Extending

- **Adapters** are plugins in the `stratum.adapters` entry-point group, each
  with a static `manifest.toml` (including its PIT declaration) and a JSON
  Schema for config. See [docs/adapter-authoring.md](docs/adapter-authoring.md).
- **Factors** are declarative YAML in the `stratum.factors` group; the PIT
  policy's only legal selection axis is `knowledge_time`. See
  [docs/factor-authoring.md](docs/factor-authoring.md).

## Development

```bash
uv run ruff check . && uv run ruff format --check .   # lint
uv run mypy                                           # strict typing = correctness gate
uv run pytest                                         # tests incl. tests/bias_gate
```

CI is configured to run lint, formatting, strict type checks, the test matrix,
and a structural bias-gate. These checks cover the scaffold only; they do not
demonstrate operational storage, ingestion, factor, backtest, snapshot, or
Ledger integration behavior.

## What Stratum deliberately does not do

No live trading or order routing. No tradable-signal or return claims. No
redistribution of licensed or scraped third-party data.
