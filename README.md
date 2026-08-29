# Stratum

**A pre-alpha toolkit for point-in-time data and signal research.**

Stratum provides typed bitemporal observation models, adapter contracts and
manifests, a guarded ingestion path into an append-only point-in-time store,
survivorship-correct universe handling, point-in-time entity resolution,
content-addressed snapshots, and declarative factor construction. Backtesting
is the remaining major piece.

> **Scope disclaimer (binding, not boilerplate):** Stratum is *research
> infrastructure*, not a prediction or trading product. It is intended to help
> researchers construct point-in-time-correct datasets, define factors, and backtest them
> with explicit bias controls. It does **not** claim, imply, or measure
> "alpha generation," and no component, doc, or marketing surface may.
> Outputs are hypotheses and diagnostics, not investment advice. This
> disclaimer is a design constraint that shapes the schema, the engine, and
> the API surface.

**Status: pre-alpha, with a working local pipeline.** You can ingest CSV
market data and index membership into a point-in-time-correct DuckDB store,
resolve entities as of the moment each value became knowable, query the store
under an `as_of` scope, ask what an index contained on a given date, and
materialize a content-addressed snapshot — all offline, all covered by tests.

What is **not** implemented: the three network source adapters (Reddit, SEC
EDGAR, Google Trends), the backtest engine and leakage suite, the Parquet
panel exporter, and snapshot upload. `stratum backtest` and `stratum report`
exit with a scaffold message rather than a plausible-looking number.

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
Every store read requires an `as_of` and returns, per key, the latest vintage
with `knowledge_time <= as_of`. There is no overload without it.

## The leakage guard

The guard (`stratum.guard`) sits between adapters and storage.
`SignalStore.write` accepts only `ValidatedObservation`, and the only way to
obtain one is `LeakageGuard.validate()` — so the guarantee is structural, not
conventional. The guard checks:

- historical **backfill** rows stamped with a "now" knowledge time (the
  classic backfill bug) are rejected;
- provider-vintage sources (e.g. Google Trends) must stamp every row with a
  `vintage_id`;
- restatement-capable sources must provide a non-empty `vintage_id`;
- knowledge stamps beyond the configured clock-skew tolerance are rejected.

`GuardedWriter` is the only implementation of the sink handed to adapters. It
attaches canonical identity, stamps the core-owned `ingest_time`, runs the
guard, and writes. Rejected rows are dropped from the store *and* named in the
run report, and `stratum ingest` exits non-zero when anything was rejected — a
guard that silently discarded rows would be indistinguishable from a source
with gaps.

The store is append-only with vintage keys: re-pulling a window is idempotent
(ids are derived from record content), restatements append new vintages, and
an attempted in-place update raises. Writes are batched — the guard still sees
every row individually, but the store takes them a transaction at a time, and
a batch that violates an invariant is rolled back whole rather than left
half-applied.

## Rate limiting

Rate limiting is **core-supplied** (spec §3.2 rule 5): adapters declare what
they need, the core enforces it. Budgets are per *provider*, not per adapter,
because two adapters against one host are one client as far as that host's
terms are concerned.

```toml
[rate_limits.sec_edgar]
requests_per_second = 8.0
burst = 8
```

An adapter whose manifest declares `network = true` and whose provider has no
configured budget **will not run**. Forgetting to throttle a remote source
should fail at startup, not at the provider's discretion.

## Ledger integration

Ledger is an intentional sibling project, and the boundary between them is a
data contract rather than a code dependency: both envelopes carry
`event_time_ns` / `knowledge_time_ns` with identical names and semantics, and
both stores are append-only bitemporal tables. Ledger's `StratumAdapter` reads
a Stratum store directly — either the live DuckDB file or a pinned Parquet
snapshot — and re-emits each row on its own side, re-validating through its
own lookahead guard (defense in depth) and recording the snapshot's content
hash in the backtest manifest.

Neither repository needs a local checkout of the other to build or test; the
contract is the column names. See [docs/ledger-handoff.md](docs/ledger-handoff.md)
for the shape of the handoff and how to verify it locally.

## Layout

```
src/stratum/
├── schema/          # Observation/PointInTimeRecord, EventTime/KnowledgeTime/
│                    #   IngestTime/AsOf, typed payloads, registry, ULIDs
├── adapters/        # lifecycle ABCs + manifests + entry-point discovery
│   ├── market_csv/         # bars, corporate actions, delistings
│   ├── universe_csv/       # effective-dated index membership
│   ├── cost_default/       # commission/spread/sqrt-impact cost model
│   ├── reddit_sentiment/   # scaffold: social.sentiment / social.attention
│   ├── sec_edgar/          # scaffold: publication/restatement semantics
│   ├── google_trends/      # scaffold: provider-vintage semantics
│   └── parquet_panel/      # scaffold: as_of-stamped Parquet panels
├── guard/           # leakage checks + the guarded write path
├── store/           # SignalStore interface, DuckDB store, snapshot reader,
│                    #   aggregate-only inspection
├── resolver/        # bitemporal entity resolution over versioned tables
├── factors/         # YAML definitions + PIT-safe transforms + engine
├── backtest/        # configuration guards + engine/suite scaffolds
├── run/             # the guarded ingestion run, run manifests, snapshots
├── config.py        # the single research TOML
└── cli/             # stratum ingest / store / universe / factor / snapshot /
                     #   backtest / report / adapters
examples/            # a runnable synthetic research environment
```

## Install from source

Requires Python 3.12+. With [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --locked             # reproducible dev environment
uv run stratum --help
uv run stratum adapters      # list installed adapter plugins + manifests
```

## Quickstart

`examples/` is a complete research environment over synthetic data. It is
deliberately shaped to exercise the bias controls: a name goes bankrupt
mid-window, an index addition is announced a week before it takes effect, and
a ticker is later reused by an unrelated issuer.

```bash
uv run stratum ingest   --config examples/research.toml --backfill
uv run stratum store    --config examples/research.toml
uv run stratum snapshot --config examples/research.toml --as-of 2024-03-01T00:00:00Z
```

```
examples/data/stratum.duckdb: 124 rows across 4 signal types
  market.bar                 rows=119  entities=3  vintages=1  resolved=100%  events 2024-01-02..2024-02-29
  market.corporate_action    rows=2    entities=2  vintages=1  resolved=100%  events 2024-02-05..2024-02-12
  market.delisting           rows=1    entities=1  vintages=1  resolved=100%  events 2024-02-15..2024-02-15
  meta.coverage              rows=2    entities=2  vintages=2  resolved=100%  events 2024-02-01..2024-02-16
```

The survivorship guarantee is something you can check by hand. ZZZ goes
bankrupt and leaves the index on 2024-02-16, so it must still be a member
before then — a "current constituents" file would have erased it, and its
loss:

```bash
uv run stratum universe --config examples/research.toml     --universe sp1500_pit --as-of 2024-02-01     # AAA, BBB, ZZZ
uv run stratum universe --config examples/research.toml     --universe sp1500_pit --as-of 2024-02-20     # AAA, BBB
```

The knowledge axis is separately checkable. BBB joins the index on 2024-02-01,
announced 2024-01-25, so it is invisible on the 24th and known-but-not-yet-a-
member on the 26th:

```bash
uv run stratum universe --config examples/research.toml     --universe sp1500_pit --as-of 2024-01-24     # AAA, ZZZ
uv run stratum universe --config examples/research.toml     --universe sp1500_pit --as-of 2024-01-26     # AAA, ZZZ
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

CI runs lint, formatting, strict type checks, the test matrix, a structural
bias-gate, and a smoke run of the `examples/` bundle. The bias-gate job is the
one that matters most: it asserts the *structural* properties — the store's
write signature accepts only the guard's proof type, no read exists without an
`as_of`, entity resolution and universe membership both require one, and every
adapter output type has a required `knowledge_time`. A change that opens a
leakage or survivorship path is, by construction, a change that fails it.

Not covered by CI: backtest behavior (unimplemented) and Ledger integration,
which needs both repositories checked out — see
[docs/ledger-handoff.md](docs/ledger-handoff.md).

## What Stratum deliberately does not do

No live trading or order routing. No tradable-signal or return claims. No
redistribution of licensed or scraped third-party data.
