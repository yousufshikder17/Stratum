# Authoring an adapter

This guide describes the adapter contract. The bundled CSV and SEC EDGAR
adapters are working references; Reddit and Google Trends remain scaffolds.

## What an adapter is

A pluggable translator between a native source/API and the common signal
schema. Five families share one lifecycle contract (`stratum.adapters.base`):

- `SourceAdapter` — alt-data (Reddit, EDGAR, Trends, …)
- `MarketDataAdapter` — bars, corporate actions, delistings
- `UniverseAdapter` — historical membership (survivorship-correct)
- `CostModelAdapter` — commission/spread/slippage models
- `OutputAdapter` — exporters and report generators

## Packaging checklist

1. Implement the family ABC; ship the class in a package.
2. Add a static `manifest.toml` next to the package (`[adapter]` + `[pit]`
   sections — see any bundled adapter for the shape).
3. Add a `config.schema.json` (JSON Schema) for your configuration.
4. Register the class in the `stratum.adapters` entry-point group.
5. Put source SDK dependencies in an optional extra, never in core.

## The one rule that matters most

**Knowledge-time honesty.** `knowledge_time` must be the earliest moment a
value could have been acted on:

- Reddit: `created_utc` (visible at creation — honest).
- EDGAR: `acceptance_datetime` (the official available-to-public stamp).
- Trends: the *request* time, plus a per-pull `vintage_id` — the provider
  rescales and backfills silently.

If your source cannot reconstruct historical knowledge times, declare
`knowledge_time_basis = "ingestion"` and refuse silent backfill. The ingest
leakage guard rejects historical rows stamped "now"; fabricating
stamps is how alt-data backtests lie.

## Restatements

New information about an existing `(entity, signal_type, series_id, event_time)`
is a **new vintage** with a later `knowledge_time`, never an overwrite. Use a
stable `series_id` when one signal type carries multiple independent series.
If your manifest declares
`supports_restatement = true`, every row must carry a `vintage_id`.

## Terms & data hygiene

Declare and respect provider rate limits and ToS (`license_tag`, the shared
rate-limiter in `AdapterContext`). Never redistribute raw licensed data; test
fixtures must be synthetic.
