# Authoring a factor

A factor is a reproducible, point-in-time function from signals + market data
to a cross-sectional exposure, plus metadata describing what it claims to
capture and how to evaluate it. Factors are plain YAML, optionally
with Python transform plugins, versioned and installable via the
`stratum.factors` entry-point group.

## Minimal shape

See `factors/price_momentum_5d.yaml` for a runnable market-data example and
`factors/reddit_attention_momentum.yaml` for the alt-data example.
The required blocks:

- `factor:` — id, version, `family` (momentum | value | quality | sentiment |
  custom), description, and a **baseline** to compare against, so incremental
  information is framed honestly ("explains X over and above price momentum").
- `inputs:` — each names exactly one of `signal:` or `market:`. The first is
  the numeric exposure; later inputs may provide labels such as sectors. Use
  `series_id` when a signal type contains independent series, and
  `min_coverage` to exclude thin observations rather than silently overstate
  them.
- `pit:` — `as_of_rule: knowledge_time` is the **only legal value**; the
  loader rejects anything else. `embargo` models the gap between "knowable"
  and "position takeable" (default 1 trading day).
- `transform:` — a pipeline of registered transform names (`pct_change`,
  `winsorize`, `sector_neutralize`, `cross_sectional_rank`, `zscore`, …).
  Each operation sees only one point-in-time cross-section.
- `evaluation:` — rebalance cadence, horizons, and diagnostics metrics
  (`ic`, `rank_ic`, `turnover`, `decay`, `coverage`).

## Honest framing

Describe your factor as a research hypothesis. Descriptions, factsheets, and
reports should not claim "alpha," edge, or expected return. The report
renderer rejects a small set of promise phrases. Stratum enforces point-in-time
construction; Ledger owns post-run leakage and return diagnostics.

## Build from a pinned snapshot

```bash
stratum factor factors/price_momentum_5d.yaml \
  --snapshot examples/data/snapshots/2024-03-01T000000Z \
  --dates 2024-02-29 \
  --out examples/data/price_momentum_5d.json
```

The JSON contains the snapshot hash, factor build hash, exposures, coverage,
and descriptive cross-sectional statistics. Return-linked IC, decay, and
turnover evaluation belongs in Ledger, which consumes the pinned snapshot.
