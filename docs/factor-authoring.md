# Authoring a factor

A factor is a reproducible, point-in-time function from signals + market data
to a cross-sectional exposure, plus metadata describing what it claims to
capture and how to evaluate it. Factors are plain YAML, optionally
with Python transform plugins, versioned and installable via the
`stratum.factors` entry-point group.

## Minimal shape

See `factors/reddit_attention_momentum.yaml` for the full annotated example.
The required blocks:

- `factor:` — id, version, `family` (momentum | value | quality | sentiment |
  custom), description, and a **baseline** to compare against, so incremental
  information is framed honestly ("explains X over and above price momentum").
- `inputs:` — each names exactly one of `signal:` or `market:`, with an
  optional `min_coverage` floor so thin alt-data is excluded or downweighted,
  never silently overstated.
- `pit:` — `as_of_rule: knowledge_time` is the **only legal value**; the
  loader rejects anything else. `embargo` models the gap between "knowable"
  and "position takeable" (default 1 trading day).
- `transform:` — a pipeline of registered transform names (`pct_change`,
  `winsorize`, `sector_neutralize`, `cross_sectional_rank`, `zscore`, …).
  These transforms and the factor engine are not implemented yet.
- `evaluation:` — rebalance cadence, horizons, and diagnostics metrics
  (`ic`, `rank_ic`, `turnover`, `decay`, `coverage`).

## Honest framing

Describe your factor as a research hypothesis. Descriptions, factsheets, and
reports should not claim "alpha," edge, or expected return. The report
renderer rejects a small set of promise phrases. The computational leakage
suite is not implemented yet.
