# V7 Research Layer

Separate non-trading research layer for the V7 ID 5-minute live pipeline.

## Code

- `eqidv2_v7_research_layer.py`
- `eqidv2_v7_pre_momentum_filter_analyst.py`

## Runner

- `bat/run_eqidv2_v7_research_layer.bat`
- `bat/run_eqidv2_v7_pre_momentum_filter_analyst.bat`

## Runtime Outputs

```text
C:\TradingData\eqidv2\live_research_v7_research_layer
  truth_table\
  reports\
  deep_analysis\
  latest\
C:\TradingData\eqidv2\v7_pre_momentum_filter_analyst
  latest\
  reports\
  suggestions\
C:\TradingData\eqidv2\dynamic_pre_momentum
  latest\
```

## Purpose

Build a daily truth table and reality-gap report from existing live files:

- raw/gated scanner candidates
- entry-engine audit rows
- live signal CSVs
- paper trade results

The daily reality-gap report also includes a `Deep Analysis Block`, with
standalone latest copies at:

```text
C:\TradingData\eqidv2\live_research_v7_research_layer\latest\latest_deep_analysis_block.md
C:\TradingData\eqidv2\live_research_v7_research_layer\latest\latest_deep_analysis_block.csv
```

The `v7 pre momentum filter analyst` is shadow-only. It publishes advisory
pre-momentum filter suggestions and a dynamic profile JSON, but does not change
entry-engine gates or paper/live trade values:

```text
C:\TradingData\eqidv2\v7_pre_momentum_filter_analyst\latest\latest_v7_pre_momentum_filter_analyst.md
C:\TradingData\eqidv2\v7_pre_momentum_filter_analyst\latest\latest_v7_pre_momentum_filter_suggestions.csv
C:\TradingData\eqidv2\dynamic_pre_momentum\latest\latest_dynamic_pre_momentum_profile.json
```

This layer does not place orders and does not modify the live scanner, entry
engine, or executors.
