# V7 Research Layer

Separate non-trading research layer for the V7 ID 5-minute live pipeline.

## Code

- `eqidv2_v7_research_layer.py`

## Runner

- `bat/run_eqidv2_v7_research_layer.bat`

## Runtime Outputs

```text
C:\TradingData\eqidv2\live_research_v7_research_layer
  truth_table\
  reports\
  latest\
```

## Purpose

Build a daily truth table and reality-gap report from existing live files:

- raw/gated scanner candidates
- entry-engine audit rows
- live signal CSVs
- paper trade results

This layer does not place orders and does not modify the live scanner, entry
engine, or executors.
