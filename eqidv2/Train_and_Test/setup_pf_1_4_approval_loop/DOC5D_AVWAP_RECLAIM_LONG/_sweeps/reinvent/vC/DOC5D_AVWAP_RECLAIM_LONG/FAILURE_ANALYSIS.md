# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN target-fill rate below 12.0%
- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST too few trades (test_n<6)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 0.0% (min 12.0%)
- TEST PF/day-block p: 0.0 / None (gate 1.4 / 0.1)