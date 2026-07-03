# L_PRESSURE_BURST_VWAP (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN target-fill rate below 12.0%
- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.40
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 0.0% (min 12.0%)
- TEST PF/day-block p: 0.51 / 0.8569 (gate 1.4 / 0.1)