# A_MOD_BREAK_C1_HIGH (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-03._

- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN target-fill rate below 12.0%
- TRAIN concentrated (one trade/day/symbol dominates)
- TRAIN too many trades/day
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.40
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)
- TEST too many trades/day

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 0.0% (min 12.0%)
- TEST PF/day-block p: 0.395 / 1.0 (gate 1.4 / 0.1)