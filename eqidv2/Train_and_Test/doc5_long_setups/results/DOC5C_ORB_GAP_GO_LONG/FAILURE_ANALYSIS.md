# DOC5C_ORB_GAP_GO_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample.

## Classified failure reasons

- TRAIN too few trades (train_n<20)
- TRAIN PF too low (<1.30)
- TRAIN target-fill rate below 12.0%
- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.30
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 0.0% (min 12.0%)
- TEST PF/day-block p: 0.258 / 0.9986 (gate 1.3 / 0.1)