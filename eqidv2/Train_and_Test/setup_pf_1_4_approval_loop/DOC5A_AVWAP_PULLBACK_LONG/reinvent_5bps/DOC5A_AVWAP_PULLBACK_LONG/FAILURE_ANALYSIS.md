# DOC5A_AVWAP_PULLBACK_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample.

## Classified failure reasons

- TRAIN too few trades (train_n<20)
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
- TEST PF/day-block p: 0.002 / 1.0 (gate 1.4 / 0.1)