# E_VWAP_LOSE_EARLY_SHORT (SHORT) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-15 | RELIGARE | SL | 61 | -932 |
| 2026-06-10 | PRICOLLTD | SL | 101 | -932 |
| 2026-06-11 | GMBREW | SL | 26 | -930 |
| 2026-06-15 | CARERATING | SL | 12 | -930 |
| 2026-06-16 | PVRINOX | SL | 13 | -929 |
| 2026-06-16 | TATASTEEL | EOD | 315 | -432 |
| 2026-06-15 | LAURUSLABS | EOD | 330 | 96 |
| 2026-06-11 | LT | EOD | 335 | 143 |

## Worst days

- 2026-06-16: Rs-1,024
- 2026-06-10: Rs-932
- 2026-06-11: Rs-787
- 2026-06-15: Rs-18

## Worst symbols

- RELIGARE: Rs-932
- PRICOLLTD: Rs-932
- GMBREW: Rs-930
- CARERATING: Rs-930
- PVRINOX: Rs-929

## Exit-type distribution (best cfg)

- {'EOD': 6, 'SL': 5}

## Notes
- SL/TGT/EOD split TRAIN = 10/0/11, TEST = 5/0/6.
- TRAIN avg win/avg loss = Rs966/Rs-919; TEST = Rs465/Rs-847.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.629.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)