# A_MOD_BREAK_C1_LOW (SHORT) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-12 | ONGC | SL | 60 | -932 |
| 2026-06-16 | INDOCO | SL | 13 | -931 |
| 2026-06-15 | KEC | SL | 128 | -930 |
| 2026-06-24 | YASHO | SL | 79 | -925 |
| 2026-06-16 | TORNTPOWER | SL | 44 | -921 |
| 2026-06-12 | POWERINDIA | SL | 109 | -649 |
| 2026-06-16 | BLACKBUCK | EOD | 225 | -409 |
| 2026-06-22 | WIPRO | EOD | 200 | -227 |

## Worst days

- 2026-06-12: Rs-1,301
- 2026-06-24: Rs-925
- 2026-06-16: Rs-713
- 2026-06-22: Rs65
- 2026-06-15: Rs161

## Worst symbols

- ONGC: Rs-932
- INDOCO: Rs-931
- KEC: Rs-930
- YASHO: Rs-925
- TORNTPOWER: Rs-921

## Exit-type distribution (best cfg)

- {'EOD': 7, 'SL': 6}

## Notes
- SL/TGT/EOD split TRAIN = 19/4/35, TEST = 6/0/7.
- TRAIN avg win/avg loss = Rs867/Rs-732; TEST = Rs642/Rs-740.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.347.

## Classified failure reasons

- TEST PF below 1.40
- TEST concentrated (one trade/day/symbol dominates)