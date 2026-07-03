# MR_CONTROLLED_VWAP_EXTREME_FADE_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-05-21 | BANDHANBNK | SL | 9 | -932 |
| 2026-05-29 | JSWSTEEL | SL | 124 | -931 |
| 2026-05-26 | AXISBANK | SL | 51 | -923 |
| 2026-05-25 | HAVELLS | EOD | 150 | -532 |
| 2026-05-25 | HAL | EOD | 115 | -217 |

## Worst days

- 2026-05-21: Rs-932
- 2026-05-29: Rs-931
- 2026-05-26: Rs-923
- 2026-05-25: Rs-749

## Worst symbols

- BANDHANBNK: Rs-932
- JSWSTEEL: Rs-931
- AXISBANK: Rs-923
- HAVELLS: Rs-532
- HAL: Rs-217

## Exit-type distribution (best cfg)

- {'SL': 3, 'EOD': 2}

## Notes
- SL/TGT/EOD split TRAIN = 3/9/2, TEST = 3/0/2.
- TRAIN avg win/avg loss = Rs764/Rs-753; TEST = Rs0/Rs-707.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 9.99.

## Classified failure reasons

- TRAIN PF too low / too few trades (train_n<20)
- too few trades (test_n<6)
- TRAIN PF too high / overfit risk (>1.70)
- TEST PF below 1.40
- TEST concentrated (one trade/day/symbol dominates)