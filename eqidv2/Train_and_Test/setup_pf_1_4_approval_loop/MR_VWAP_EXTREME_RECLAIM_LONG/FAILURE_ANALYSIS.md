# MR_VWAP_EXTREME_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-05-26 | ICICIBANK | SL | 159 | -1,425 |
| 2026-05-25 | TORNTPHARM | SL | 21 | -1,388 |
| 2026-05-26 | GRASIM | EOD | 130 | -516 |
| 2026-05-26 | HINDALCO | EOD | 135 | -471 |
| 2026-05-25 | AUROPHARMA | EOD | 85 | -93 |
| 2026-05-27 | HINDALCO | EOD | 95 | 158 |
| 2026-05-26 | KFINTECH | EOD | 120 | 746 |

## Worst days

- 2026-05-26: Rs-1,666
- 2026-05-25: Rs-1,481
- 2026-05-27: Rs158

## Worst symbols

- ICICIBANK: Rs-1,425
- TORNTPHARM: Rs-1,388
- GRASIM: Rs-516
- HINDALCO: Rs-313
- AUROPHARMA: Rs-93

## Exit-type distribution (best cfg)

- {'EOD': 5, 'SL': 2}

## Notes
- SL/TGT/EOD split TRAIN = 4/1/51, TEST = 2/0/5.
- TRAIN avg win/avg loss = Rs636/Rs-630; TEST = Rs452/Rs-779.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.825.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)