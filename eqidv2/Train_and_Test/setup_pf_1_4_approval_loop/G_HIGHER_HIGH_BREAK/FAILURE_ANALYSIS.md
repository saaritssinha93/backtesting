# G_HIGHER_HIGH_BREAK (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-10 | DOLLAR | SL | 48 | -1,731 |
| 2026-06-22 | METROPOLIS | SL | 153 | -1,727 |
| 2026-06-15 | TMB | SL | 96 | -1,721 |
| 2026-06-22 | WSTCSTPAPR | EOD | 184 | -1,495 |
| 2026-06-15 | SHRIRAMFIN | EOD | 205 | -989 |
| 2026-06-10 | CHAMBLFERT | EOD | 185 | -4 |
| 2026-06-22 | CUB | EOD | 240 | 147 |
| 2026-06-22 | WEBELSOLAR | EOD | 235 | 767 |

## Worst days

- 2026-06-15: Rs-2,710
- 2026-06-10: Rs-469
- 2026-06-22: Rs210

## Worst symbols

- DOLLAR: Rs-1,731
- METROPOLIS: Rs-1,727
- TMB: Rs-1,721
- WSTCSTPAPR: Rs-1,495
- SHRIRAMFIN: Rs-989

## Exit-type distribution (best cfg)

- {'EOD': 5, 'SL': 3, 'TARGET': 3}

## Notes
- SL/TGT/EOD split TRAIN = 2/9/7, TEST = 3/3/5.
- TRAIN avg win/avg loss = Rs1,117/Rs-1,233; TEST = Rs940/Rs-1,278.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.27.

## Classified failure reasons

- TRAIN PF too low / too few trades (train_n<20)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)