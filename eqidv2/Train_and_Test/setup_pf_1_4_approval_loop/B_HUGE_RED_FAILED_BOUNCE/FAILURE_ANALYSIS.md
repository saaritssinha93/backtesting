# B_HUGE_RED_FAILED_BOUNCE (SHORT) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-24 | PINELABS | SL | 30 | -1,232 |
| 2026-06-15 | JSWCEMENT | SL | 75 | -1,232 |
| 2026-06-15 | FIVESTAR | SL | 9 | -1,232 |
| 2026-06-12 | TATASTEEL | SL | 68 | -1,232 |
| 2026-06-24 | DEVYANI | SL | 9 | -1,231 |
| 2026-06-22 | CEINSYS | SL | 25 | -1,231 |
| 2026-06-22 | MBAPL | SL | 66 | -1,231 |
| 2026-06-12 | TATAPOWER | SL | 151 | -1,230 |

## Worst days

- 2026-06-24: Rs-6,083
- 2026-06-12: Rs-3,167
- 2026-06-22: Rs-3,156
- 2026-06-16: Rs-2,842
- 2026-06-15: Rs-2,302

## Worst symbols

- PINELABS: Rs-1,232
- JSWCEMENT: Rs-1,232
- FIVESTAR: Rs-1,232
- TATASTEEL: Rs-1,232
- DEVYANI: Rs-1,231

## Exit-type distribution (best cfg)

- {'SL': 20, 'EOD': 15, 'TARGET': 7}

## Notes
- SL/TGT/EOD split TRAIN = 56/37/68, TEST = 20/7/15.
- TRAIN avg win/avg loss = Rs876/Rs-880; TEST = Rs969/Rs-910.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.119.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)
- too many trades/day