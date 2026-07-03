# G_HIGHER_HIGH_BREAK (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-22 | WSTCSTPAPR | SL | 109 | -1,428 |
| 2026-06-24 | TORNTPOWER | EOD | 95 | -1,071 |
| 2026-06-22 | RATEGAIN | EOD | 135 | -1,018 |
| 2026-06-24 | CIPLA | EOD | 155 | -974 |
| 2026-06-12 | CHAMBLFERT | EOD | 85 | -211 |
| 2026-06-10 | CHAMBLFERT | EOD | 185 | -4 |
| 2026-06-12 | RCF | EOD | 75 | 82 |
| 2026-06-22 | WEBELSOLAR | EOD | 235 | 767 |

## Worst days

- 2026-06-24: Rs-2,045
- 2026-06-22: Rs-427
- 2026-06-10: Rs-4
- 2026-06-12: Rs3,390

## Worst symbols

- WSTCSTPAPR: Rs-1,428
- TORNTPOWER: Rs-1,071
- RATEGAIN: Rs-1,018
- CIPLA: Rs-974
- CHAMBLFERT: Rs-215

## Exit-type distribution (best cfg)

- {'EOD': 8, 'TARGET': 3, 'SL': 1}

## Notes
- SL/TGT/EOD split TRAIN = 2/8/10, TEST = 1/3/8.
- TRAIN avg win/avg loss = Rs1,009/Rs-825; TEST = Rs937/Rs-784.
- Concentration: TEST top-day share 3.71, top-symbol share 1.386, top-trade gross share 0.225.

## Classified failure reasons

- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.40
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 40.0% (min 12.0%)
- TEST PF/day-block p: 1.194 / 0.427 (gate 1.4 / 0.1)