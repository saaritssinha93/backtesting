# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-16 | BECTORFOOD | SL | 60 | -1,232 |
| 2026-06-16 | MBAPL | SL | 52 | -1,232 |
| 2026-06-22 | RPEL | SL | 211 | -1,229 |
| 2026-06-15 | CARTRADE | SL | 14 | -1,224 |
| 2026-06-22 | JBMA | EOD | 210 | -1,162 |
| 2026-06-24 | ENRIN | EOD | 55 | -1,004 |
| 2026-06-24 | ASIANENE | EOD | 70 | -690 |
| 2026-06-24 | JUBLPHARMA | EOD | 65 | -538 |

## Worst days

- 2026-06-22: Rs-1,557
- 2026-06-15: Rs-1,224
- 2026-06-24: Rs-324
- 2026-06-16: Rs811

## Worst symbols

- BECTORFOOD: Rs-1,232
- MBAPL: Rs-1,232
- RPEL: Rs-1,229
- CARTRADE: Rs-1,224
- JBMA: Rs-1,162

## Exit-type distribution (best cfg)

- {'EOD': 13, 'SL': 4, 'TARGET': 1}

## Notes
- SL/TGT/EOD split TRAIN = 38/10/56, TEST = 4/1/13.
- TRAIN avg win/avg loss = Rs1,186/Rs-846; TEST = Rs937/Rs-805.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.345.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)
- too many trades/day