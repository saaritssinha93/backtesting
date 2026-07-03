# L_PRESSURE_BURST_VWAP (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-12 | PREMEXPLN | SL | 102 | -1,233 |
| 2026-06-12 | APTECHT | SL | 59 | -1,232 |
| 2026-06-22 | CAMLINFINE | SL | 103 | -1,232 |
| 2026-06-12 | HITECH | SL | 12 | -1,232 |
| 2026-06-15 | VINCOFE | SL | 18 | -1,232 |
| 2026-06-12 | PNBGILTS | SL | 33 | -1,232 |
| 2026-06-12 | REFEX | SL | 5 | -1,231 |
| 2026-06-22 | MUFIN | SL | 46 | -1,231 |

## Worst days

- 2026-06-12: Rs-6,391
- 2026-06-15: Rs-5,543
- 2026-06-22: Rs-828
- 2026-06-24: Rs-248

## Worst symbols

- PREMEXPLN: Rs-1,233
- APTECHT: Rs-1,232
- CAMLINFINE: Rs-1,232
- HITECH: Rs-1,232
- VINCOFE: Rs-1,232

## Exit-type distribution (best cfg)

- {'SL': 20, 'EOD': 12, 'TARGET': 7}

## Notes
- SL/TGT/EOD split TRAIN = 68/23/62, TEST = 20/7/12.
- TRAIN avg win/avg loss = Rs1,134/Rs-944; TEST = Rs1,260/Rs-1,042.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.117.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)
- too many trades/day