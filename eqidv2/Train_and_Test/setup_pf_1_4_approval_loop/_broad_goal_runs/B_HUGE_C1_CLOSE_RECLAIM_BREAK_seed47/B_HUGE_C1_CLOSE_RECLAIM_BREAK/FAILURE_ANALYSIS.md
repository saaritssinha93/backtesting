# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-05-27 | BAJAJHFL | SL | 59 | -1,432 |
| 2026-06-10 | BIOCON | SL | 228 | -1,432 |
| 2026-06-10 | IXIGO | EOD | 205 | -1,386 |
| 2026-05-22 | J&KBANK | EOD | 200 | -1,053 |
| 2026-05-25 | CHALET | EOD | 226 | -694 |
| 2026-05-27 | INDIGOPNTS | EOD | 225 | -592 |
| 2026-05-19 | CYIENTDLM | EOD | 250 | -569 |
| 2026-05-22 | PIDILITIND | EOD | 210 | -299 |

## Worst days

- 2026-06-10: Rs-2,818
- 2026-05-27: Rs-2,025
- 2026-05-25: Rs-980
- 2026-05-22: Rs8
- 2026-06-09: Rs309

## Worst symbols

- BAJAJHFL: Rs-1,432
- BIOCON: Rs-1,432
- IXIGO: Rs-1,386
- J&KBANK: Rs-1,053
- CHALET: Rs-694

## Exit-type distribution (best cfg)

- {'EOD': 13, 'TARGET': 6, 'SL': 2}

## Notes
- SL/TGT/EOD split TRAIN = 2/6/13, TEST = 0/0/0.
- TRAIN avg win/avg loss = Rs1,469/Rs-750; TEST = Rs0/Rs0.
- Concentration: TEST top-day share None, top-symbol share None, top-trade gross share None.

## Classified failure reasons

- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST too few trades (test_n<6)
- TRAIN PF above preferred band (>1.70)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 28.6% (min 12.0%)
- TEST PF/day-block p: 0.0 / None (gate 1.4 / 0.1)