# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-25 | IREDA | SL | 121 | -1,132 |
| 2026-06-30 | JAYNECOIND | SL | 159 | -1,132 |
| 2026-06-25 | GMRAIRPORT | SL | 171 | -1,131 |
| 2026-06-29 | CONCORDBIO | SL | 58 | -1,131 |
| 2026-06-24 | VINDHYATEL | SL | 26 | -1,112 |
| 2026-06-24 | TATAELXSI | SL | 23 | -1,111 |
| 2026-06-30 | BSE | SL | 176 | -1,106 |
| 2026-06-24 | DLINKINDIA | EOD | 144 | -604 |

## Worst days

- 2026-06-25: Rs-2,263
- 2026-06-23: Rs-343
- 2026-06-24: Rs-326
- 2026-06-29: Rs1,782
- 2026-06-30: Rs2,668

## Worst symbols

- CONCORDBIO: Rs-1,181
- IREDA: Rs-1,132
- JAYNECOIND: Rs-1,132
- GMRAIRPORT: Rs-1,131
- VINDHYATEL: Rs-1,112

## Exit-type distribution (best cfg)

- {'EOD': 19, 'SL': 7, 'TARGET': 4}

## Notes
- SL/TGT/EOD split TRAIN = 33/11/51, TEST = 7/4/19.
- TRAIN avg win/avg loss = Rs1,134/Rs-833; TEST = Rs1,044/Rs-721.
- Concentration: TEST top-day share 0.687, top-symbol share 0.488, top-trade gross share 0.151.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN target-fill rate below 12.0%
- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 11.6% (min 12.0%)
- TEST PF/day-block p: 1.448 / 0.1602 (gate 1.4 / 0.1)