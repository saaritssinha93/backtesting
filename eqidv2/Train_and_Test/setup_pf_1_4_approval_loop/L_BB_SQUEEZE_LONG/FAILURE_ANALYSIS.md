# L_BB_SQUEEZE_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-05-14 | V2RETAIL | SL | 30 | -1,332 |
| 2026-05-18 | COCHINSHIP | SL | 79 | -1,314 |
| 2026-05-15 | IKS | EOD | 60 | -131 |
| 2026-05-18 | VENUSPIPES | TARGET | 54 | 1,257 |
| 2026-05-18 | INOXGREEN | TARGET | 118 | 1,265 |
| 2026-05-12 | BBOX | TARGET | 3 | 1,266 |
| 2026-05-15 | WEBELSOLAR | TARGET | 44 | 1,266 |

## Worst days

- 2026-05-14: Rs-1,332
- 2026-05-15: Rs1,135
- 2026-05-18: Rs1,208
- 2026-05-12: Rs1,266

## Worst symbols

- V2RETAIL: Rs-1,332
- COCHINSHIP: Rs-1,314
- IKS: Rs-131
- VENUSPIPES: Rs1,257
- INOXGREEN: Rs1,265

## Exit-type distribution (best cfg)

- {'TARGET': 4, 'SL': 2, 'EOD': 1}

## Notes
- SL/TGT/EOD split TRAIN = 84/57/40, TEST = 2/4/1.
- TRAIN avg win/avg loss = Rs1,030/Rs-1,179; TEST = Rs1,263/Rs-926.
- Concentration: TEST top-day share 0.556, top-symbol share 0.556, top-trade gross share 0.251.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)