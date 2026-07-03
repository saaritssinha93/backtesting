# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-25 | MANINFRA | SL | 209 | -1,232 |
| 2026-06-25 | AAVAS | SL | 69 | -1,230 |
| 2026-06-22 | HERITGFOOD | SL | 127 | -1,230 |
| 2026-06-25 | CLEAN | SL | 48 | -1,225 |
| 2026-06-24 | TATAELXSI | SL | 25 | -1,209 |
| 2026-06-30 | BSE | SL | 176 | -1,204 |
| 2026-06-25 | IREDA | EOD | 140 | -1,164 |
| 2026-06-29 | HONAUT | SL | 89 | -987 |

## Worst days

- 2026-06-25: Rs-4,645
- 2026-06-29: Rs-412
- 2026-06-23: Rs-343
- 2026-06-24: Rs260
- 2026-06-30: Rs909

## Worst symbols

- MANINFRA: Rs-1,232
- AAVAS: Rs-1,230
- HERITGFOOD: Rs-1,230
- CLEAN: Rs-1,225
- TATAELXSI: Rs-1,209

## Exit-type distribution (best cfg)

- {'EOD': 15, 'SL': 7, 'TARGET': 2}

## Notes
- SL/TGT/EOD split TRAIN = 11/6/21, TEST = 7/2/15.
- TRAIN avg win/avg loss = Rs1,152/Rs-841; TEST = Rs758/Rs-881.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.284.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.40
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 15.8% (min 12.0%)
- TEST PF/day-block p: 0.728 / 0.6945 (gate 1.4 / 0.1)