# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-25 | CONCOR | SL | 124 | -1,327 |
| 2026-06-10 | MAXHEALTH | SL | 217 | -1,320 |
| 2026-06-19 | PAGEIND | SL | 95 | -1,085 |
| 2026-06-24 | PETRONET | EOD | 140 | -778 |
| 2026-06-30 | AUROPHARMA | EOD | 195 | -115 |
| 2026-06-09 | DIVISLAB | EOD | 200 | 126 |
| 2026-06-17 | IRFC | TARGET | 192 | 1,016 |
| 2026-06-18 | NYKAA | TARGET | 19 | 1,016 |

## Worst days

- 2026-06-25: Rs-1,327
- 2026-06-10: Rs-1,320
- 2026-06-19: Rs-1,085
- 2026-06-24: Rs-778
- 2026-06-30: Rs-115

## Worst symbols

- CONCOR: Rs-1,327
- MAXHEALTH: Rs-1,320
- PAGEIND: Rs-1,085
- PETRONET: Rs-778
- AUROPHARMA: Rs-115

## Exit-type distribution (best cfg)

- {'EOD': 3, 'SL': 3, 'TARGET': 2}

## Notes
- SL/TGT/EOD split TRAIN = 5/8/9, TEST = 3/2/3.
- TRAIN avg win/avg loss = Rs798/Rs-922; TEST = Rs719/Rs-925.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.471.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.30
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 36.4% (min 12.0%)
- TEST PF/day-block p: 0.467 / 0.8288 (gate 1.3 / 0.1)