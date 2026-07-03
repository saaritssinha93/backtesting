# B_AVWAP_RECLAIM_REVERSAL (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-12 | VINATIORGA | SL | 100 | -921 |

## Worst days

- 2026-06-12: Rs-921

## Worst symbols

- VINATIORGA: Rs-921

## Exit-type distribution (best cfg)

- {'SL': 1}

## Notes
- SL/TGT/EOD split TRAIN = 8/5/8, TEST = 1/0/0.
- TRAIN avg win/avg loss = Rs1,077/Rs-623; TEST = Rs0/Rs-921.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 9.99.

## Classified failure reasons

- too few trades (test_n<6)
- TRAIN PF too low (<1.30)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)