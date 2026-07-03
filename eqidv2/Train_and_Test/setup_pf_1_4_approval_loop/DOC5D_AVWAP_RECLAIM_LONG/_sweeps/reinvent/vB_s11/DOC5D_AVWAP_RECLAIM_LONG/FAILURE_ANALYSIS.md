# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-29 | GRAPHITE | SL | 50 | -829 |
| 2026-06-22 | ROHLTD | EOD | 162 | -153 |

## Worst days

- 2026-06-29: Rs-829
- 2026-06-22: Rs-153

## Worst symbols

- GRAPHITE: Rs-829
- ROHLTD: Rs-153

## Exit-type distribution (best cfg)

- {'EOD': 1, 'SL': 1}

## Notes
- SL/TGT/EOD split TRAIN = 10/7/3, TEST = 1/0/1.
- TRAIN avg win/avg loss = Rs1,650/Rs-800; TEST = Rs0/Rs-491.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 9.99.

## Classified failure reasons

- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST too few trades (test_n<6)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 35.0% (min 12.0%)
- TEST PF/day-block p: 0.0 / None (gate 1.4 / 0.1)