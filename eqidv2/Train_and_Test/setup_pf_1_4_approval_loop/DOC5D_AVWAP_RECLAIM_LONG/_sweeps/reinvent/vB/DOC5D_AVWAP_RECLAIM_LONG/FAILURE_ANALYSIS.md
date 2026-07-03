# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-25 | MANINFRA | SL | 54 | -982 |
| 2026-06-22 | CHENNPETRO | EOD | 220 | -591 |
| 2026-06-30 | CONCORDBIO | EOD | 155 | -50 |
| 2026-06-24 | OIL | EOD | 195 | 477 |
| 2026-06-24 | RBLBANK | EOD | 140 | 620 |
| 2026-06-30 | KITEX | TARGET | 122 | 1,867 |

## Worst days

- 2026-06-25: Rs-982
- 2026-06-22: Rs-591
- 2026-06-24: Rs1,096
- 2026-06-30: Rs1,817

## Worst symbols

- MANINFRA: Rs-982
- CHENNPETRO: Rs-591
- CONCORDBIO: Rs-50
- OIL: Rs477
- RBLBANK: Rs620

## Exit-type distribution (best cfg)

- {'EOD': 4, 'SL': 1, 'TARGET': 1}

## Notes
- SL/TGT/EOD split TRAIN = 7/2/11, TEST = 1/1/4.
- TRAIN avg win/avg loss = Rs842/Rs-749; TEST = Rs988/Rs-541.
- Concentration: TEST top-day share 1.356, top-symbol share 1.394, top-trade gross share 0.63.

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
- TRAIN target-fill rate: 10.0% (min 12.0%)
- TEST PF/day-block p: 1.825 / 0.2968 (gate 1.4 / 0.1)