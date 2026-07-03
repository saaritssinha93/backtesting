# G_LOWER_LOW_BREAK (SHORT) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-22 | PRICOLLTD | SL | 85 | -1,432 |
| 2026-06-22 | HGINFRA | EOD | 169 | -731 |
| 2026-06-15 | APEX | EOD | 119 | -124 |
| 2026-06-15 | UNIMECH | EOD | 170 | 124 |
| 2026-06-15 | JYOTICNC | TARGET | 52 | 762 |

## Worst days

- 2026-06-22: Rs-2,164
- 2026-06-15: Rs762

## Worst symbols

- PRICOLLTD: Rs-1,432
- HGINFRA: Rs-731
- APEX: Rs-124
- UNIMECH: Rs124
- JYOTICNC: Rs762

## Exit-type distribution (best cfg)

- {'EOD': 3, 'TARGET': 1, 'SL': 1}

## Notes
- SL/TGT/EOD split TRAIN = 4/23/28, TEST = 1/1/3.
- TRAIN avg win/avg loss = Rs644/Rs-557; TEST = Rs443/Rs-763.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.86.

## Classified failure reasons

- too few trades (test_n<6)
- TRAIN PF too high / overfit risk (>1.70)
- TEST PF below 1.40
- TEST concentrated (one trade/day/symbol dominates)