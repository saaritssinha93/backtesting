# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-25 | MANINFRA | SL | 3 | -832 |
| 2026-06-23 | AYE | SL | 48 | -832 |
| 2026-06-25 | AAVAS | SL | 58 | -831 |
| 2026-06-25 | CONCOR | SL | 67 | -829 |
| 2026-06-25 | ONESOURCE | SL | 98 | -828 |
| 2026-06-22 | CHENNPETRO | EOD | 220 | -591 |
| 2026-06-23 | HDBFS | EOD | 315 | -475 |
| 2026-06-24 | OIL | EOD | 195 | 477 |

## Worst days

- 2026-06-23: Rs-1,307
- 2026-06-25: Rs-956
- 2026-06-22: Rs-591
- 2026-06-24: Rs477

## Worst symbols

- MANINFRA: Rs-832
- AYE: Rs-832
- AAVAS: Rs-831
- CONCOR: Rs-829
- ONESOURCE: Rs-828

## Exit-type distribution (best cfg)

- {'SL': 5, 'EOD': 3, 'TARGET': 1}

## Notes
- SL/TGT/EOD split TRAIN = 14/5/10, TEST = 5/1/3.
- TRAIN avg win/avg loss = Rs1,833/Rs-726; TEST = Rs1,421/Rs-746.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.832.

## Classified failure reasons

- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.40
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 17.2% (min 12.0%)
- TEST PF/day-block p: 0.544 / 0.9483 (gate 1.4 / 0.1)