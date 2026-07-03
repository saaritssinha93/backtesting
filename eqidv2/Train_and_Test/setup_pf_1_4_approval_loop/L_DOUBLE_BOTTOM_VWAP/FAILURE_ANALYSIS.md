# L_DOUBLE_BOTTOM_VWAP (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-24 | RICOAUTO | SL | 58 | -1,233 |
| 2026-06-15 | PTC | SL | 155 | -1,232 |
| 2026-06-24 | SWANCORP | SL | 75 | -1,232 |
| 2026-06-15 | J&KBANK | SL | 93 | -1,232 |
| 2026-06-12 | MARINE | SL | 35 | -1,230 |
| 2026-06-12 | MIDHANI | SL | 52 | -1,230 |
| 2026-06-15 | PARKHOSPS | SL | 203 | -1,229 |
| 2026-06-15 | ANANDRATHI | SL | 113 | -1,229 |

## Worst days

- 2026-06-15: Rs-9,873
- 2026-06-24: Rs-2,967
- 2026-06-12: Rs-441
- 2026-06-22: Rs-350
- 2026-06-16: Rs1,647

## Worst symbols

- APARINDS: Rs-2,330
- RICOAUTO: Rs-1,233
- PTC: Rs-1,232
- SWANCORP: Rs-1,232
- J&KBANK: Rs-1,232

## Exit-type distribution (best cfg)

- {'SL': 19, 'EOD': 14, 'TARGET': 9}

## Notes
- SL/TGT/EOD split TRAIN = 94/41/116, TEST = 19/9/14.
- TRAIN avg win/avg loss = Rs817/Rs-882; TEST = Rs921/Rs-956.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.092.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TEST PF below 1.40
- TRAIN concentrated (one trade/day/symbol dominates)
- TEST concentrated (one trade/day/symbol dominates)
- too many trades/day