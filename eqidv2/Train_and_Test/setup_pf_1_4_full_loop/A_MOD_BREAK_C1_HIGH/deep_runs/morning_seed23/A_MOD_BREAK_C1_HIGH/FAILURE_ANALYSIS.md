# A_MOD_BREAK_C1_HIGH (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-03._

## Worst trades (best-config book)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-06-22 | MANINFRA | SL | 57 | -1,732 |
| 2026-06-18 | SURYAROSNI | SL | 152 | -1,731 |
| 2026-06-15 | AJMERA | SL | 9 | -1,730 |
| 2026-06-23 | GARUDA | SL | 56 | -1,730 |
| 2026-06-10 | ASTERDM | SL | 199 | -1,729 |
| 2026-06-10 | SMSPHARMA | SL | 76 | -1,729 |
| 2026-07-01 | MIDHANI | SL | 249 | -1,727 |
| 2026-06-11 | NELCO | SL | 33 | -1,722 |

## Worst days

- 2026-06-10: Rs-3,457
- 2026-06-23: Rs-3,446
- 2026-06-18: Rs-2,756
- 2026-06-22: Rs-1,732
- 2026-06-15: Rs-1,730

## Worst symbols

- MANINFRA: Rs-1,732
- SURYAROSNI: Rs-1,731
- AJMERA: Rs-1,730
- GARUDA: Rs-1,730
- ASTERDM: Rs-1,729

## Exit-type distribution (best cfg)

- {'SL': 11, 'EOD': 4, 'TARGET': 2}

## Notes
- SL/TGT/EOD split TRAIN = 5/11/7, TEST = 11/2/4.
- TRAIN avg win/avg loss = Rs1,522/Rs-1,426; TEST = Rs1,469/Rs-1,630.
- Concentration: TEST top-day share 9.99, top-symbol share 9.99, top-trade gross share 0.301.

## Classified failure reasons

- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.40
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 47.8% (min 12.0%)
- TEST PF/day-block p: 0.277 / 0.9901 (gate 1.4 / 0.1)