# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline TRAIN book — loser classification

- trades 550, losers 368 (66.9%)
- outcome mix: {'SL': 331, 'TARGET': 140, 'EOD': 79}
- loser outcome mix: {'SL': 331, 'EOD': 37}
- avg bars held (win/lose): 69.1 / 42.7

### Net PnL by signal hour (baseline TRAIN)

| hour | n | net Rs | PF proxy |
|---|---|---|---|
| 10:00 | 2 | -1,861 | 0.0 |
| 11:00 | 130 | -34,686 | 0.54 |
| 12:00 | 152 | -43,028 | 0.51 |
| 13:00 | 154 | -44,601 | 0.51 |
| 14:00 | 112 | -37,025 | 0.41 |

### Worst days (baseline TRAIN)

- 2026-05-06: Rs-18,018
- 2026-05-21: Rs-15,025
- 2026-03-27: Rs-13,865
- 2026-05-07: Rs-13,102
- 2026-03-19: Rs-12,713

### Worst symbols (baseline TRAIN)

- PRIVISCL: Rs-3,681
- AHLUCONT: Rs-3,247
- AYE: Rs-2,796
- GOKEX: Rs-2,787
- FINCABLES: Rs-2,780

### Worst trades (baseline TRAIN)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-03-02 | INOXGREEN | SL | 4 | -933 |
| 2026-03-20 | INDRAMEDCO | SL | 78 | -933 |
| 2026-03-27 | V2RETAIL | SL | 2 | -933 |
| 2026-05-12 | APTUS | SL | 13 | -933 |
| 2026-03-30 | RGL | SL | 7 | -933 |
| 2026-04-30 | SAMMAANCAP | SL | 38 | -933 |
| 2026-03-06 | SAIL | SL | 17 | -933 |
| 2026-05-05 | V2RETAIL | SL | 89 | -933 |

## Why rejected candidates failed (from the loop)

- **finalist #1** — TRAIN PF 2.104 n=26 -> TRAIN not in band or too thin (PF 2.104, n 26)
- **finalist #2** — TRAIN PF 1.53 n=26, TEST PF 0.524 n=8 -> TEST PF 0.524 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.7626 > 0.1; threshold-neighborhood robustness failed; term-dropout robustness failed
- **finalist #3** — TRAIN PF 1.53 n=26, TEST PF 0.524 n=8 -> TEST PF 0.524 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.7626 > 0.1; threshold-neighborhood robustness failed; term-dropout robustness failed
- **finalist #4** — TRAIN PF 1.279 n=37 -> TRAIN not in band or too thin (PF 1.279, n 37)
- **finalist #5** — TRAIN PF 1.411 n=36, TEST PF 0.249 n=11 -> TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.249 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.9608 > 0.1; threshold-neighborhood robustness failed; term-dropout robustness failed
- **finalist #6** — TRAIN PF 1.411 n=36, TEST PF 0.249 n=11 -> TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.249 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.9608 > 0.1; threshold-neighborhood robustness failed; term-dropout robustness failed
- **R1-premom-off** — TRAIN PF 1.303 n=54, TEST PF 0.812 n=23 -> TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.812 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.8879 > 0.1; threshold-neighborhood robustness failed

## Structural notes

- Pre-momentum issues, indicator weakness, filter/guard weakness and volume/volatility/trend issues are quantified knob-by-knob in PARAMETER_SWEEP_SUMMARY.md (every knob's relaxed/medium/strict variants with FIT/VAL outcomes).
- Fake-breakdown avoidance shows up in the wick/close_loc sweeps; time-of-day weakness in the min_slot/max_slot sweeps; exhaustion in the pre5_mom_r/pre3_range_r premom sweeps.