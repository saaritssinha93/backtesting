# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline TRAIN book — loser classification

- trades 743, losers 497 (66.9%)
- outcome mix: {'SL': 326, 'EOD': 247, 'TARGET': 170}
- loser outcome mix: {'SL': 326, 'EOD': 171}
- avg bars held (win/lose): 84.2 / 80.1

### Net PnL by signal hour (baseline TRAIN)

| hour | n | net Rs | PF proxy |
|---|---|---|---|
| 10:00 | 1 | 1,210 | inf |
| 11:00 | 207 | -92,289 | 0.42 |
| 12:00 | 221 | -50,878 | 0.62 |
| 13:00 | 185 | -51,788 | 0.55 |
| 14:00 | 129 | -53,846 | 0.33 |

### Worst days (baseline TRAIN)

- 2026-05-25: Rs-20,843
- 2026-05-08: Rs-16,477
- 2026-04-20: Rs-15,570
- 2026-04-23: Rs-14,805
- 2026-04-28: Rs-14,661

### Worst symbols (baseline TRAIN)

- INDOTHAI: Rs-5,791
- MUFIN: Rs-4,928
- JUBLPHARMA: Rs-4,490
- AYE: Rs-3,695
- HARIOMPIPE: Rs-3,692

### Worst trades (baseline TRAIN)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-03-10 | SAPPHIRE | SL | 12 | -1,233 |
| 2026-03-04 | FRACTAL | SL | 5 | -1,233 |
| 2026-05-11 | NUVAMA | SL | 41 | -1,233 |
| 2026-03-09 | MANAKCOAT | SL | 98 | -1,233 |
| 2026-03-17 | UGROCAP | SL | 37 | -1,233 |
| 2026-05-15 | HEMIPROP | SL | 95 | -1,233 |
| 2026-04-27 | MUFIN | SL | 59 | -1,233 |
| 2026-03-17 | J&KBANK | SL | 22 | -1,233 |

## Why rejected candidates failed (from the loop)

- **finalist #1** — TRAIN PF 1.698 n=21, TEST PF 0.166 n=3 -> TRAIN domination (trade>35% gross or day/sym>40% net); TEST n 3 < 5
- **finalist #2** — TRAIN PF 1.371 n=21, TEST PF 0.166 n=3 -> TRAIN domination (trade>35% gross or day/sym>40% net); TEST n 3 < 5; threshold-neighborhood robustness failed; term-dropout robustness failed
- **finalist #3** — TRAIN PF 1.287 n=21 -> TRAIN not in band or too thin (PF 1.287, n 21)
- **finalist #4** — TRAIN PF 1.201 n=21 -> TRAIN not in band or too thin (PF 1.201, n 21)
- **finalist #5** — TRAIN PF 1.122 n=28 -> TRAIN not in band or too thin (PF 1.122, n 28)
- **finalist #6** — TRAIN PF 1.158 n=28 -> TRAIN not in band or too thin (PF 1.158, n 28)
- **R3-window-{"max_slot": "12:00"}** — TRAIN PF 1.698 n=21, TEST PF 0.166 n=3 -> TRAIN domination (trade>35% gross or day/sym>40% net); TEST n 3 < 5

## Structural notes

- Pre-momentum issues, indicator weakness, filter/guard weakness and volume/volatility/trend issues are quantified knob-by-knob in PARAMETER_SWEEP_SUMMARY.md (every knob's relaxed/medium/strict variants with FIT/VAL outcomes).
- Fake-breakdown avoidance shows up in the wick/close_loc sweeps; time-of-day weakness in the min_slot/max_slot sweeps; exhaustion in the pre5_mom_r/pre3_range_r premom sweeps.