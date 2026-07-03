# B_HUGE_RED_FAILED_BOUNCE (SHORT) — FAILURE_ANALYSIS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline TRAIN book — loser classification

- trades 48, losers 26 (54.2%)
- outcome mix: {'EOD': 23, 'SL': 14, 'TARGET': 11}
- loser outcome mix: {'SL': 14, 'EOD': 12}
- avg bars held (win/lose): 98.3 / 85.1

### Net PnL by signal hour (baseline TRAIN)

| hour | n | net Rs | PF proxy |
|---|---|---|---|
| 11:00 | 8 | 206 | 1.06 |
| 12:00 | 12 | -2,351 | 0.54 |
| 13:00 | 18 | 1,239 | 1.2 |
| 14:00 | 10 | -4,894 | 0.18 |

### Worst days (baseline TRAIN)

- 2026-04-30: Rs-2,897
- 2026-05-21: Rs-2,357
- 2026-04-07: Rs-2,256
- 2026-04-27: Rs-1,711
- 2026-05-15: Rs-1,220

### Worst symbols (baseline TRAIN)

- LATENTVIEW: Rs-1,132
- TRITURBINE: Rs-1,131
- PATANJALI: Rs-1,131
- FEDFINA: Rs-1,131
- EIHOTEL: Rs-1,130

### Worst trades (baseline TRAIN)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-04-30 | LATENTVIEW | SL | 71 | -1,132 |
| 2026-05-21 | TRITURBINE | SL | 16 | -1,131 |
| 2026-04-07 | PATANJALI | SL | 58 | -1,131 |
| 2026-04-30 | FEDFINA | SL | 13 | -1,131 |
| 2026-04-16 | EIHOTEL | SL | 113 | -1,130 |
| 2026-05-25 | GODREJCP | SL | 67 | -1,125 |
| 2026-04-07 | AURIONPRO | SL | 49 | -1,125 |
| 2026-04-30 | FORCEMOT | SL | 31 | -1,125 |

## Why rejected candidates failed (from the loop)

- **finalist #1** — TRAIN PF 0.799 n=23 -> TRAIN not in band or too thin (PF 0.799, n 23)
- **finalist #2** — TRAIN PF 0.799 n=23 -> TRAIN not in band or too thin (PF 0.799, n 23)
- **finalist #3** — TRAIN PF 0.692 n=90 -> TRAIN not in band or too thin (PF 0.692, n 90)
- **finalist #4** — TRAIN PF 0.611 n=29 -> TRAIN not in band or too thin (PF 0.611, n 29)
- **finalist #5** — TRAIN PF 0.58 n=79 -> TRAIN not in band or too thin (PF 0.58, n 79)
- **finalist #6** — TRAIN PF 0.559 n=34 -> TRAIN not in band or too thin (PF 0.559, n 34)

## Structural notes

- Pre-momentum issues, indicator weakness, filter/guard weakness and volume/volatility/trend issues are quantified knob-by-knob in PARAMETER_SWEEP_SUMMARY.md (every knob's relaxed/medium/strict variants with FIT/VAL outcomes).
- Fake-breakdown avoidance shows up in the wick/close_loc sweeps; time-of-day weakness in the min_slot/max_slot sweeps; exhaustion in the pre5_mom_r/pre3_range_r premom sweeps.