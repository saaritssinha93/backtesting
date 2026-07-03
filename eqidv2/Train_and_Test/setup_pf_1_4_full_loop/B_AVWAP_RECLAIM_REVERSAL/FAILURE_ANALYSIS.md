# B_AVWAP_RECLAIM_REVERSAL (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline TRAIN book — loser classification

- trades 1771, losers 1340 (75.7%)
- outcome mix: {'SL': 1035, 'EOD': 491, 'TARGET': 245}
- loser outcome mix: {'SL': 1035, 'EOD': 305}
- avg bars held (win/lose): 107.8 / 77.7

### Net PnL by signal hour (baseline TRAIN)

| hour | n | net Rs | PF proxy |
|---|---|---|---|
| 10:00 | 11 | -8,001 | 0.14 |
| 11:00 | 613 | -249,378 | 0.37 |
| 12:00 | 562 | -221,462 | 0.35 |
| 13:00 | 352 | -126,477 | 0.36 |
| 14:00 | 233 | -89,070 | 0.3 |

### Worst days (baseline TRAIN)

- 2026-03-25: Rs-33,570
- 2026-05-04: Rs-33,405
- 2026-05-13: Rs-30,939
- 2026-04-23: Rs-29,084
- 2026-05-15: Rs-28,607

### Worst symbols (baseline TRAIN)

- VIYASH: Rs-5,590
- RAIN: Rs-5,402
- VBL: Rs-4,885
- BAJFINANCE: Rs-4,778
- CGCL: Rs-4,401

### Worst trades (baseline TRAIN)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-03-13 | MRF | SL | 75 | -1,256 |
| 2026-03-18 | EDELWEISS | SL | 204 | -933 |
| 2026-05-13 | ADFFOODS | SL | 75 | -933 |
| 2026-05-29 | KALAMANDIR | SL | 121 | -933 |
| 2026-03-05 | ELECON | SL | 52 | -933 |
| 2026-03-24 | 63MOONS | SL | 5 | -933 |
| 2026-03-25 | TVSSCS-BE | SL | 3 | -933 |
| 2026-03-06 | FIRSTCRY | SL | 83 | -933 |

## Why rejected candidates failed (from the loop)

- **finalist #1** — TRAIN PF 0.553 n=167 -> TRAIN not in band or too thin (PF 0.553, n 167)
- **finalist #2** — TRAIN PF 0.531 n=24 -> TRAIN not in band or too thin (PF 0.531, n 24)
- **finalist #3** — TRAIN PF 0.515 n=28 -> TRAIN not in band or too thin (PF 0.515, n 28)
- **finalist #4** — TRAIN PF 0.478 n=200 -> TRAIN not in band or too thin (PF 0.478, n 200)
- **finalist #5** — TRAIN PF 0.489 n=24 -> TRAIN not in band or too thin (PF 0.489, n 24)
- **finalist #6** — TRAIN PF 0.496 n=205 -> TRAIN not in band or too thin (PF 0.496, n 205)

## Structural notes

- Pre-momentum issues, indicator weakness, filter/guard weakness and volume/volatility/trend issues are quantified knob-by-knob in PARAMETER_SWEEP_SUMMARY.md (every knob's relaxed/medium/strict variants with FIT/VAL outcomes).
- Fake-breakdown avoidance shows up in the wick/close_loc sweeps; time-of-day weakness in the min_slot/max_slot sweeps; exhaustion in the pre5_mom_r/pre3_range_r premom sweeps.