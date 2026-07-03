# B_HUGE_FAILED_BOUNCE (SHORT) — FAILURE_ANALYSIS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline TRAIN book — loser classification

- trades 1628, losers 1159 (71.2%)
- outcome mix: {'SL': 905, 'EOD': 450, 'TARGET': 273}
- loser outcome mix: {'SL': 905, 'EOD': 254}
- avg bars held (win/lose): 97.3 / 60.4

### Net PnL by signal hour (baseline TRAIN)

| hour | n | net Rs | PF proxy |
|---|---|---|---|
| 10:00 | 3 | -843 | 0.55 |
| 11:00 | 317 | -126,494 | 0.35 |
| 12:00 | 485 | -191,378 | 0.34 |
| 13:00 | 498 | -142,934 | 0.46 |
| 14:00 | 325 | -121,424 | 0.31 |

### Worst days (baseline TRAIN)

- 2026-03-13: Rs-24,768
- 2026-04-24: Rs-22,419
- 2026-04-08: Rs-21,250
- 2026-05-06: Rs-20,370
- 2026-04-16: Rs-19,518

### Worst symbols (baseline TRAIN)

- KPRMILL: Rs-5,486
- RKFORGE: Rs-4,654
- FEDFINA: Rs-4,490
- PRIVISCL: Rs-4,365
- STARHEALTH: Rs-4,049

### Worst trades (baseline TRAIN)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-05-14 | MRF | SL | 68 | -1,168 |
| 2026-04-24 | AJMERA | SL | 22 | -932 |
| 2026-03-19 | USHAMART | SL | 37 | -932 |
| 2026-03-30 | JARO | SL | 4 | -932 |
| 2026-04-10 | JSFB | SL | 34 | -932 |
| 2026-04-17 | ASHOKLEY | SL | 79 | -932 |
| 2026-04-07 | ORIENTELEC | SL | 54 | -932 |
| 2026-04-10 | TATAPOWER | SL | 37 | -932 |

## Why rejected candidates failed (from the loop)

- **finalist #1** — TRAIN PF 1.043 n=26 -> TRAIN not in band or too thin (PF 1.043, n 26)
- **finalist #2** — TRAIN PF 0.861 n=25 -> TRAIN not in band or too thin (PF 0.861, n 25)
- **finalist #3** — TRAIN PF 0.811 n=27 -> TRAIN not in band or too thin (PF 0.811, n 27)
- **finalist #4** — TRAIN PF 0.951 n=27 -> TRAIN not in band or too thin (PF 0.951, n 27)
- **finalist #5** — TRAIN PF 0.951 n=27 -> TRAIN not in band or too thin (PF 0.951, n 27)
- **finalist #6** — TRAIN PF 0.733 n=48 -> TRAIN not in band or too thin (PF 0.733, n 48)

## Structural notes

- Pre-momentum issues, indicator weakness, filter/guard weakness and volume/volatility/trend issues are quantified knob-by-knob in PARAMETER_SWEEP_SUMMARY.md (every knob's relaxed/medium/strict variants with FIT/VAL outcomes).
- Fake-breakdown avoidance shows up in the wick/close_loc sweeps; time-of-day weakness in the min_slot/max_slot sweeps; exhaustion in the pre5_mom_r/pre3_range_r premom sweeps.