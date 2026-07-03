# A_MOD_BREAK_C1_LOW (SHORT) — FAILURE_ANALYSIS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline TRAIN book — loser classification

- trades 164, losers 90 (54.9%)
- outcome mix: {'TARGET': 59, 'SL': 56, 'EOD': 49}
- loser outcome mix: {'SL': 56, 'EOD': 34}
- avg bars held (win/lose): 97.2 / 86.1

### Net PnL by signal hour (baseline TRAIN)

| hour | n | net Rs | PF proxy |
|---|---|---|---|
| 11:00 | 35 | -10,938 | 0.49 |
| 12:00 | 55 | -9,727 | 0.65 |
| 13:00 | 45 | -15,031 | 0.44 |
| 14:00 | 29 | -5,198 | 0.6 |

### Worst days (baseline TRAIN)

- 2026-03-23: Rs-8,274
- 2026-05-06: Rs-6,253
- 2026-03-09: Rs-5,088
- 2026-03-11: Rs-4,301
- 2026-04-24: Rs-3,932

### Worst symbols (baseline TRAIN)

- ACI: Rs-2,654
- GOODLUCK: Rs-2,647
- JARO: Rs-1,667
- ANTELOPUS: Rs-1,469
- SPMLINFRA: Rs-1,332

### Worst trades (baseline TRAIN)

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-03-09 | SPMLINFRA | SL | 51 | -1,332 |
| 2026-03-02 | ASHOKA | SL | 136 | -1,332 |
| 2026-03-05 | LXCHEM | SL | 43 | -1,332 |
| 2026-05-26 | GOCLCORP | SL | 9 | -1,332 |
| 2026-03-18 | SMSPHARMA | SL | 119 | -1,332 |
| 2026-03-13 | SGMART | SL | 59 | -1,331 |
| 2026-03-23 | VIYASH | SL | 34 | -1,331 |
| 2026-03-11 | MEESHO | SL | 8 | -1,331 |

## Why rejected candidates failed (from the loop)

- **finalist #1** — TRAIN PF 0.93 n=85 -> TRAIN not in band or too thin (PF 0.93, n 85)
- **finalist #2** — TRAIN PF 0.924 n=86 -> TRAIN not in band or too thin (PF 0.924, n 86)
- **finalist #3** — TRAIN PF 0.883 n=86 -> TRAIN not in band or too thin (PF 0.883, n 86)
- **finalist #4** — TRAIN PF 0.838 n=86 -> TRAIN not in band or too thin (PF 0.838, n 86)
- **finalist #5** — TRAIN PF 0.843 n=85 -> TRAIN not in band or too thin (PF 0.843, n 85)
- **finalist #6** — TRAIN PF 0.92 n=84 -> TRAIN not in band or too thin (PF 0.92, n 84)

## Structural notes

- Pre-momentum issues, indicator weakness, filter/guard weakness and volume/volatility/trend issues are quantified knob-by-knob in PARAMETER_SWEEP_SUMMARY.md (every knob's relaxed/medium/strict variants with FIT/VAL outcomes).
- Fake-breakdown avoidance shows up in the wick/close_loc sweeps; time-of-day weakness in the min_slot/max_slot sweeps; exhaustion in the pre5_mom_r/pre3_range_r premom sweeps.

## PHASE 2 — enriched-search failure evidence (added 2026-07-03)

- 846 standalone feature scans: best single slice PF 0.48/0.55 (gap_pct) — 0 slices at PF>=1.0 on both FIT and VAL.
- 1200 TPE trials found 3-term TRAIN-band cohorts (PF 1.41-1.80, n 22-42) — ALL failed the TRAIN day-domination cap (0.50-0.81 vs 0.40) and ALL collapsed on TEST (PF 0.049-0.283, 0 target fills, every book net-negative).
- Interpretation: with a base population PF of ~0.40, any cohort that reaches PF 1.3+ in-sample is a handful of lucky day-clustered trades; the OOS month falsifies every one. This mirrors the P_PDH structural-wall finding (2026-06-30).
