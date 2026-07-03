# A_MOD_BREAK_C1_LOW Optimization Report

Status: **NOT SELECTED**

## Windows

- FIT: 2026-05-27..2026-06-03 (2026-05-27, 2026-05-29, 2026-06-01, 2026-06-02, 2026-06-03)
- VAL: 2026-06-04..2026-06-10 (2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10)
- TRAIN: 2026-05-27..2026-06-10
- TEST: 2026-06-12..2026-06-24

TEST was evaluated only after the FIT/VAL search loop finished.

## Engine

- Trials run: 300 of requested 300
- Search engine: Optuna TPE
- Best FIT/VAL score: 0.740631
- Command: `python Train_and_Test\optimize_setup_card_loop.py --setup A_MOD_BREAK_C1_LOW --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\A_MOD_BREAK_C1_LOW --trials 300 --time_budget_min 20 --search_mode exit_only`

## Metrics

| Book | net Rs | PF | trades |
|---|---|---|---|
| Baseline TRAIN | -1748.34 | 0.6133 | 11 |
| Baseline TEST | -2352.24 | 0.5323 | 10 |
| Best TRAIN | 4261.44 | 1.5426 | 22 |
| Best TEST | -7691.34 | 0.246 | 18 |

## Clean OOS Verdict

- Clean TEST verdict: **no train-side pass**
- Train gate: FAIL (top day net share 1.0209>0.4; top symbol net share 0.5277>0.4)
- Test gate: FAIL (net -7691.34<=0.0; PF 0.246<1.3; max trades/day 7>6)

## Changed Knobs vs Original

sl: 1.1 -> 0.85<br>tgt: 1.0 -> 2.5

## Best Config

```json
{
  "name": "trial_0",
  "sl": 0.85,
  "tgt": 2.5,
  "mask_terms": [
    [
      "vol_ratio",
      ">=",
      1.955814
    ]
  ],
  "premom_terms": [
    [
      "pre5_mom_r",
      ">=",
      0.425861
    ],
    [
      "pre3_range_r",
      "<=",
      0.202087
    ]
  ],
  "guard": {},
  "max_positions": 20,
  "daily_loss_rs": 0.0,
  "regime_align": false,
  "regime_band": 0.0
}
```

## Live Crosscheck / Known Mismatch Notes

### A_MOD_BREAK_C1_LOW (SHORT) — *active — mined short*
| **A_MOD_BREAK_C1_LOW** | mask `vol_ratio≥1.956` + pre-mom (`pre5_mom_r≥0.426, pre3_range_r≤0.202`) | `abs(rs_pct)≥9.2 & vol_ratio≥1.80` | ⛔ Different gate; **no pre-momentum** |
| A_MOD_BREAK_C1_LOW | SHORT | train 38 / PF 2.58, test 30 / PF 2.83 |

## Source Card

```text
### A_MOD_BREAK_C1_LOW (SHORT) — *active — mined short*
- **Logic:** break of the prior C1 (first-candle) low — momentum-down continuation out of a TIGHT pre-break range.
- **Detection:** production clean-pool scanner raw_candidates (corrected VWAP).
- **Indicators:** vol_ratio, pre-momentum, range features.
- **Filters (mask):** `vol_ratio≥1.955814`.
- **Gates (pre-mom, ALL, missing→block):** `pre5_mom_r≥0.425861`, `pre3_range_r≤0.202087`.
- **Guards:** none. **Exit:** SL 1.10 / Tgt 1.00 (alt 0.90/1.00 cleaner day-spread). **Status:** STRONG PROBATION (monotone sensitivity, 88% months).

```

## Artifacts

- trials.csv: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\results\A_MOD_BREAK_C1_LOW\trials.csv`
- best_config.json: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\results\A_MOD_BREAK_C1_LOW\best_config.json`
- equity_train.png: written
- equity_test.png: written

No live execution was performed. No final_setup_conf.py edit was made.
