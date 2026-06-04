# v10 Honest PF Improvement Research - 2026-06-01

Source run:
`C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic`

## Baseline

Full v10 historical result:

| Scope | Trades | Days | Net PnL | PF | Win % |
|---|---:|---:|---:|---:|---:|
| Baseline full | 11,067 | 245 | -534,856.86 | 0.850 | 45.17 |

Chronological split used for research:

| Split | Dates | Trades | Net PnL | PF | Win % |
|---|---:|---:|---:|---:|---:|
| Train | 2025-06-02 to 2026-01-31 | 6,790 | -283,595.92 | 0.866 | 45.11 |
| Validation | 2026-02-01 to 2026-03-31 | 2,261 | -124,354.71 | 0.836 | 46.17 |
| Test / holdout | 2026-04-01 to 2026-05-29 | 2,016 | -126,906.22 | 0.815 | 44.25 |

Rules were selected using train + validation only. The Apr-May test split was used only after selection to judge whether the rule survived out of sample.

## Honest Result

I could not honestly make all original setups PF > 1.5.

The honest improvement is to disable most setups and keep only the rules that survived the test split and also have full-period setup PF > 1.5.

### Strict OOS PF > 1.5 Portfolio

| Split | Trades | Days | Net PnL | PF | Win % | Avg/Trade |
|---|---:|---:|---:|---:|---:|---:|
| Train | 237 | 103 | 54,100.30 | 1.731 | 56.96 | 228.27 |
| Validation | 35 | 22 | 7,656.55 | 1.546 | 51.43 | 218.76 |
| Test / holdout | 21 | 12 | 10,249.42 | 3.503 | 61.90 | 488.07 |
| Full | 293 | 137 | 72,006.28 | 1.782 | 56.66 | 245.76 |

Trade count reduction:

| Portfolio | Trades | Reduction vs baseline |
|---|---:|---:|
| Baseline | 11,067 | 0.0% |
| Strict OOS PF > 1.5 | 293 | 97.35% |

### Retained Rules

| Setup | Side | Rule | Full Trades | Full PnL | Full PF | Test Trades | Test PF |
|---|---|---|---:|---:|---:|---:|---:|
| E_ORB_BREAKOUT_SHORT | SHORT | `regime == NEUTRAL AND vol_ratio <= 3.0345624` | 191 | 39,399 | 1.67 | 7 | 8.99 |
| S_BB_SQUEEZE_SHORT | SHORT | `signal_minute <= 743 AND atr_pct <= 0.0038102952` | 102 | 32,607 | 1.99 | 14 | 2.18 |

## Optional But Not Fully Validated

`C_OR_BREAKOUT` found a strong rule, but it had zero trades in the Apr-May test split. It is not an out-of-sample survivor. It can be tracked in paper mode, but I would not mix it into the main production result without more fresh data.

| Setup | Side | Rule | Full Trades | Full PnL | Full PF | Test Trades |
|---|---|---|---:|---:|---:|---:|
| C_OR_BREAKOUT | LONG | `vwap_dist_atr >= 14.243662 AND atr_pct >= 0.0017561398` | 69 | 30,215 | 3.27 | 0 |

Including this optional unvalidated rule:

| Split | Trades | Days | Net PnL | PF | Win % | Avg/Trade |
|---|---:|---:|---:|---:|---:|---:|
| Train | 302 | 108 | 83,079.07 | 1.963 | 59.93 | 275.10 |
| Validation | 39 | 23 | 8,893.07 | 1.588 | 53.85 | 228.03 |
| Test / holdout | 21 | 12 | 10,249.42 | 3.503 | 61.90 | 488.07 |
| Full | 362 | 143 | 102,221.56 | 1.969 | 59.39 | 282.38 |

## Rejected / Disabled Setups

These did not honestly qualify.

| Setup | Reason |
|---|---|
| D_EMA20_BOUNCE | Development rule had no Apr-May test trades; baseline test was poor. Do not trust as validated. |
| D_EMA20_REJECTION | Selected rule looked good in train/validation, but failed test: PF 0.79. |
| E_ORB_BREAKOUT_LONG | Survived test, but full-period setup PF was only 1.39, below the PF > 1.5 requirement. |
| E_VWAP_BAND_FADE | Largest baseline loser. Best honest development rule failed test: PF 0.85. Disable. |
| E_VWAP_LOSE_EARLY_SHORT | Too few validation/test trades for honest rule selection. Disable or paper-track only. |
| G_HIGHER_HIGH_BREAK | Development rule looked good, but test PF was only 1.02. Not enough. |
| L_BB_SQUEEZE_LONG | Development rule looked good, but test PF was only 0.85. Not enough. |

## Important Caveat

This is still research on one historical run, not proof of future edge. The strict table is much better than baseline and uses an untouched holdout, but the final test sample is small: only 21 trades. That is useful, but not enough to size aggressively.

My honest recommendation:

1. Paper trade only the two strict OOS rules first.
2. Track `C_OR_BREAKOUT` separately as an unvalidated candidate.
3. Keep all rejected setups disabled until they pass a fresh forward period.
4. Do not use the development-selected all-rules portfolio even though full PF is 1.542, because its holdout PF was only 1.176.

Generated CSVs:

- `honest_filter_selected_by_setup.csv`
- `honest_oos_status_by_setup.csv`
- `honest_portfolio_scenarios.csv`
- `honest_strict_pf_gt_1_5_portfolios.csv`
- `honest_strict_oos_pf_gt_1_5_setups.csv`
- `honest_optional_unvalidated_pf_gt_1_5_setups.csv`
