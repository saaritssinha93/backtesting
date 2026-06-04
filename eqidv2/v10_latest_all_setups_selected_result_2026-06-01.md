# V10 Latest All-Setups Selected Result - 2026-06-01

## Date Range

| Split | Dates |
|---|---|
| Train | 2025-06-02 to 2026-01-31 |
| Validation | 2026-02-01 to 2026-03-31 |
| Holdout/Test | 2026-04-01 to 2026-05-29 |
| Full | 2025-06-02 to 2026-05-29 |

## Full Book Result

Latest selected book uses all 10 setups with the most recent selected/probation rules.

| Book | Split | Trades | PF | PnL Rs | Win % | Avg/Trade Rs |
|---|---|---:|---:|---:|---:|---:|
| Latest selected all setups | Train | 574 | 1.840 | 135,284.51 | 58.54 | 235.69 |
| Latest selected all setups | Validation | 100 | 2.354 | 30,535.67 | 66.00 | 305.36 |
| Latest selected all setups | Holdout/Test | 77 | 1.153 | 4,377.76 | 45.45 | 56.85 |
| Latest selected all setups | Full | 751 | 1.802 | 170,197.94 | 58.19 | 226.63 |
| Baseline all raw v10 | Full | 11,067 | 0.850 | -534,856.86 | 45.17 | -48.33 |

Trade count is reduced by 93.21% versus baseline.

## Setup-Wise Full Result

| Setup | Bucket | Exit | Trades | PF | PnL Rs | Win % |
|---|---|---|---:|---:|---:|---:|
| C_OR_BREAKOUT | PF15 broad watchlist | current | 139 | 1.792 | 31,967.55 | 57.55 |
| D_EMA20_BOUNCE | PF13 low sample | current | 33 | 2.303 | 10,725.84 | 60.61 |
| D_EMA20_REJECTION | probation | current | 83 | 1.653 | 14,484.82 | 55.42 |
| E_ORB_BREAKOUT_LONG | PF13 low sample | current | 28 | 1.798 | 7,668.91 | 57.14 |
| E_ORB_BREAKOUT_SHORT | strict | `SL=0.80,TGT=1.50` | 186 | 1.766 | 46,144.30 | 53.76 |
| E_VWAP_BAND_FADE | weak holdout fail included | current | 96 | 1.310 | 8,201.60 | 60.42 |
| E_VWAP_LOSE_EARLY_SHORT | probation tiny sample | current | 19 | 3.874 | 10,009.83 | 73.68 |
| G_HIGHER_HIGH_BREAK | probation | current | 70 | 1.527 | 9,524.76 | 52.86 |
| L_BB_SQUEEZE_LONG | PF13 low sample | current | 21 | 2.505 | 6,751.84 | 71.43 |
| S_BB_SQUEEZE_SHORT | probation | `SL=0.80,TGT=1.00` | 76 | 2.297 | 24,718.48 | 67.11 |

## Holdout/Test By Setup

| Setup | Trades | PF | PnL Rs | Comment |
|---|---:|---:|---:|---|
| C_OR_BREAKOUT | 0 | n/a | 0.00 | No Apr-May holdout trades |
| D_EMA20_BOUNCE | 5 | 1.566 | 1,040.85 | Positive, low sample |
| D_EMA20_REJECTION | 7 | 0.649 | -1,098.79 | Weak |
| E_ORB_BREAKOUT_LONG | 5 | 2.243 | 1,993.14 | Positive, low sample |
| E_ORB_BREAKOUT_SHORT | 12 | 1.650 | 3,113.12 | Strict survivor |
| E_VWAP_BAND_FADE | 16 | 0.515 | -3,385.49 | Weakest inclusion |
| E_VWAP_LOSE_EARLY_SHORT | 3 | 2.863 | 1,299.18 | Positive, tiny sample |
| G_HIGHER_HIGH_BREAK | 6 | 2.045 | 1,529.80 | Positive, low sample |
| L_BB_SQUEEZE_LONG | 7 | 1.333 | 748.62 | Positive, low sample |
| S_BB_SQUEEZE_SHORT | 16 | 0.852 | -862.67 | Weak |

## Honest Read

This latest all-setups book is much better than raw v10:

- Full PF improves from 0.850 to 1.802.
- Full PnL improves from Rs -534,856.86 to Rs +170,197.94.
- Trades reduce from 11,067 to 751.

The weak point is still holdout quality. Holdout/test is positive, but PF is only 1.153 because `E_VWAP_BAND_FADE`, `D_EMA20_REJECTION`, and `S_BB_SQUEEZE_SHORT` drag it down.

## Output Files

- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\latest_all_setups_selected_summary.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\latest_all_setups_selected_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\latest_all_setups_selected_trades.csv`
