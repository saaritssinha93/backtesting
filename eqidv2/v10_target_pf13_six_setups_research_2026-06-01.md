# V10 Target PF 1.3 Research - Six Setups - 2026-06-01

## Target Setups

- `C_OR_BREAKOUT`
- `D_EMA20_BOUNCE`
- `E_ORB_BREAKOUT_LONG`
- `E_VWAP_BAND_FADE`
- `E_VWAP_LOSE_EARLY_SHORT`
- `L_BB_SQUEEZE_LONG`

Date range is unchanged:

| Split | Dates |
|---|---|
| Train | 2025-06-02 to 2026-01-31 |
| Validation | 2026-02-01 to 2026-03-31 |
| Holdout/Test | 2026-04-01 to 2026-05-29 |

## Result

All six can be made to show full-history PF > 1.3. The honest quality is mixed: some are low-sample, and `E_VWAP_BAND_FADE` still fails holdout.

| Setup | Rule | Full Trades | Full PF | Full PnL Rs | Holdout Trades | Holdout PF | Status |
|---|---|---:|---:|---:|---:|---:|---|
| C_OR_BREAKOUT | `signal_volume >= 5417.8 AND market_ret_pct >= -1.421 AND vwap_dist_atr >= 14.243662` | 20 | 11.955 | 12,822.00 | 0 | n/a | PF15 full-history add, no holdout |
| D_EMA20_BOUNCE | `(vol_ratio <= 1.5975512 OR vwap_dist_atr >= -0.38557115) AND signal_minute <= 705` | 33 | 2.303 | 10,725.84 | 5 | 1.566 | PF13 pass, low sample |
| E_ORB_BREAKOUT_LONG | `v7_signal_notional_rs >= 99937.32` | 28 | 1.798 | 7,668.91 | 5 | 2.243 | PF13 pass, low sample |
| E_VWAP_BAND_FADE | `signal_range_pct >= 0.85017508 AND lower_wick_pct >= 0.13144439 AND atr_pct >= 0.0071656647` | 96 | 1.310 | 8,201.60 | 16 | 0.515 | Full PF only, holdout fail |
| E_VWAP_LOSE_EARLY_SHORT | `regime == BEAR` | 19 | 3.874 | 10,009.83 | 3 | 2.863 | Probation, very low sample |
| L_BB_SQUEEZE_LONG | `(market_abs_ret_pct <= 0.74284715 OR vol_ratio <= 3.0227043) AND ranker_score >= 0.7332456` | 21 | 2.505 | 6,751.84 | 7 | 1.333 | PF13 pass, low sample |

## Combined Six-Setup Basket

Using the selected current-exit rules above:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 135 | 2.320 | 40,825.11 | 65.19 |
| Validation | 46 | 2.490 | 13,658.61 | 71.74 |
| Holdout/Test | 36 | 1.127 | 1,696.30 | 47.22 |
| Full | 217 | 2.051 | 56,180.03 | 63.59 |

This basket clears full PF > 1.3 comfortably. Holdout is positive but not strong. The main drag is `E_VWAP_BAND_FADE`.

## C_OR_BREAKOUT PF15 Add

`C_OR_BREAKOUT` can be added to the above list as a PF > 1.5 full-history candidate:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 17 | 9.957 | 10,483.40 | 70.59 |
| Validation | 3 | inf | 2,338.60 | 100.00 |
| Holdout/Test | 0 | n/a | 0.00 | n/a |
| Full | 20 | 11.955 | 12,822.00 | 75.00 |

The rule is:

`signal_volume >= 5417.8 AND market_ret_pct >= -1.421 AND vwap_dist_atr >= 14.243662`

This is a very strong historical filter, but it only traded across 4 historical days and has no Apr-May holdout trades. I would include it as an active watchlist/probation add, not a strict OOS-confirmed rule.

## C_OR_BREAKOUT Higher-Trade PF 1.5-2.0 Version

To increase trade count while keeping PF between 1.5 and 2.0, use all `C_OR_BREAKOUT` trades:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 135 | 1.783 | 30,731.03 | 57.04 |
| Validation | 4 | 2.122 | 1,236.52 | 75.00 |
| Holdout/Test | 0 | n/a | 0.00 | n/a |
| Full | 139 | 1.792 | 31,967.55 | 57.55 |

Rule:

`setup == C_OR_BREAKOUT`

This is the better choice if the goal is more trades with PF still above 1.5 and below 2.0. It still has the same weakness: no Apr-May holdout trades.

### Basket With Broad C_OR_BREAKOUT

Replacing the tight C filter with broad `C_OR_BREAKOUT`:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 253 | 1.885 | 61,072.74 | 60.47 |
| Validation | 47 | 2.223 | 12,556.53 | 70.21 |
| Holdout/Test | 36 | 1.127 | 1,696.30 | 47.22 |
| Full | 336 | 1.813 | 75,325.58 | 60.42 |

## Exit Check

I also checked exit retune on the selected candidates. Only `E_VWAP_BAND_FADE` found an eligible retune:

| Setup | Exit | Full PF | Holdout PF | Comment |
|---|---|---:|---:|---|
| E_VWAP_BAND_FADE | current | 1.310 | 0.515 | Selected PF13 rule |
| E_VWAP_BAND_FADE | `SL=0.70,TGT=0.60` | 1.309 | 0.515 | No real improvement |

So exit tuning did not rescue `E_VWAP_BAND_FADE`.

## Honest Recommendation

For PF > 1.3 target:

1. Use `D_EMA20_BOUNCE`, `E_ORB_BREAKOUT_LONG`, and `L_BB_SQUEEZE_LONG` as low-sample PF13 candidates.
2. Use `E_VWAP_LOSE_EARLY_SHORT` only as probation because validation and holdout sample are tiny.
3. Keep `C_OR_BREAKOUT` in a watchlist, not active production, because it has zero Apr-May holdout trades.
4. Treat `E_VWAP_BAND_FADE` as the weakest. It reaches full PF 1.31, but holdout PF is only 0.515.

## Files

- `research_v10_target_setups_pf13.py`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\target_pf13_selected_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\target_pf13_option_results.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\target_pf13_selected_exit_check.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\target_pf13_six_setup_portfolio.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\target_pf13_with_C_OR_BREAKOUT_pf15_portfolio.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\target_pf13_with_C_OR_BREAKOUT_broad_pf15_2_portfolio.csv`
