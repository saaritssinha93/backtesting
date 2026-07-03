# Train-PF Optimized FAST MOMENTUM LONG

Verdict: **RESEARCH ONLY / DO NOT TRADE**.

The requested TRAIN PF > 1.4 is achievable only with very selective filters. The selected split-checked variant clears FIT, VAL, and TRAIN, but fails TEST badly.

## Selected Split-Checked Parameters

- source rule: `LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5`
- exit: `sl0.75_tgt1_tb3`
- top_n_per_slot: `None`
- predicates: `atr_pct 0.8-2.25, adx>=36, atr_pct 0.3-1, rsi>=70, slot 2-24`
- effective filters: `{"base_rule": {"adx_min": 12.0, "atr_pct_max": 2.25, "atr_pct_min": 0.16, "close_loc_min": 0.65, "dist_vwap_abs_max": 1.6, "green_body_pct_min": 0.12, "green_streak_3_max": 2, "prior_high_break_bars": 5, "rsi_min": 48.0, "vol_ratio_min": 2.0, "vol_rising_2": true}, "extra_filters": {"adx_min": 36.0, "atr_pct_max": 1.0, "atr_pct_min": 0.8, "rsi_min": 70.0, "slot_max": 24.0, "slot_min": 2.0}}`

## Metrics

| split | trades | PF | WR% | net PnL |
|---|---:|---:|---:|---:|
| FIT | 8 | 3.3426 | 75.0 | 2709.42 |
| VAL | 17 | 2.227 | 70.59 | 4319.03 |
| TRAIN | 25 | 2.5029 | 72.0 | 7028.45 |
| TEST | 12 | 0.2965 | 25.0 | -5443.35 |

## Best Train-Only Pocket

- exit: `sl0.75_tgt1_tb9`
- top_n_per_slot: `2`
- predicates: `atr_pct 0.8-2.25, slot 2-10, atr_pct 0.3-1, adx>=24, ema20_slope_pct>=0`
- TRAIN: trades `27`, PF `2.6876`, WR `70.37`, net `8471.2`
- TEST: trades `22`, PF `0.2143`, WR `22.73`, net `-12680.47`

## Why Not Promote

- TEST PF remains far below 1.0.
- Trade count is very small after filters.
- The filter pocket is concentrated in early-session high-ATR/high-RSI breakouts.
- This is an in-sample parameter match, not a live-ready edge.

## Files

- config JSON: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\long_setup_discovery_from_raw_data\candidates\FAST_MOMENTUM_LONG_TRAIN_PF_OPTIMIZED_config.json`
- top configs CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\long_setup_discovery_from_raw_data\results\train_pf_optimization_top_configs.csv`
- train trades CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\long_setup_discovery_from_raw_data\results\train_pf_optimized_train_trades.csv`
- test trades CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\long_setup_discovery_from_raw_data\results\train_pf_optimized_test_trades.csv`
