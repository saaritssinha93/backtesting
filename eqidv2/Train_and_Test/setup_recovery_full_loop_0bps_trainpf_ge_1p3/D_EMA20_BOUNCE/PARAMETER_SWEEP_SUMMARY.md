# D_EMA20_BOUNCE Parameter Sweep Summary

- Iterations run: 75
- Search engine for combo fill: Optuna TPE
- Signal features with train quantiles: ADX, ATR, CCI, EMA_20, EMA_50, MACD_Hist, MFI, RSI, atr_pct, body_pct, close_loc, day_value_so_far_rs, ema20_dist_atr, lower_wick_pct, market_abs_ret_pct, market_ret_pct, pressure_ratio_5, quality_score, rs_pct, signal_minute, signal_range_pct, signal_volume, upper_wick_pct, vol_ratio, vwap_dist_atr, wick_skew_pct
- Pre-momentum features with train quantiles: pre10_close_pos, pre10_dir_count, pre10_mom_r, pre10_range_r, pre15_mom_r, pre1_adx, pre1_mom_r, pre1_rsi_dir, pre2_mom_r, pre3_close_pos, pre3_dir_count, pre3_mom_r, pre3_range_r, pre5_close_pos, pre5_dir_count, pre5_mom_r, pre5_range_r, pre_entry_momentum_score, sig5_adx_calc, sig5_body_r, sig5_close_pos, sig5_range_r, sig5_rsi_dir, sig5_vol_ratio20

| Stage | Iterations | TRAIN PF >= 1.30 | TEST PF > 1.40 |
|---|---|---|---|
| stage1_baseline | 1 | 0 | 0 |
| stage4_exit_sweep | 18 | 0 | 0 |
| stage3_time_rank_redesign | 12 | 0 | 0 |
| stage4_signal_filter_sweep | 14 | 0 | 0 |
| stage3_premomentum_redesign | 15 | 0 | 0 |
| stage5_optuna_combo | 15 | 0 | 0 |
