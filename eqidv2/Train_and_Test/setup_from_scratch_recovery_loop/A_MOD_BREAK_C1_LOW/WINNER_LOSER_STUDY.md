# A_MOD_BREAK_C1_LOW (SHORT) — WINNER_LOSER_STUDY (TRAIN only, 1-min paths)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- deduped TRAIN book n=11733, winners 4180 (35.6%) at baseline exits 1.10/1.00.
- favorable-move-first share: 48.9% (coin flip).
- median EOD drift: -0.12% (near zero).

## MFE / MAE percentiles (% from entry, SHORT)

| percentile | MFE all | MAE all | MFE winners | MAE winners | MFE losers | MAE losers |
|---|---|---|---|---|---|---|
| p25 | 0.109 | 0.462 | 0.856 | 0.294 | 0.006 | 0.706 |
| p40 | 0.31 | 0.656 | 1.09 | 0.39 | 0.119 | 0.976 |
| p50 | 0.472 | 0.823 | 1.232 | 0.461 | 0.201 | 1.163 |
| p60 | 0.661 | 1.011 | 1.396 | 0.533 | 0.292 | 1.361 |
| p75 | 1.034 | 1.434 | 1.742 | 0.691 | 0.473 | 1.795 |
| p90 | 1.722 | 2.296 | 2.387 | 0.949 | 0.764 | 2.646 |

## Winner vs loser feature medians

| feature | winner med | loser med | winner p25-p75 |
|---|---|---|---|
| rsi | 34.4412 | 34.592 | 29.6245..40.0605 |
| adx5 | 27.5044 | 27.4943 | 22.7123..35.2436 |
| ema20_dist_atr | -2.0487 | -2.0414 | -2.6591..-1.3657 |
| ema_stack_atr | -1.3325 | -1.1222 | -2.3011..-0.12 |
| macd_hist_atr | -0.1098 | -0.1299 | -0.2843..0.0541 |
| bb_pos | -0.9467 | -0.9259 | -1.151..-0.6964 |
| stoch_k | 6.6666 | 6.8027 | 1.8345..14.5531 |
| mfi14 | 28.8338 | 28.6633 | 18.8025..41.0389 |
| obv_slope6 | -0.6164 | -0.5907 | -0.922..-0.3376 |
| vol_z | 1.6731 | 1.5781 | 1.0318..2.4803 |
| sess_vwap_dist_atr | -3.4056 | -3.3786 | -4.9584..-2.041 |
| below_vwap_streak6 | 6.0 | 6.0 | 6.0..6.0 |
| day_pos | 0.0833 | 0.0881 | 0.0235..0.2938 |
| day_low_dist_atr | 0.8802 | 0.9286 | 0.2832..2.9592 |
| bars_since_day_low | 14.0 | 14.0 | 0.0..35.0 |
| gap_pct | -0.0945 | 0.0497 | -0.8..0.4301 |
| day_ret_pct | -1.3076 | -1.2898 | -2.55..-0.0279 |
| c1_break_depth_atr | 1.814 | 1.7975 | -1.9984..5.52 |
| ret6_atr | -1.8157 | -1.8291 | -2.6903..-1.0614 |
| red_streak | 2.0 | 2.0 | 1.0..3.0 |
| body_sum6_atr | -1.8322 | -1.8283 | -2.706..-1.011 |
| range6_atr | 2.8169 | 2.7954 | 2.2719..3.4725 |
| range_expansion | 1.5639 | 1.559 | 1.1722..2.0 |
| vol_ratio | 2.5466 | 2.4065 | 1.9085..3.6735 |
| atr_pct | 0.0028 | 0.0026 | 0.0022..0.0035 |
| close_loc | 0.1157 | 0.1205 | 0.0..0.2308 |
| quality_score | 59.7159 | 58.3026 | 43.0832..83.2502 |

## Observations

- The winner/loser feature medians are nearly identical on most dimensions — the losing population is homogeneous; edge cannot be carved by features (confirms phase-2's 846-scan result).
- Suggested exits fed to the loop (TRAIN-only): targets [0.31, 0.472, 0.661] / SLs [0.823, 1.011, 1.434] — even the best-fitting geometry cannot clear ~0.30% round-trip costs at these excursion sizes.