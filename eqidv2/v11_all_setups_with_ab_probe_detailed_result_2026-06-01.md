# V11 All Setups Detailed Result With A/B Probe - 2026-06-01

## Scope

- Source raw candidates: `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\historical_all_available_raw_candidates.csv`
- Output folder: `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250`
- Date range: `2025-06-02` to `2026-05-29`
- Trading window: `09:15` to `15:00`
- Split: train `<= 2026-01-31`, validation `2026-02-01` to `2026-03-31`, holdout/test `2026-04-01` to `2026-05-29`
- A/B gate: `quality_top_slot`, `quality_score >= 250`, max `1` per side and `2` per slot
- Honesty note: this is a cached-raw combined diagnostic. `B_HUGE_PULLBACK_HOLD_BREAK` is absent because it was excluded before the cached raw file was written.

## Pipeline

| Stage | Count |
|---|---:|
| Raw candidates | 256,426 |
| Regular v8 gated candidates | 12,949 |
| A/B gate accepted | 1,430 |
| Combined gated candidates | 14,379 |
| Research rejected candidates | 1,247 |
| Pre-dedupe live candidates | 13,132 |
| Live-like candidates | 11,802 |
| Entry-engine raw entries | 13,129 |
| Entry-engine rejects | 3 |
| Entry-engine signals | 11,799 |
| Resolved trades | 11,799 |

## Overall

| Split | Trades | PF | PnL Rs | Win % | Avg/Trade Rs |
|---|---:|---:|---:|---:|---:|
| train | 7,317 | 0.879 | -281,633.10 | 44.64 | -38.49 |
| validation | 2,368 | 0.843 | -126,731.79 | 45.82 | -53.52 |
| holdout_test | 2,114 | 0.820 | -131,338.02 | 43.80 | -62.13 |
| full | 11,799 | 0.860 | -539,702.91 | 44.72 | -45.74 |

## Setup Detail - Full Period

| Setup | Status | Raw | Gate | Live-Like | Entry Signals | Trades | PF | PnL Rs | Win % | Target % | SL % | EOD % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| E_ORB_BREAKOUT_SHORT | TRADED | 562 | 356 | 340 | 339 | 339 | 1.287 | 35,153.03 | 49.26 | 31.27 | 42.18 | 26.55 |
| C_OR_BREAKOUT | TRADED | 9,481 | 304 | 137 | 137 | 137 | 1.718 | 28,968.79 | 56.93 | 27.74 | 16.06 | 56.20 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | TRADED | 3,403 | 66 | 44 | 44 | 44 | 1.479 | 8,640.47 | 40.91 | 40.91 | 59.09 | 0.00 |
| E_VWAP_LOSE_EARLY_SHORT | TRADED | 337 | 79 | 74 | 74 | 74 | 1.306 | 7,791.43 | 48.65 | 41.89 | 47.30 | 10.81 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | TRADED | 21,227 | 45 | 36 | 36 | 36 | 1.469 | 6,367.55 | 55.56 | 55.56 | 44.44 | 0.00 |
| S_BB_SQUEEZE_SHORT | TRADED | 955 | 565 | 538 | 538 | 538 | 1.022 | 5,542.98 | 45.91 | 24.91 | 43.49 | 31.60 |
| A_MOD_BREAK_C1_LOW | TRADED | 62,089 | 18 | 11 | 11 | 11 | 1.899 | 3,133.80 | 54.55 | 27.27 | 45.45 | 27.27 |
| B_HUGE_RED_FAILED_BOUNCE | TRADED | 4,347 | 29 | 22 | 22 | 22 | 1.224 | 2,186.43 | 36.36 | 36.36 | 63.64 | 0.00 |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | TRADED | 16,486 | 84 | 52 | 52 | 52 | 1.103 | 1,994.16 | 46.15 | 46.15 | 53.85 | 0.00 |
| B_HUGE_PULLBACK_HOLD_BREAK | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| C_OR_BREAKDOWN | RAW_ONLY_OR_FILTERED | 26,414 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| D_AVWAP_LOSE_REVERSAL | RAW_ONLY_OR_FILTERED | 1,737 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_FAILED_OR_BREAKDOWN_TRAP_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_FAILED_OR_BREAKOUT_TRAP_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_GAP_HOLD_CONTINUATION_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_GAP_HOLD_CONTINUATION_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_OPENING_DRIVE_CONTINUATION_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_OPENING_DRIVE_CONTINUATION_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_ORB_RETEST_HOLD_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_ORB_RETEST_HOLD_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_RS_FIRST_HOUR_BREAK_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_RS_FIRST_HOUR_BREAK_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| E_VWAP_RECLAIM_EARLY_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| G_LOWER_LOW_BREAK | RAW_ONLY_OR_FILTERED | 2,954 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| L_DOUBLE_BOTTOM_VWAP | RAW_ONLY_OR_FILTERED | 10,126 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| L_PRESSURE_BURST_VWAP | RAW_ONLY_OR_FILTERED | 20,193 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| L_TREND_PULLBACK | RAW_ONLY_OR_FILTERED | 1,215 | 738 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| S_LIQUIDITY_SWEEP_REVERSAL | NOT_IN_CACHED_RAW | 0 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| S_MACD_HIST_FLIP | RAW_ONLY_OR_FILTERED | 7,725 | 0 | 0 | 0 | 0 | N/A | 0.00 | N/A | N/A | N/A | N/A |
| G_HIGHER_HIGH_BREAK | TRADED | 1,330 | 255 | 136 | 136 | 136 | 0.932 | -3,025.04 | 44.85 | 12.50 | 26.47 | 61.03 |
| L_BB_SQUEEZE_LONG | TRADED | 1,675 | 292 | 230 | 230 | 230 | 0.956 | -3,726.28 | 49.57 | 45.65 | 48.70 | 5.65 |
| B_AVWAP_RECLAIM_REVERSAL | TRADED | 2,215 | 144 | 63 | 63 | 63 | 0.839 | -5,104.91 | 28.57 | 26.98 | 71.43 | 1.59 |
| A_MOD_CLOSE_CONTINUATION_BREAK | TRADED | 9,402 | 213 | 113 | 113 | 113 | 0.896 | -5,647.19 | 29.20 | 28.32 | 69.03 | 2.65 |
| A_MOD_BREAK_C1_HIGH | TRADED | 40,337 | 895 | 405 | 405 | 405 | 0.934 | -11,165.29 | 39.26 | 38.52 | 59.51 | 1.98 |
| E_ORB_BREAKOUT_LONG | TRADED | 144 | 144 | 124 | 124 | 124 | 0.680 | -21,163.96 | 33.06 | 29.84 | 66.94 | 3.23 |
| D_EMA20_REJECTION | TRADED | 2,126 | 1,486 | 1,408 | 1,408 | 1,408 | 0.937 | -28,416.24 | 44.32 | 13.35 | 37.43 | 49.22 |
| D_EMA20_BOUNCE | TRADED | 2,239 | 1,769 | 1,514 | 1,514 | 1,514 | 0.737 | -131,883.83 | 36.72 | 9.18 | 40.16 | 50.66 |
| E_VWAP_BAND_FADE | TRADED | 7,707 | 6,897 | 6,555 | 6,553 | 6,553 | 0.781 | -429,348.80 | 46.80 | 35.45 | 38.47 | 26.08 |

## Positive Setups

| Setup | Trades | PF | PnL Rs | Validation PF | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|---:|
| E_ORB_BREAKOUT_SHORT | 339 | 1.287 | 35,153.03 | 0.520 | 1.603 | 4,324.90 |
| C_OR_BREAKOUT | 137 | 1.718 | 28,968.79 | 2.122 | N/A | 0.00 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 44 | 1.479 | 8,640.47 | 1.431 | 1.426 | 1,782.50 |
| E_VWAP_LOSE_EARLY_SHORT | 74 | 1.306 | 7,791.43 | 0.714 | 1.430 | 600.57 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 36 | 1.469 | 6,367.55 | 1.176 | 0.589 | -1,393.54 |
| S_BB_SQUEEZE_SHORT | 538 | 1.022 | 5,542.98 | 0.987 | 1.112 | 4,802.39 |
| A_MOD_BREAK_C1_LOW | 11 | 1.899 | 3,133.80 | 1.079 | 0.842 | -110.90 |
| B_HUGE_RED_FAILED_BOUNCE | 22 | 1.224 | 2,186.43 | 1.072 | 0.000 | -2,096.37 |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | 52 | 1.103 | 1,994.16 | 0.000 | 2.549 | 2,168.82 |

## Biggest Losing Setups

| Setup | Trades | PF | PnL Rs | Validation PF | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|---:|
| E_VWAP_BAND_FADE | 6,553 | 0.781 | -429,348.80 | 0.815 | 0.785 | -73,403.82 |
| D_EMA20_BOUNCE | 1,514 | 0.737 | -131,883.83 | 0.834 | 0.546 | -71,016.33 |
| D_EMA20_REJECTION | 1,408 | 0.937 | -28,416.24 | 0.793 | 1.045 | 5,005.25 |
| E_ORB_BREAKOUT_LONG | 124 | 0.680 | -21,163.96 | 0.750 | 0.665 | -2,938.72 |
| A_MOD_BREAK_C1_HIGH | 405 | 0.934 | -11,165.29 | 1.575 | 0.659 | -6,189.54 |
| A_MOD_CLOSE_CONTINUATION_BREAK | 113 | 0.896 | -5,647.19 | 0.306 | 1.129 | 1,704.55 |
| B_AVWAP_RECLAIM_REVERSAL | 63 | 0.839 | -5,104.91 | 0.575 | 0.535 | -1,297.40 |
| L_BB_SQUEEZE_LONG | 230 | 0.956 | -3,726.28 | 0.871 | 1.358 | 4,523.82 |
| G_HIGHER_HIGH_BREAK | 136 | 0.932 | -3,025.04 | 0.564 | 1.947 | 2,195.79 |

## No Current Executable Trades

| Setup | Status | Raw Candidates | Gate | Entry Signals |
|---|---|---:|---:|---:|
| B_HUGE_PULLBACK_HOLD_BREAK | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| C_OR_BREAKDOWN | RAW_ONLY_OR_FILTERED | 26,414 | 0 | 0 |
| D_AVWAP_LOSE_REVERSAL | RAW_ONLY_OR_FILTERED | 1,737 | 0 | 0 |
| E_FAILED_OR_BREAKDOWN_TRAP_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_FAILED_OR_BREAKOUT_TRAP_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_GAP_HOLD_CONTINUATION_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_GAP_HOLD_CONTINUATION_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_OPENING_DRIVE_CONTINUATION_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_OPENING_DRIVE_CONTINUATION_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_ORB_RETEST_HOLD_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_ORB_RETEST_HOLD_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_RS_FIRST_HOUR_BREAK_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_RS_FIRST_HOUR_BREAK_SHORT | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| E_VWAP_RECLAIM_EARLY_LONG | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| G_LOWER_LOW_BREAK | RAW_ONLY_OR_FILTERED | 2,954 | 0 | 0 |
| L_DOUBLE_BOTTOM_VWAP | RAW_ONLY_OR_FILTERED | 10,126 | 0 | 0 |
| L_PRESSURE_BURST_VWAP | RAW_ONLY_OR_FILTERED | 20,193 | 0 | 0 |
| L_TREND_PULLBACK | RAW_ONLY_OR_FILTERED | 1,215 | 738 | 0 |
| S_LIQUIDITY_SWEEP_REVERSAL | NOT_IN_CACHED_RAW | 0 | 0 | 0 |
| S_MACD_HIST_FLIP | RAW_ONLY_OR_FILTERED | 7,725 | 0 | 0 |

## Files

- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\trades.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\all_setups_summary_by_split.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\all_setups_by_setup_full.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\all_setups_by_setup_split.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\all_setups_stage_counts_by_setup.csv`
