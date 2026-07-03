# A_MOD_BREAK_C1_LOW (SHORT) — PARAMETER_INVENTORY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## 1. Current setup rules (config source: FINAL_SETUP_CONF (active))

- **setup / side:** A_MOD_BREAK_C1_LOW / SHORT
- **entry trigger (detection, read-only):** 5-min bar breaks the prior C1 (first-candle) LOW with a moderate impulse — reason tag `moderate_impulse_break_prior_low`, produced by the production scanner (`_scan_day`); momentum continuation DOWN out of a tight pre-break range.
- **indicator rules (mask):** `[['vol_ratio', '>=', 1.955814]]`
- **non-indicator rules:** entry = next 1-min open after the 5-min signal (max 3-min delay), SHORT fill with adverse slippage; one trade per ticker per day after family dedupe.
- **pre-momentum rules (gate, ALL required, missing->block):** `[['pre5_mom_r', '>=', 0.425861], ['pre3_range_r', '<=', 0.202087]]`
- **filters:** mask terms above (vol_ratio conviction filter).
- **guards:** `{}`
- **SL / target:** 1.1% / 1.0%
- **exit logic:** first-touch SL/TARGET on 1-min OHLC, else EOD forced exit 15:20 IST.
- **time windows:** none in baseline (scanner emits 09:15..15:00 slots).
- **portfolio limits:** max_positions 20, daily_loss_rs 0.0 (0 = off).

## 2. Available columns/features in the recreated pool

Populated for A_MOD raw rows (everything else in the wide schema is empty for this setup):

- **price/OHLC:** signal_open, signal_high, signal_low, signal_close
- **volume:** signal_volume, vol_ratio (bar vs 20-bar avg)
- **VWAP:** vwap_dist_atr (distance from session VWAP in ATRs)
- **volatility:** atr_pct
- **candle structure (derived at load):** body_pct, close_loc, signal_range_pct, upper_wick_pct, lower_wick_pct, wick_skew_pct
- **relative strength / market:** rs_pct, market_ret_pct, market_abs_ret_pct (banned as overfit vector), regime (BEAR/NEUTRAL/TREND)
- **scanner quality:** quality_score (ranker_score ~99% empty -> excluded)
- **time/session:** signal_time_ist, signal_minute, scan_slot_ist, _day, _slot
- **symbol:** ticker
- **pre-momentum (computed 1-min/5-min at eval):** pre_entry_momentum_score, sig5_adx_calc, sig5_rsi_dir, sig5_vol_ratio20, pre1_adx, pre3_range_r, pre5_mom_r, pre3_close_pos
- **NOT available for this setup's raw rows:** EMA/SMA columns, RSI/MACD columns, BB/Keltner, MFI/OBV/CCI/Stoch/W%R/ROC/Supertrend, pressure_ratio, breakout-geometry columns (breakout_strength_atr, orh/pdh/prev20 distances) — all empty in the raw scanner schema; indicator structure enters via the pre-momentum features + quality_score instead.

## 3. Supported pipeline knobs (all exercised in this campaign)

| knob | supported | search range |
|---|---|---|
| mask_terms (<=2 numeric + optional regime categorical) | yes | feats above x q0.1..0.9 x >=/<= |
| pre_momentum_terms (<=2) | yes | 8 premom feats x q0.1..0.9 x >=/<= |
| min_slot | yes | 09:30,09:45,10:00,10:30,11:00,12:00 |
| max_slot | yes | 11:30,12:00,12:30,13:00,14:00,14:30 |
| top_n (per slot, by vwap_dist_atr) | yes | 0-3 |
| max_positions | yes | 10, 20 |
| daily_loss_rs kill-switch | yes | 0 (off), 2000, 4000 |
| SL % | yes | 0.50,0.70,0.85,1.00,1.10,1.20,1.50 |
| target % | yes | 0.60,0.80,1.00,1.25,1.50,2.00,2.50 |
| EOD forced exit | fixed 15:20 IST | (production convention) |
| trailing SL / break-even / time-exit | NOT supported by repo resolver | not searched |
| regime_align overlay | supported (book-level) | regime mask term searched instead |
| max trades/day / per symbol | via family dedupe (1/ticker/day) + top_n + max_positions | — |

## 4. Why these ranges are realistic

- Thresholds come from TRAIN-only quantiles (q0.1..q0.9) — no hand-picked magic numbers, no TEST leakage.
- Exit grid spans tight-scalp (0.5/0.6) to wide-runner (1.5/2.5), covering all four SL-x-target quadrants around the production 1.10/1.00.
- max 2 mask + 2 premom terms + 1 categorical keeps configs explainable and audit-able (the historical overfit failures in this repo all came from >=3-term 6-decimal gates).
- market_ret_pct / signal_minute / notional are EXCLUDED as mask features (documented dominant overfit vectors in setup_train_test.py); time-of-day is expressed via the coarse min_slot/max_slot guards instead.

## 5. PHASE 2 — enriched feature dictionary (added 2026-07-03)

36 additional CAUSAL point-in-time 5-minute features were computed per pool row (`scripts/enrich_features.py`, 100% row coverage) and searched alongside the base 11:

- **indicators:** rsi, rsi_slope3, adx5, adx_slope3, ema20_dist_atr, ema50_dist_atr, ema20_slope_atr, ema_stack_atr, macd_hist_atr, macd_hist_slope3, bb_pos, bb_width_atr, stoch_k, stoch_kd, cci20, mfi14, obv_slope6, vol_z
- **session/day context:** sess_vwap_dist_atr, below_vwap_streak6, day_pos, day_low_dist_atr, day_high_dist_atr, bars_since_day_low, bars_since_day_high, gap_pct, day_ret_pct, c1_range_atr, c1_break_depth_atr
- **price action:** ret3_atr, ret6_atr, ret12_atr, red_streak, body_sum6_atr, range6_atr, range_expansion

Search widened to <=3 mask terms + regime + <=2 pre-momentum terms; exits extended to SL up to 2.0% / target up to 3.0%.
