# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — PARAMETER_INVENTORY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## 1. Current setup rules (config source: default exits 0.70/1.25 (no conf entry — raw detection baseline))

- **setup / side:** B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK / LONG
- **entry trigger (detection, read-only):** catalog detector `huge_green_pullback_hold_then_break`: prev bar HUGE GREEN (range >=1.80x prev ATR, `prev_close>prev_open`), long structure (`close>open`, `close_loc>=0.60`), `close>prev_bar_high`, bar low holds the prev body (`low>=min(prev_open,prev_close)`), `close>VWAP`, `close_loc>=0.65`
- **indicator rules (mask):** `[]`
- **non-indicator rules:** entry = next 1-min open after the 5-min signal (max 3-min delay), SHORT fill with adverse slippage; one trade per ticker per day after family dedupe.
- **pre-momentum rules (gate, ALL required, missing->block):** `[]`
- **filters:** mask terms above (vol_ratio conviction filter).
- **guards:** `{}`
- **SL / target:** 0.7% / 1.25%
- **exit logic:** first-touch SL/TARGET on 1-min OHLC, else EOD forced exit 15:20 IST.
- **time windows:** none in baseline (scanner emits 09:15..15:00 slots).
- **portfolio limits:** max_positions 20, daily_loss_rs 0.0 (0 = off).

## 2. Available columns/features in the recreated pool

Populated for this setup's raw scanner rows (wide-schema columns the scanner does not emit for it are empty and are pruned from the search automatically):

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