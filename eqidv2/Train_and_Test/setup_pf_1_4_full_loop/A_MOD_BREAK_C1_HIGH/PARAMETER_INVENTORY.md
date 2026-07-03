# A_MOD_BREAK_C1_HIGH — Parameter Inventory

_Generated 2026-07-02. Research-only. Stage-1 artifact of the full-loop optimization._

## 1. Current Setup Rules (as running today)

| item | value | source |
|---|---|---|
| setup | `A_MOD_BREAK_C1_HIGH` | catalog `all_setups_catalog.py:77` |
| side | LONG | same |
| entry trigger (detector) | moderate-impulse bar (0.60·ATR ≤ range ≤ 2.20·ATR) breaking prior bar high, above session VWAP | `avwap_5min_ID_v2_backtesting.py:689` (`_scan_day`) |
| detector indicator rules | `vol_ratio ≥ 1.5`; regime ≠ BEAR | same |
| detector non-indicator rules | `long_struct` (green bar, close-loc), `close > prev_bar_high`, `rs_pct > 0.05`, quality_score ≥ 7.0 (v2 scale) | same |
| production gate (2026-06-09) | `rs_pct ≥ 2.0` AND `atr_pct ≤ 0.006` AND `signal_minute ≤ 670` (11:10) | `avwap_5min_ID_v11_backtesting.py:158-160`, mirrored `eqidv2_v11_live_overlay.py:63` |
| pre-momentum rules | **none** (not in `PRE_ENTRY_MOMENTUM_SETUP_GATES`) | `avwap_5min_ID_v11_backtesting.py:400+` |
| entry-engine guards | reject signal_time ≥ 11:10; **top-2 per (day, slot)** ranked by `vwap_dist_atr` desc | `avwap_5min_ID_v11_backtesting.py:378-383,1332-1381` |
| SL / target | **0.70% / 1.00%** (no profile override) | `avwap_5min_ID_v6_backtesting.py:46` `SETUP_EXIT_RULES` |
| exit logic | bracket SL/target on 1-min resolution, EOD flat 15:20 | v6/v11 resolution pipeline |
| time window | detector emits 09:30–14:30 (entry window env); gate caps at 11:10 | live scanner env |
| portfolio limits | max_positions 20 (executor), per-ticker dedupe | executor |
| config source | production overlay profile `production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced`; **NOT in final_setup_conf** (overlay-only setup, §5.3 of SETUP_CARDS_AND_LIVE_CROSSCHECK.md) | — |

## 2. Available Pool Columns (94, recreated pool)

**Identity/plumbing:** candidate_id, scan_session, selection_mode, candidate_family, scan_slot_ist, signal_time_ist, ticker, side, setup, reason, status, created_at_ist, diagnostics_json, research_shadow_*, v11_source_day, candidate_schema_version, bar_time_ist, _basis, signal_id, signal_datetime, trade_date, date, source_setup, source_quality_score.

**Price/OHLC (signal bar):** signal_open, signal_high, signal_low, signal_close, signal_volume.

**Core mask features (numeric):** quality_score, rs_pct, market_ret_pct, vol_ratio, atr_pct, body_pct, close_loc, vwap_dist_atr, ranker_score, score, rsi, rsi3max, adx, upper_wick_pct, lower_wick_pct, macd_hist, macd_hist_delta, ema20_slope, stock_ret, lower_wick_price_pct.

**Categorical:** regime (BULL/BEAR/NEUTRAL).

**Derived by harness at load:** signal_minute, `_day`, `_slot`, wick_skew_pct, signal_range_pct; pre-momentum features computed on demand: pre_entry_momentum_score, sig5_adx_calc, sig5_rsi_dir, sig5_vol_ratio20, pre1_adx, pre3_range_r, pre5_mom_r, pre3_close_pos.

**Entry/exit bookkeeping (not tunable):** v7_signal_* (entry/stop/target/sl_pct/target_pct/notional), v6_* (outcome, exit price/time, pnl, cost), capital_per_trade_rs, leverage, notional_exposure_rs, v11_* provenance columns.

## 3. Supported Optimization Knobs (repo pipeline: `pf_band_fitval_loop.py` + `setup_train_test.py`)

| knob | supported values (engine grid) |
|---|---|
| `mask_terms` (≤2) | 26 features × quantile thresholds QGRID {0.1..0.9} × {>=, <=}; categorical `regime` terms |
| `pre_momentum_terms` (≤2) | 8 pre-momentum features × quantile thresholds |
| `sl_pct` | {0.50, 0.70, 0.85, 1.00, 1.10, 1.20, 1.50} |
| `tgt_pct` | {0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50} |
| `min_slot` | {09:30, 09:45, 10:00, 10:30, 11:00} |
| `max_slot` | {12:00, 12:30, 13:00, 14:00, 14:30} + explicit 11:05 (production mirror) in sweeps |
| `top_n` per slot | {0(off), 1, 2, 3} ranked by vwap_dist_atr desc |
| `max_positions` | {10, 20} |
| `daily_loss_rs` | {0 (off), 4000} |
| trailing SL / break-even / time-exit | **not supported** by the harness (bracket + EOD only) — noted as limitation |
| regime_align | via categorical mask term on `regime` |

## 4. Candidate Ranges Selected (and why)

| group | ranges swept | rationale |
|---|---|---|
| rs_pct | relaxed ≥1.0 / medium ≥2.0 (current) / strict ≥3.5..5 | current gate value sits at ~median of pool; failure-study quintiles ground the extremes |
| atr_pct | ≤0.005 / ≤0.006 (current) / ≤0.008 / off | current cap is tight; test both directions |
| vol_ratio | ≥1.5 (detector floor) / ≥1.8 / ≥2.2 / ≥3.0 | volume confirmation strength |
| time window | ≤10:30 / ≤11:05 (current) / ≤12:30 / ≥10:00 late-start | morning-vs-midday behavior differs (failure study by-hour table) |
| top_n | off / 1 / 2 (current) / 3 | slot-crowding control |
| SL×Tgt | full 7×7 grid | baseline 0.70/1.00 has 70% SL-rate — clearly wrong; must re-fit |
| body/close_loc/wick | quantile sweeps | candle-quality (fake-break avoidance) |
| vwap_dist_atr | ≤ overextension caps / ≥ floors | anti-chase vs momentum trade-off |
| pre-momentum (8 feats) | quantile sweeps, ≤2 terms | only place "momentum into signal" is expressible |
| regime | != BEAR (detector) / == BULL strict | market-condition filter |
| macd_hist_delta, ema20_slope, rsi, adx | quantile sweeps | trend/momentum confirmation available in pool |

Anti-overfit constraints applied throughout: ≤2 mask terms + ≤2 pre-momentum terms, quantile-snapped thresholds only, neighborhood (±1 quantile) and term-dropout robustness, day-block permutation p, domination caps (trade/day/symbol), TRAIN band 1.30–1.80 with TEST>1.40 checked once per candidate.
