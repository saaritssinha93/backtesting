# A_PULLBACK_C2_THEN_BREAK_C2_LOW - PARAMETER_INVENTORY

## 1. Current Setup Rules

- Setup name: A_PULLBACK_C2_THEN_BREAK_C2_LOW
- Side: SHORT
- Current entry trigger: next 1-minute open after 5-minute signal.
- Indicator rules: VWAP loss, vol_ratio>=1.4, regime!=BULL, sig5_adx_calc gate.
- Non-indicator rules: red candle, close_loc<=0.40, close below previous bar low, prior two-bar pullback.
- Current pre-momentum rules: sig5_adx_calc>=21.4683
- Current filters: quality_score>=123.7606
- Current guards: {}
- Current SL/target: 1.2 / 1.5
- Current exit logic: SL/target/EOD via 1-minute resolver.
- Current time windows: default live 09:30-14:30 unless guard overrides.
- Current portfolio limits: max_positions default 20; daily_loss_rs default 0 unless candidate overrides.
- Current config source: root `final_setup_conf.py`.

## 2. Available Columns/Features In Recreated Pool

### price_ohlc

`close_loc`, `entry_price_v6`, `lower_wick_pct`, `lower_wick_price_pct`, `old_entry_price_v7`, `signal_close`, `signal_high`, `signal_low`, `signal_open`, `v6_exit_price`, `v6_pnl_pct_price`, `v7_executor_entry_price_source`, `v7_live_stop_price`, `v7_live_target_price`, `v7_signal_entry_price`, `v7_signal_stop_price`, `v7_signal_target_price`

### volume

`notional_exposure_rs`, `signal_volume`, `v7_signal_notional_rs`, `vol_ratio`

### vwap_avwap

`vwap_dist_atr`

### ema_sma

`candidate_schema_version`, `ema20_slope`

### rsi_adx_macd

`adx`, `candidate_schema_version`, `macd_hist`, `macd_hist_delta`, `research_shadow_version`, `rsi`, `rsi3max`

### atr_volatility

`atr_pct`, `vwap_dist_atr`

### candle_structure

`body_pct`, `close_loc`, `lower_wick_pct`, `lower_wick_price_pct`, `upper_wick_pct`

### pre_momentum

(none)

### setup_reason

`quality_score`, `ranker_model`, `ranker_score`, `reason`, `research_shadow_reason`, `setup`, `source_quality_score`, `source_setup`

### time_session

`bar_time_ist`, `candidate_family`, `candidate_id`, `candidate_schema_version`, `date`, `entry_time_v6`, `old_entry_time_v7`, `scan_slot_ist`, `signal_bar_time_ist`, `signal_datetime`, `signal_entry_datetime_ist`, `signal_time_ist`, `signal_time_v8`, `trade_date`, `v11_source_day`, `v6_exit_time_ist`, `v7_signal_entry_time_ist`, `v8_entry_delay_minutes`

### symbol

`ticker`

### other

`_basis`, `_sig_local`, `_source_path`, `capital_per_trade_rs`, `created_at_ist`, `diagnostics_json`, `leverage`, `market_ret_pct`, `pnl`, `quantity`, `regime`, `research_shadow_action`, `research_shadow_status`, `rs_pct`, `scan_session`, `score`, `selection_mode`, `side`, `signal_id`, `status`, `stock_ret`, `v11_exit_override_applied`, `v11_exit_rule_source`, `v11_selected_strategy_profile`, `v6_bars_held`, `v6_cost_rs`, `v6_gross_pnl_rs`, `v6_net_pnl_rs`, `v6_outcome`, `v6_sl_pct`, `v6_target_pct`, `v7_entry_engine_model`, `v7_resolution_source`, `v7_signal_sl_pct`, `v7_signal_target_pct`, `v8_entry_model`, `v8_resolution_source`

## 3. Supported Optimization Knobs

- mask_terms
- pre_momentum_terms
- min_slot / max_slot / top_n entry guards
- max_positions
- daily_loss_rs
- fixed SL and fixed target
- EOD exit through repo resolver
- portfolio overlay via `setup_train_test.eval_family`

## 4. Candidate Parameter Ranges

- SL grid: [0.5, 0.7, 0.85, 0.9, 1.0, 1.1, 1.2, 1.5]
- Target grid: [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5]
- Min slot grid: ['09:30', '09:45', '10:00', '10:30', '11:00']
- Max slot grid: ['11:30', '12:00', '12:30', '13:00', '14:00', '14:30']
- Top-N grid: [0, 1, 2, 3]
- max_positions grid: [10, 20]
- daily_loss_rs grid: [0.0, 4000.0]
- Mask thresholds: FIT-only quantiles q=0.1..0.9 for available indicator, candle, volume, VWAP, quality, market, and time columns.
- Pre-momentum thresholds: FIT-only sampled 1-minute pre-entry quantiles q=0.1..0.9.
- Ranges are realistic because they use observed FIT distributions and repo-supported fields only; TEST columns are never used for threshold construction.

## Searchable FIT Quantile Features

- Mask: atr_pct, body_pct, close_loc, lower_wick_pct, market_abs_ret_pct, market_ret_pct, notional, quality_score, rs_pct, signal_minute, signal_range_pct, upper_wick_pct, vol_ratio, vwap_dist_atr, wick_skew_pct
- Pre-momentum: pre1_adx, pre3_close_pos, pre3_range_r, pre5_mom_r, pre_entry_momentum_score, sig5_adx_calc, sig5_rsi_dir, sig5_vol_ratio20
## Staged 5m-Enriched Feature Addendum
Additional 5-minute features were joined from `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2` by ticker and signal bar time.
Feature cache: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_LOW\pools\feature5m_signal_cache.csv`.

Searchable enriched feature columns:

- `rsi`, `rsi3max`, `adx`, `macd_hist`, `macd_hist_delta`, `ema20_slope`, `stock_ret`, `lower_wick_price_pct`, `day_value_so_far_rs`, `feat5_rsi`, `feat5_rsi3max`, `feat5_adx`, `feat5_atr_pct`, `feat5_ema20_dist_pct`, `feat5_ema50_dist_pct`, `feat5_ema20_slope_3`, `feat5_ema20_vs_ema50_pct`, `feat5_macd`, `feat5_macd_signal`, `feat5_macd_hist`, `feat5_macd_hist_delta`, `feat5_cci`, `feat5_mfi`, `feat5_stoch_k`, `feat5_stoch_d`, `feat5_bb_pos`, `feat5_bb_width_pct`, `feat5_stock_ret_5m_pct`, `feat5_stock_ret_15m_pct`, `feat5_stock_ret_30m_pct`, `feat5_volume_ratio_20`, `feat5_range_pct`, `feat5_body_efficiency`, `feat5_close_location`, `feat5_upper_wick_pct`, `feat5_lower_wick_pct`, `feat5_vwap_dist_pct`, `feat5_recent_low_dist_pct`, `feat5_recent_high_dist_pct`, `feat5_opening_snapshot`
