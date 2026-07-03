# A_PULLBACK_C2_THEN_BREAK_C2_LOW - POOL_RECREATION_REPORT

Generated 2026-07-02. Research-only; no live execution.

## Raw Data Sources Used

- `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv`: exists=True setup_rows=14973 first=2025-11-03 last=2026-06-24 sessions=145
- `C:\TradingData\eqidv2\outputs_ID_v11_conf_fresh_20260629\historical_all_available_raw_candidates.csv`: exists=True setup_rows=4512 first=2026-04-13 last=2026-06-24 sessions=46
- `C:\TradingData\eqidv2\outputs_ID_v11_unified_recent_raw_0622\historical_all_available_raw_candidates.csv`: exists=True setup_rows=237 first=2026-06-12 last=2026-06-22 sessions=4
- `C:\TradingData\eqidv2\outputs_ID_v11_unified_recent_raw_0624\historical_all_available_raw_candidates.csv`: exists=True setup_rows=82 first=2026-06-24 last=2026-06-24 sessions=1
- `C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\historical_full_day_raw_candidates.csv`: exists=True setup_rows=0 first=None last=None sessions=0
- `C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\live_parity_raw_candidates.csv`: exists=True setup_rows=0 first=None last=None sessions=0
- `C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\historical_full_day_pre_dedupe_live_candidates.csv`: exists=True setup_rows=0 first=None last=None sessions=0
- `C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\live_parity_pre_dedupe_live_candidates.csv`: exists=True setup_rows=0 first=None last=None sessions=0

## Recreated Pool

- Path: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_LOW\pools\historical_all_available_pre_dedupe_live_candidates.csv`
- Rows pre-dedupe: 11213
- Rows after dedupe: 8101
- Columns: 96
- Available first/last setup-candidate session: 2026-03-02 / 2026-06-24

## Requested vs Actual

- Requested TRAIN: 2026-03-01 to 2026-05-30
- Actual TRAIN setup-candidate sessions: 53 (2026-03-02..2026-05-29)
- Requested TEST: 2026-06-01 to 2026-07-02
- Actual TEST setup-candidate sessions: 17 (2026-06-01..2026-06-24)
- Missing TRAIN business dates from setup-candidate pool: 2026-03-03, 2026-03-12, 2026-03-26, 2026-03-31, 2026-04-03, 2026-04-08, 2026-04-14, 2026-04-17, 2026-04-21, 2026-05-01, 2026-05-20, 2026-05-28
- Missing TEST business dates from setup-candidate pool: 2026-06-11, 2026-06-25, 2026-06-26, 2026-06-29, 2026-06-30, 2026-07-01, 2026-07-02

## 5-Minute and 1-Minute Coverage

- 5-minute signal pool was recreated from v11 unified/raw/backtest candidate files listed above.
- 1-minute exit realism is provided by `setup_train_test.py` via `avwap_5min_ID_v11_backtesting._load_1m_with_open` and `v5_exit_resolver`.
- Entry rows after 1-minute entry attach: TRAIN 5589 / TEST 2511.

## Data Quality Issues

- No `A_PULLBACK_C2_THEN_BREAK_C2_LOW` rows were found in the July 1 backtesting raw/pre-dedupe files inspected.
- Therefore the nearest available TEST setup-candidate session in the recreated pool is reported above.