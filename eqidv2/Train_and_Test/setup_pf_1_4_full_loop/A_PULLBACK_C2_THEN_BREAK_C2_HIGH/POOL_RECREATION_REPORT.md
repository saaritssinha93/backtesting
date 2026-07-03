# Pool Recreation Report - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

## Result
- Pool recreation succeeded: YES
- Output pool: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\pools\pool_full\historical_all_available_pre_dedupe_live_candidates.csv`
- Raw/master source: `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv`
- Tail source: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\pools\tail_v11_raw\historical_all_available_raw_candidates.csv`
- Requested TRAIN: 2026-03-01..2026-05-30
- Actual TRAIN sessions: 2026-03-02..2026-05-29 (58 sessions)
- Requested TEST: 2026-06-01..2026-07-02
- Actual TEST sessions: 2026-06-01..2026-07-02 (19 sessions)
- Missing TRAIN weekdays: 2026-03-03, 2026-03-26, 2026-03-31, 2026-04-03, 2026-04-14, 2026-05-01, 2026-05-28
- Missing TEST weekdays: 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-23, 2026-06-26
- Available first/last session: 2026-03-02..2026-07-02
- Setup rows final: 6834
- Setup signal sessions: 71
- Symbols in setup pool: 1039
- Rows with 1-minute entry after repo attach_entries: 6703

## 5-Minute / 1-Minute Coverage
- 5-minute candidate generation: global unified pool through 2026-06-24 plus fresh v11 historical-all-available generation for 2026-06-25, 2026-06-29, 2026-06-30, 2026-07-01, 2026-07-02.
- 1-minute exit simulation: repo `setup_train_test` / `avwap_5min_ID_v11_backtesting._load_1m_with_open`, merging historical `stocks_indicators_1min_eq` with live raw 1-minute tail when available.
- Data quality issue: missing weekdays are listed above; some are likely exchange holidays/weekends but are treated as missing from available completed-session data, not imputed.
