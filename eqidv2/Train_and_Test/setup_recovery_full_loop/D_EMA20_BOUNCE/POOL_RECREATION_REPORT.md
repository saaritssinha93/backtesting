# D_EMA20_BOUNCE

- Requested TRAIN: 2026-03-01..2026-05-30
- Requested TEST: 2026-06-01..2026-07-02
- Actual TRAIN data sessions: 2026-03-02..2026-05-29 (58)
- Actual TEST data sessions: 2026-06-01..2026-07-02 (23)
- FIT sessions: 2026-03-02..2026-04-23 (34)
- VAL sessions: 2026-04-24..2026-05-29 (24)
- Candidate sessions in TRAIN: 48
- Candidate sessions in TEST: 21
- Pool rows: 1228; rows with 1-minute entry: 1228
- Slippage model: setup_train_test statutory costs with 5.0 bps per leg.

## Rebuild Method

The pool was recreated from 5-minute parquet data with `avwap_5min_ID_v2_backtesting._prepare_5m` and `_scan_day`, filtered to this setup only, then saved as a per-setup `pre_dedupe_live_candidates` CSV. Entry and exits were resolved later through `setup_train_test` on 1-minute data.

## Requested Vs Actual

- Data root: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`
- Tickers scanned: 1280
- Candidate sessions: 2026-03-05..2026-07-02 (69)
- Pool CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop\D_EMA20_BOUNCE\pools\historical_all_available_pre_dedupe_live_candidates.csv`
