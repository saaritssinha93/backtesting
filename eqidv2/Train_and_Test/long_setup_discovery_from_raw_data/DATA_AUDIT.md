# DATA_AUDIT

## Raw Data Paths Found
- 5-minute raw/indicator store used: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`
- 1-minute raw/indicator store used: `C:\TradingData\eqidv2\stocks_indicators_1min_eq`
- Other 5-minute stores inspected: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`, `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live`, `C:\TradingData\eqidv2\stocks_indicators_5min_eq`, `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\outputs_advanced_indicators_5min`, `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\outputs_advanced_indicators_5min_current`
- Other 1-minute stores inspected: `C:\TradingData\eqidv2\stocks_indicators_1min_eq`, `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\stocks_indicators_1min_eq`, `C:\TradingData\eqidv2\stocks_raw_1min_entry_v5_id_live`

## Available Date Range And Symbols
- 5-minute symbols: 1,295; sessions: 267; date range: 2025-06-02 to 2026-06-30
- 1-minute symbols: 1,286; sessions: 267; date range: 2025-06-02 to 2026-06-30
- common symbols with both stores: 1,285
- selected completed common sessions: 2026-04-30..2026-06-29 (40 sessions)

## FIT / VAL / TRAIN / TEST Sessions
- FIT: 2026-04-30, 2026-05-04, 2026-05-05, 2026-05-06, 2026-05-07, 2026-05-08, 2026-05-11, 2026-05-12, 2026-05-13, 2026-05-14, 2026-05-15, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-21
- VAL: 2026-05-22, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-29, 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12
- TRAIN: 2026-04-30, 2026-05-04, 2026-05-05, 2026-05-06, 2026-05-07, 2026-05-08, 2026-05-11, 2026-05-12, 2026-05-13, 2026-05-14, 2026-05-15, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-21, 2026-05-22, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-29, 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12
- TEST: 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-22, 2026-06-23, 2026-06-24, 2026-06-25, 2026-06-29

## Session Coverage (latest 20 common completed sessions)
| session | 5m complete symbols | 1m complete symbols |
|---|---:|---:|
| 2026-06-01 | 1264 | 1222 |
| 2026-06-02 | 1265 | 1216 |
| 2026-06-03 | 1265 | 1222 |
| 2026-06-04 | 1262 | 1209 |
| 2026-06-05 | 1262 | 1204 |
| 2026-06-08 | 1262 | 1197 |
| 2026-06-09 | 1262 | 1189 |
| 2026-06-10 | 1262 | 1192 |
| 2026-06-11 | 1261 | 1196 |
| 2026-06-12 | 1261 | 1192 |
| 2026-06-15 | 1261 | 1215 |
| 2026-06-16 | 1261 | 1201 |
| 2026-06-17 | 1261 | 1204 |
| 2026-06-18 | 1259 | 1211 |
| 2026-06-19 | 1258 | 1209 |
| 2026-06-22 | 1203 | 1213 |
| 2026-06-23 | 1258 | 1217 |
| 2026-06-24 | 1257 | 1195 |
| 2026-06-25 | 1256 | 1199 |
| 2026-06-29 | 1254 | 1192 |

## Columns
### 5-minute sample `360ONE_stocks_indicators_5min.parquet`
- rows=15,590, columns=32, range=2025-08-25 09:20:00 to 2026-06-30 15:30:00
- indicator columns (19): RSI, ATR, EMA_20, EMA_50, EMA_200, 20_SMA, VWAP, CCI, MFI, OBV, MACD, MACD_Signal, MACD_Hist, Upper_Band, Lower_Band, ADX, Stoch_%K, Stoch_%D, opening_snapshot
- non-indicator columns (13): date, open, high, low, close, volume, Recent_High, Recent_Low, date_only, Intra_Change, Prev_Day_Close, Daily_Change, gap_filled

### 1-minute sample `360ONE_stocks_indicators_1min.parquet`
- rows=99,435, columns=30, range=2025-06-02 09:16:00 to 2026-06-30 09:17:00
- indicator columns (18): RSI, ATR, EMA_20, EMA_50, EMA_200, 20_SMA, VWAP, CCI, MFI, OBV, MACD, MACD_Signal, MACD_Hist, Upper_Band, Lower_Band, Stoch_%K, Stoch_%D, ADX
- non-indicator columns (12): date, open, high, low, close, volume, Recent_High, Recent_Low, date_only, Intra_Change, Prev_Day_Close, Daily_Change

## Missing Required Columns
- 5-minute missing: none
- 1-minute missing: none

## Quality Issues / Caveats
- Current-date incomplete 1-minute rows are excluded by requiring >= 300 bars per symbol/session.
- Current-date or partial 5-minute rows are excluded by requiring >= 60 bars per symbol/session.
- VWAP quality: some older repo reports flagged stale anchored 5m VWAP in specific stores; this run uses the latest live2 5m store and also constrains by EMA/price action so VWAP is not the only trigger.
- Duplicate timestamps are dropped per symbol before feature generation and 1-minute exit simulation.
- Halted/thin sessions are indirectly filtered when per-symbol 1-minute coverage is below the completed-session threshold.

## Signal Pool Built From Raw 5-Minute Bars
- symbols considered: 1,285
- symbols with at least one trigger: 1,272
- unique signal candidates: 77,692
- rule-candidate rows: 442,395
- rules with hits: 42