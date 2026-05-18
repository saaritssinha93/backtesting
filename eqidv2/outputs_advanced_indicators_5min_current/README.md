# Advanced Indicator Calculator Notes

- Data dir: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`
- Output dir: `outputs_advanced_indicators_5min_current`
- Processed OK: `1041`
- Processed errors: `1`
- Market file: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2\NIFTYBEES_stocks_indicators_5min.parquet`

## Missing Or Limited
- True bid/ask spread and order-book liquidity cannot be calculated from OHLCV parquet files.
- True volume delta/order flow cannot be calculated; buy/sell pressure is only a candle-direction proxy.
- News candle anchors require an external/manual event feed; this script uses day/opening/level-derived anchors only.
- Sector relative strength is calculated only when a matching sector index/ETF parquet exists in the 5m data folder.
- Backtest quality depends on data freshness; stale input folders will produce stale feature outputs.

## Main Outputs
- `features/<TICKER>_advanced_indicators_5min.parquet`
- `advanced_indicator_signals_all.csv`
- `advanced_indicator_top5_by_day_side.csv`
- `manifest.json`
