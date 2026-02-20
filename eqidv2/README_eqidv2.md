# EQIDV2 — Folder Guide (AVWAP + Live Trading + Meta-Label ML)

This README is an updated map of the `eqidv2` folder based on the current branch contents.

---

## 1) What is in `eqidv2`

`eqidv2` combines four major parts:

1. **Historical data ingestion and refresh** (5m/15m parquet builders + EOD schedulers)
2. **Signal generation** (AVWAP long/short scans and live signal emitters)
3. **Execution** (paper and real-order executors)
4. **ML overlay** (meta-label dataset creation, walk-forward training, and ML-filtered backtests)

---

## 2) File-by-file quick map

### Core strategy / backtest engines
- `avwap_combined_runner.py` — monolithic AVWAP long+short scanner/backtest runner (legacy-style all-in-one).
- `avwap_v11_refactored/avwap_common.py` — shared dataclasses/config/helpers for refactored AVWAP flows.
- `avwap_v11_refactored/avwap_long_strategy.py` — refactored long strategy scan and exits.
- `avwap_v11_refactored/avwap_short_strategy.py` — refactored short strategy scan and exits.
- `avwap_v11_refactored/avwap_combined_runner.py` — orchestrates refactored long/short runners.

### Live signal generation / analysis
- `eqidv2_live_trading_signal_15m_v11_combined_parquet.py` — live 15m combined AVWAP signal scanner.
- `eqidv2_live_combined_analyser.py` — analyser variant for live combined signal evaluation.
- `avwap_live_signal_generator.py` — ML-aware live signal generator that writes actionable signals.

### Trade execution
- `avwap_trade_execution_PAPER_TRADE_TRUE.py` — paper trade execution simulator.
- `avwap_trade_execution_PAPER_TRADE_FALSE.py` — real-order executor (Zerodha/Kite based).
- `authentication.py` — login/session bootstrap helper.

### Data ingestion / schedulers
- `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` — multi-timeframe historical parquet updater.
- `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py` — 15-minute-focused data updater.
- `eqidv2_eod_15min_data_stocks.py` — end-of-day 15m updater.
- `eqidv2_eod_scheduler_for_15mins_data.py` — periodic schedule wrapper for 15m updates.
- `eqidv2_eod_scheduler_for_1540_update.py` — fixed-time (~15:40) EOD scheduler.

### ML meta-label pipeline
- `ml_meta_filter.py` — inference + baseline training utilities for trade filtering.
- `eqidv2_meta_label_triple_barrier.py` — generates triple-barrier labels for candidate trades.
- `eqidv2_meta_train_walkforward.py` — walk-forward model training/evaluation.
- `eqidv2_ml_backtest.py` — applies ML filter over historical trade CSVs and summarizes results.
- `avwap_ml_backtest_runner.py` — richer performance report runner for ML-filtered backtests.

### Universe / config / artifacts
- `filtered_stocks.py` — stock universe list.
- `filtered_stocks_MIS.py` — MIS list placeholder.
- `models/meta_model.pkl` — serialized trained meta model.
- `models/meta_features.json` — model feature schema.
- `stocks_tokens_cache.json` — instrument token cache.
- `live_signals/.seen_signals.json` — dedupe state for emitted live signals.

---

## 3) Typical workflows

### A) Refresh candle data
1. Run one of the parquet updater scripts:
   - full/multi-timeframe: `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py`
   - 15m-focused: `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py`
2. Optionally run the EOD schedulers for automated refresh windows.

### B) Generate strategy signals
1. For historical scans/backtests, run one of the AVWAP runners (`avwap_combined_runner.py` or refactored equivalent).
2. For live signal generation, use:
   - `eqidv2_live_trading_signal_15m_v11_combined_parquet.py`, and/or
   - `avwap_live_signal_generator.py` (with ML gating fields like `p_win`, threshold, confidence multiplier).

### C) Execute trades
- Paper mode: `avwap_trade_execution_PAPER_TRADE_TRUE.py`
- Real mode: `avwap_trade_execution_PAPER_TRADE_FALSE.py`

### D) Train + apply ML overlay
1. Build meta-label dataset: `eqidv2_meta_label_triple_barrier.py`
2. Train with walk-forward validation: `eqidv2_meta_train_walkforward.py`
3. Run filtered backtest/report:
   - `eqidv2_ml_backtest.py`
   - optionally `avwap_ml_backtest_runner.py` for richer metrics.

---

## 4) Minimal command examples

```bash
# 1) Generate ML-filtered backtest summary from base trade CSV
python eqidv2/eqidv2_ml_backtest.py \
  --input-csv eqidv2/outputs/base_trades.csv \
  --output-csv eqidv2/outputs/ml_filtered_trades.csv \
  --summary-json eqidv2/outputs/ml_backtest_summary.json \
  --threshold 0.60

# 2) Emit live ML-gated signals
python eqidv2/avwap_live_signal_generator.py --ml-threshold 0.62

# 3) Run paper execution loop
python eqidv2/avwap_trade_execution_PAPER_TRADE_TRUE.py
```

---

## 5) Notes

- Keep credentials and broker session handling isolated in `authentication.py` and runtime env/config files.
- The `meta_model.pkl` + `meta_features.json` pair should be versioned together.
- If both old and refactored AVWAP runners exist, prefer one path per deployment to avoid configuration drift.
