# EQIDV2 Reference Guide

Last updated: 2026-02-24 (IST)

This README maps the current `eqidv2` folder for both backtesting and live operations.

## 1) What this folder contains

`eqidv2` includes:

- Authentication + session management
- 15m/5m data ingestion and schedulers
- AVWAP signal generation (v1 and v2 live analyzers)
- Paper execution and live Kite execution
- Dashboard server + public URL/email tooling
- ML meta-label training/filtering pipeline

## 2) Typical operating flow

### A) Backtesting

1. Refresh historical candles/indicators.
2. Run AVWAP combined backtest runner.
3. Optionally run ML meta-label pipeline and compare base vs ML-filtered results.
4. Review CSV/TXT/PNG in `outputs/`.

### B) Live (paper or live orders)

1. Refresh/validate authentication.
2. Keep 15m data fresh during market session.
3. Run analyzer (`csv` or `csv_v2`) to generate live signals.
4. Run matching executor:
   - paper true (v1 or v2), or
   - live Kite executor (`PAPER_TRADE_FALSE.py`)
5. Monitor dashboard cards and logs.

## 3) Script map

### Authentication

- `authentication_v2.py`: Token lifecycle + refresh flow for intraday slots.
- `authentication_backup.py`: Alternate auth path.

### Data ingestion and schedulers

- `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py`: 5m + 15m fetch + indicators.
- `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py`: 15m-only fetch.
- `eqidv2_eod_scheduler_for_15mins_data.py`: Intraday scheduler for 15m updates.
- `eqidv2_eod_scheduler_for_1540_update.py`: Final 15:40 refresh run.

### Live signal generation

- `eqidv2_live_combined_analyser_csv.py`: Main live scanner writing `live_signals/signals_YYYY-MM-DD.csv`.
- `eqidv2_live_combined_analyser_csv_v2.py`: V2 scanner writing `live_signals/signals_YYYY-MM-DD_v2.csv`.
- `eqidv2_live_combined_analyser.py`: Compatibility shim.
- `eqidv2_live_combined_analyser_parquet.py`: Parquet-mode analyzer utility.
- `eqidv2_live_combined_analyser_rowbyrow.py`: Off-market row-by-row replay utility.
- `eqidv2_live_fetch_n_latestsignalprint.py`: Fetch + latest signal print helper.
- `eqidv2_live_trading_signal_15m_v11_combined_parquet.py`: Additional live scanner variant.
- `avwap_live_signal_generator.py`: AVWAP v11 live signal generator with ML filter integration.
- `eqidv2_daily_combined_analyser_csv.py`: Daily all-bar signal scan.
- `eqidv2_daily_combined_analyser_csv_tester.py`: Daily scan tester.
- `eqidv2_daily_combined_analyser_csv_tester_n_compare.py`: Daily compare helper.

### Execution engines

- `avwap_trade_execution_PAPER_TRADE_TRUE.py`
  - reads: `live_signals/signals_YYYY-MM-DD.csv`
  - writes: `live_signals/paper_trades_YYYY-MM-DD.csv`
  - state: `executed_signals_paper.json`, `paper_trade_summary.json`
  - forced close: 15:20 IST
- `avwap_trade_execution_PAPER_TRADE_TRUE_v2.py`
  - reads: `live_signals/signals_YYYY-MM-DD_v2.csv`
  - writes: `live_signals/paper_trades_YYYY-MM-DD_v2.csv`
  - state: `executed_signals_paper_v2.json`, `paper_trade_summary_v2.json`
  - forced close: 15:20 IST
- `avwap_trade_execution_PAPER_TRADE_FALSE.py` (real orders via Kite)
  - reads: `live_signals/signals_YYYY-MM-DD.csv`
  - writes: `live_signals/live_trades_YYYY-MM-DD.csv`
  - state: `executed_signals_live.json`, `live_trade_summary.json`
  - forced close: 15:20 IST

### Backtesting engines

- `avwap_combined_runner.py`
- `avwap_v11_refactored/avwap_common.py`
- `avwap_v11_refactored/avwap_long_strategy.py`
- `avwap_v11_refactored/avwap_short_strategy.py`
- `avwap_v11_refactored/avwap_combined_runner.py`
- `summarize_latest_outputs_csv.py`

### ML pipeline

- `eqidv2_meta_label_triple_barrier.py`
- `eqidv2_meta_train_walkforward.py`
- `ml_meta_filter.py`
- `eqidv2_ml_backtest.py`
- `avwap_ml_backtest_runner.py`

### Universe/config

- `filtered_stocks.py`
- `filtered_stocks_MIS.py`

## 4) Dashboard and observability

### Dashboard service files

- `log_dashboard_server.py`
- `bat/run_log_dashboard_server.bat`
- `bat/run_log_dashboard_public_link.bat`
- `bat/run_log_dashboard_public_link_capture.ps1`
- `bat/run_log_dashboard_public_link_scheduled.bat`
- `bat/run_log_dashboard_stop.bat`
- `bat/send_gmail_api.py`

### Dashboard cards currently configured

- Live Data Fetch (15mins)
- Live Analysis And Signal Generation
- Live Analysis And Signal Generation V2
- Live Entries CSV
- Live Entries CSV V2
- Live Papertrade Result CSV
- Live Papertrade Result CSV V2
- Papertrade Runner view
- Papertrade Runner View V2
- Live Kite Trades Log
- Live Kite Trades CSV
- Auth_V2
- Live EOD Data Fetch

## 5) BAT runners

- `bat/run_authentication_backup.bat`
- `bat/run_authentication_v2.bat`
- `bat/run_eqidv2_eod_scheduler_for_15mins_data.bat`
- `bat/run_eqidv2_eod_scheduler_for_1540_update.bat`
- `bat/run_eqidv2_live_combined_analyser_csv.bat`
- `bat/run_eqidv2_live_combined_analyser_csv_v2.bat`
- `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE.bat`
- `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v2.bat`
- `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE.bat`
- `bat/run_log_dashboard_server.bat`
- `bat/run_log_dashboard_public_link.bat`
- `bat/run_log_dashboard_public_link_scheduled.bat`
- `bat/run_log_dashboard_stop.bat`
- `bat/schedule_log_dashboard_weekday.bat`
- `bat/run_log_dashboard_gmail_api_bootstrap.bat`

Notes:

- Most runtime BATs have restart loops and cutoff checks.
- `run_log_dashboard_server.bat` currently uses `END_CUTOFF_HHMM=2359`.

## 6) Scheduled tasks on this machine

Detected EQIDV2 tasks:

| Task Name | Schedule | Command |
|---|---|---|
| EQIDV2_authentication_v2_0900 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 09:00 | `...\run_authentication_v2.bat --test-now` |
| EQIDV2_avwap_paper_trade_0900 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 09:00 | `...\run_avwap_trade_execution_PAPER_TRADE_TRUE.bat` |
| EQIDV2_avwap_paper_trade_v2_0900 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 09:00 | `...\run_avwap_trade_execution_PAPER_TRADE_TRUE_v2.bat` |
| EQIDV2_eod_1540_update_1540 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 15:40 | `...\run_eqidv2_eod_scheduler_for_1540_update.bat` |
| EQIDV2_eod_15mins_data_0900 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 09:00 | `...\run_eqidv2_eod_scheduler_for_15mins_data.bat` |
| EQIDV2_live_combined_csv_0900 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 09:00 | `...\run_eqidv2_live_combined_analyser_csv.bat` |
| EQIDV2_live_combined_csv_v2_0900 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 09:00 | `...\run_eqidv2_live_combined_analyser_csv_v2.bat` |
| EQIDV2_log_dashboard_start_0855 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 08:55 | `...\run_log_dashboard_public_link_scheduled.bat` |
| EQIDV2_log_dashboard_stop_1615 | Weekly [Mon,Tue,Wed,Thu,Fri] @ 16:15 | `cmd /c "call ...\run_log_dashboard_stop.bat"` |

## 7) Live file patterns

### Signals

- `live_signals/signals_YYYY-MM-DD.csv`
- `live_signals/signals_YYYY-MM-DD_v2.csv`

### Paper results

- `live_signals/paper_trades_YYYY-MM-DD.csv`
- `live_signals/paper_trades_YYYY-MM-DD_v2.csv`
- `live_signals/paper_trade_summary.json`
- `live_signals/paper_trade_summary_v2.json`

### Live Kite results

- `live_signals/live_trades_YYYY-MM-DD.csv`
- `live_signals/live_trade_summary.json`
- `live_signals/live_trade_execution.log`
- `logs/avwap_trade_execution_PAPER_TRADE_FALSE_YYYY-MM-DD.log`

## 8) Quick commands

Run from `backtesting/eqidv2/backtesting/eqidv2`:

```bat
call .\bat\run_authentication_v2.bat
call .\bat\run_eqidv2_eod_scheduler_for_15mins_data.bat
call .\bat\run_eqidv2_live_combined_analyser_csv.bat
call .\bat\run_eqidv2_live_combined_analyser_csv_v2.bat
call .\bat\run_avwap_trade_execution_PAPER_TRADE_TRUE.bat
call .\bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_v2.bat
call .\bat\run_avwap_trade_execution_PAPER_TRADE_FALSE.bat
call .\bat\run_log_dashboard_public_link.bat
```

## 9) Active source files scanned for this update

### Python

- `authentication_backup.py`
- `authentication_v2.py`
- `avwap_combined_runner.py`
- `avwap_live_signal_generator.py`
- `avwap_ml_backtest_runner.py`
- `avwap_trade_execution_PAPER_TRADE_FALSE.py`
- `avwap_trade_execution_PAPER_TRADE_TRUE.py`
- `avwap_trade_execution_PAPER_TRADE_TRUE_v2.py`
- `avwap_v11_refactored/avwap_combined_runner.py`
- `avwap_v11_refactored/avwap_common.py`
- `avwap_v11_refactored/avwap_long_strategy.py`
- `avwap_v11_refactored/avwap_short_strategy.py`
- `bat/send_gmail_api.py`
- `eqidv2_daily_combined_analyser_csv.py`
- `eqidv2_daily_combined_analyser_csv_tester.py`
- `eqidv2_daily_combined_analyser_csv_tester_n_compare.py`
- `eqidv2_eod_scheduler_for_1540_update.py`
- `eqidv2_eod_scheduler_for_15mins_data.py`
- `eqidv2_live_combined_analyser.py`
- `eqidv2_live_combined_analyser_csv.py`
- `eqidv2_live_combined_analyser_csv_v2.py`
- `eqidv2_live_combined_analyser_parquet.py`
- `eqidv2_live_combined_analyser_rowbyrow.py`
- `eqidv2_live_fetch_n_latestsignalprint.py`
- `eqidv2_live_trading_signal_15m_v11_combined_parquet.py`
- `eqidv2_meta_label_triple_barrier.py`
- `eqidv2_meta_train_walkforward.py`
- `eqidv2_ml_backtest.py`
- `filtered_stocks.py`
- `filtered_stocks_MIS.py`
- `log_dashboard_server.py`
- `ml_meta_filter.py`
- `summarize_latest_outputs_csv.py`
- `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py`
- `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py`

### BAT / PowerShell

- `bat/run_authentication_backup.bat`
- `bat/run_authentication_v2.bat`
- `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE.bat`
- `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE.bat`
- `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v2.bat`
- `bat/run_eqidv2_eod_scheduler_for_1540_update.bat`
- `bat/run_eqidv2_eod_scheduler_for_15mins_data.bat`
- `bat/run_eqidv2_live_combined_analyser_csv.bat`
- `bat/run_eqidv2_live_combined_analyser_csv_v2.bat`
- `bat/run_log_dashboard_gmail_api_bootstrap.bat`
- `bat/run_log_dashboard_public_link.bat`
- `bat/run_log_dashboard_public_link_capture.ps1`
- `bat/run_log_dashboard_public_link_scheduled.bat`
- `bat/run_log_dashboard_server.bat`
- `bat/run_log_dashboard_stop.bat`
- `bat/schedule_log_dashboard_weekday.bat`

## 10) Operational notes

- Keep broker and Gmail credentials out of repository commits.
- Use matching analyzer/executor pairs (`v1` with `v1`, `v2` with `v2`) when running in parallel.
- Public dashboard URLs are temporary (`trycloudflare`) and change after restart.
