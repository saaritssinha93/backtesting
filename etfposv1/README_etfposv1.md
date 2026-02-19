# ETFPOSV1

ETFPOSV1 is a LONG-only multi-timeframe ETF strategy for backtesting and live signal monitoring.

## Strategy Flow

1. Weekly filter:
- `EMA_50_W > EMA_200_W`

2. Daily filter:
- Previous day daily change in allowed range.

3. 15m entry trigger:
- Trend + momentum + volatility + breakout checks on 15m indicators.

4. Exit logic:
- Target-based exit (no stop-loss in current positional design).

5. Capital model:
- Cash-constrained fixed capital-per-trade.
- Multiple concurrent entries allowed (no ticker lock).

## Production Files Kept

### Data + Indicators
- `algosm1_trading_data_continous_run_historical_alltf_v3_parquet_etfsonly.py`
- `etf_eod_daily_weekly_scheduler_for_15mins_data.py`
- `etf_eod_daily_weekly_scheduler_for_daily_1540_update.py`
- `etf_filtered_etfs_all.py`

### Backtesting
- `algosm1_trading_signal_profit_analysis_etf_15m_v7_parquet.py`

### Live Signals
- `etf_live_trading_signal_15m_v7_parquet.py`

### Paper Trading (Recurring + Traceable)
- `etf_positional_paper_trader.py`
- `avwap_trade_execution_PAPER_TRADE_TRUE.py` (compatibility wrapper to paper trader)
- `avwap_trade_execution_PAPER_TRADE_FALSE.py` (compatibility wrapper; real-order mode disabled)

### Auth
- `authentication.py`

### Batch Runners
- `bat/run_auth.bat`
- `bat/run_15m_updater.bat`
- `bat/run_live_signals.bat`
- `bat/run_eod_1540.bat`
- `bat/run_paper_trader.bat`

## Runbook

### 1) Authenticate
```bash
python authentication.py
```

### 2) Keep 15m/Daily/Weekly data fresh
```bash
python etf_eod_daily_weekly_scheduler_for_15mins_data.py
python etf_eod_daily_weekly_scheduler_for_daily_1540_update.py
```

### 3) Run live signal scanner
```bash
python etf_live_trading_signal_15m_v7_parquet.py
```

### 4) Run recurring positional paper-trader
```bash
python etf_positional_paper_trader.py --interval-sec 60
```

Behavior:
- Positional carry-forward only (no intraday force close).
- Open positions continue across days until target exit.
- Use `--trading-hours-only` only if you explicitly want scans restricted to market hours.

### 5) Run backtest
```bash
python algosm1_trading_signal_profit_analysis_etf_15m_v7_parquet.py
```

## Paper Trader Outputs

Saved under `paper_trade/`:
- `paper_trader_state.json` (persistent state)
- `trade_book.csv` (all open + closed paper trades)
- `cash_ledger.csv` (every debit/credit event)
- `open_positions_latest.csv` (current open positions)
- `run_snapshots.csv` (equity/cash snapshots per run)
- `status_latest.txt` (quick human-readable summary)

## Notes

- Paths are anchored to the script directory where needed, so execution is robust from different working directories.
- Batch files are now relative-path safe and log to `logs/` under this folder.
