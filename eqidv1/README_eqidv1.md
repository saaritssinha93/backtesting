# EQIDV1 — Rule-Based AVWAP Rejection Intraday Strategy (Equities)

EQIDV1 is the **baseline rule-based** intraday strategy for Indian equities (NSE cash market). It uses 15-minute AVWAP rejection patterns (long + short) without ML filtering.

---

## 1) Architecture Overview

```
Data Ingestion → Signal Generation → Live Execution
     ↓                  ↓                   ↓
  Parquet 15m      AVWAP v11 rules     Paper/Real orders
  (historical)    (impulse + reject)   (Zerodha/Kite)
```

**No ML pipeline** — eqidv1 is purely rule-based. For ML-enhanced versions, see eqidv2/eqidv3.

---

## 2) Strategy Logic (AVWAP v11)

### Entry Conditions (SHORT)
1. **Impulse detection**: Red candle with body >= 0.45×ATR (MODERATE) or >= 1.60×ATR (HUGE)
2. **Trend filters**: ADX >= 25 (rising), RSI <= 55 (falling), StochK <= 75 (K < D, falling)
3. **EMA trend**: Close < EMA20 < EMA50
4. **AVWAP**: Close below AVWAP with rejection evidence
5. **Volume**: Impulse bar volume >= 1.2× SMA(20)
6. **ATR%**: ATR/close >= 0.20%
7. **Close-confirm**: Entry candle close confirms breakout direction

### Entry Conditions (LONG)
- Mirror of SHORT: green impulse, ADX rising, RSI >= 45 (rising), StochK >= 25 (K > D), close > EMA20 > EMA50, close above AVWAP

### Entry Setups
| Setup | Description |
|-------|-------------|
| A_MOD_BREAK_C1 | MODERATE impulse → next candle breaks impulse extreme |
| A_PULLBACK_C2 | MODERATE impulse → small pullback → C3 breaks C2 extreme |
| B_HUGE_FAILED_BOUNCE | HUGE impulse → small counter candles fail to reclaim AVWAP → breakdown |

### Risk Parameters
- **Stop-loss**: 0.75% from entry
- **Target**: SHORT 1.2%, LONG 1.5%
- **Cap**: 1 signal per ticker per day per side
- **Session**: 09:15–14:30 IST (signal windows: 09:15–11:30, 13:00–14:30)

---

## 3) File-by-File Map

### Strategy Engines (avwap_v11_refactored/)
| File | Role |
|------|------|
| `avwap_v11_refactored/avwap_common.py` | Shared `StrategyConfig`, indicators (ATR/RSI/Stoch/ADX/EMA/AVWAP), IO helpers, metrics |
| `avwap_v11_refactored/avwap_short_strategy.py` | SHORT rule engine: impulse detection, AVWAP rejection, entry/exit simulation |
| `avwap_v11_refactored/avwap_long_strategy.py` | LONG rule engine: impulse detection, AVWAP support, entry/exit simulation |
| `avwap_v11_refactored/avwap_combined_runner.py` | Orchestrator: parallel ticker scanning, 15m entry + 5m exit, portfolio sim, analytics |

### Live Signal Generation
| File | Role |
|------|------|
| `eqidv1_live_trading_signal_15m_v11_combined_parquet.py` | Main live 15m signal scanner (parquet-based, scheduler loop) |
| `eqidv1_live_combined_analyser.py` | Live combined analyser using refactored v11 modules |
| `eqidv1_live_combined_analyser_csv.py` | Same as above with CSV bridge using runner-parity scan flow |
| `eqidv1_live_combined_analyser_parquet.py` | Same as above with parquet output + CSV bridge |
| `eqidv1_live_fetch_n_latestsignalprint.py` | Quick fetch latest signal and print |
| `avwap_combined_runner.py` | Root-level combined backtest runner (imports refactored modules) |

### Trade Execution
| File | Role |
|------|------|
| `avwap_trade_execution_PAPER_TRADE_TRUE.py` | Paper trade simulator |
| `avwap_trade_execution_PAPER_TRADE_FALSE.py` | Real-order executor (Zerodha/Kite) |
| `authentication.py` | Browser-assisted Kite login (Selenium + TOTP) |

### Data Ingestion
| File | Role |
|------|------|
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` | Multi-TF historical parquet builder (5m + 15m) |
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py` | 15m-only lighter variant |
| `eqidv1_eod_scheduler_for_15mins_data.py` | Periodic 15m update scheduler |
| `eqidv1_eod_scheduler_for_1540_update.py` | 15:40 IST EOD flush |

### Universe
| File | Role |
|------|------|
| `filtered_stocks.py` | Curated stock list (~400 stocks, `selected_stocks` list) |
| `filtered_stocks_MIS.py` | MIS-focused universe (~900+ stocks, `selected_stocks` set) |

---

## 4) Workflows

### A) Run Backtest
```bash
cd eqidv1
python avwap_combined_runner.py
# or
python -m avwap_v11_refactored.avwap_combined_runner
```
Outputs: `outputs/avwap_longshort_trades_ALL_DAYS_*.csv`, analytics charts in `outputs/charts/`

### B) Update Historical Data
```bash
# Full multi-TF update
python trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py

# 15m only (lighter)
python trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py
```

### C) Live Signal Generation
```bash
# Continuous scanner (runs until 15:40 IST)
python eqidv1_live_trading_signal_15m_v11_combined_parquet.py

# Or the analyser variant with CSV bridge for trade executors
python eqidv1_live_combined_analyser_csv.py --verbose
```

### D) Trade Execution
```bash
# Paper mode
python avwap_trade_execution_PAPER_TRADE_TRUE.py

# Real mode (requires authentication first)
python authentication.py
python avwap_trade_execution_PAPER_TRADE_FALSE.py
```

---

## 5) Known Issues & Bugs

### CRITICAL

1. **`allow_signal_today()` wrong keyword argument** (`eqidv1_live_combined_analyser.py:942`, `eqidv1_live_combined_analyser_parquet.py:942`)
   - **Bug**: `allow_signal_today(state, ticker, sid=today_str)` — uses `sid=` but function signature expects positional args `(state, ticker, side, today, cap_per_day)`.
   - **Impact**: `TypeError` at runtime — live analyser crashes when checking signal caps.
   - **Fix**: Change to `allow_signal_today(state, ticker, side="SHORT", today=today_str, cap_per_day=SHORT_CAP_PER_TICKER_PER_DAY)` or equivalent.

2. **Duplicate `_generate_signal_id` definition** (`eqidv1_live_combined_analyser_csv.py:1114–1123`)
   - **Bug**: Two functions with the same name defined — the 3-arg version (line 1114) is immediately shadowed by a 5-arg version (line 1120).
   - **Impact**: Dead code; could cause confusion during maintenance.

### MODERATE

3. **`sys.path` manipulation with wrong relative path** (`eqidv1_live_trading_signal_15m_v11_combined_parquet.py:44`)
   - `_EQIDV1 = _ROOT / "backtesting" / "eqidv1"` — adds a path relative to the file's parent, but this assumes the file is one directory ABOVE `backtesting/`. When run from within `eqidv1/`, this path won't exist.
   - The `import core` on line 48 may fail depending on working directory.

4. **`filtered_stocks_MIS.py` uses a `set` instead of `list`**
   - `selected_stocks = {...}` — using a set means ordering is non-deterministic across Python runs. If downstream code depends on order (e.g., for reproducible backtests), results may vary.

5. **Missing `eqidv3` subdirectory reference** — `eqidv3/eqidv3/` nested directory exists but isn't documented and may cause import confusion.

### LOW

6. **Hardcoded XPath selectors in `authentication.py`** — Zerodha UI changes will break the Selenium login flow. Consider using the Kite Connect API directly.

7. **No error handling for missing `api_key.txt` / `request_token.txt`** — `authentication.py` will crash with an unhelpful error if files are missing.

---

## 6) Key Differences from eqidv2/eqidv3

| Feature | eqidv1 | eqidv2 | eqidv3 |
|---------|--------|--------|--------|
| ML filtering | No | Yes | Yes (enhanced) |
| Position sizing | Fixed | ML confidence-based | ML confidence + ATR vol cap |
| Feature set | N/A | 5 legacy features | 30 features (6 groups) |
| Model | N/A | LogReg/LightGBM | LightGBM + calibration |
| Risk controls | Basic (cap per ticker/day) | ML-gated | Full (kill-switch, max positions, etc.) |
