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
| `eqidv1_live_combined_analyser.py` | Live combined analyser using refactored v11 modules |
| `eqidv1_live_combined_analyser_csv.py` | Same with CSV bridge using runner-parity scan flow |
| `eqidv1_live_combined_analyser_parquet.py` | Same with parquet output + CSV bridge |
| `eqidv1_live_trading_signal_15m_v11_combined_parquet.py` | Standalone live 15m signal scanner (inline v11 logic) |
| `eqidv1_live_fetch_n_latestsignalprint.py` | Quick fetch latest signal and print to console |
| `avwap_combined_runner.py` | Root-level combined backtest runner (imports refactored modules) |

### Trade Execution
| File | Role |
|------|------|
| `avwap_trade_execution_PAPER_TRADE_TRUE.py` | Paper trade simulator (Watchdog CSV monitor, LTP polling, concurrent threads) |
| `avwap_trade_execution_PAPER_TRADE_FALSE.py` | Real-order executor (Zerodha/Kite API, MARKET entry + LIMIT TP / SL-M orders) |
| `authentication.py` | Browser-assisted Kite login (Selenium + TOTP) |

### Data Ingestion
| File | Role |
|------|------|
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` | Multi-TF historical parquet builder (5m + 15m), incremental updates, warmup |
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py` | 15m-only lighter variant |
| `eqidv1_eod_scheduler_for_15mins_data.py` | Periodic 15m update scheduler (every 15m during market hours, 09:15–15:30) |
| `eqidv1_eod_scheduler_for_1540_update.py` | 15:40 IST EOD flush (final data snapshot) |

### Universe
| File | Role |
|------|------|
| `filtered_stocks.py` | Curated stock list (~400 stocks, `selected_stocks` list) |
| `filtered_stocks_MIS.py` | MIS-focused universe (~1045 stocks, `selected_stocks` sorted list) |

---

## 4) Data Flow Architecture

```
Historical Data (Zerodha Kite API)
        ↓
trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py
        ↓ (fetches + computes indicators)
stocks_indicators_15min_eq/   (15m parquet files per ticker)
stocks_indicators_5min_eq/    (5m parquet files per ticker)
        ↓
┌───────┴─────────────────────────────────────────┐
│                                                   │
│  Backtesting:                                    │
│   avwap_combined_runner.py                       │
│   ├─ 15m entries + 5m exit resolution            │
│   ├─ portfolio sim + metrics                     │
│   └─ outputs/avwap_longshort_trades_*.csv        │
│                                                   │
│  Live Signal Generation:                         │
│   eqidv1_live_combined_analyser_csv.py           │
│   └─ writes live_signals/signals_YYYY-MM-DD.csv  │
│                                                   │
│  Data Refresh:                                   │
│   eqidv1_eod_scheduler_for_15mins_data.py        │
│   eqidv1_eod_scheduler_for_1540_update.py        │
│                                                   │
│  Trade Execution:                                │
│   avwap_trade_execution_PAPER_TRADE_TRUE.py      │
│   avwap_trade_execution_PAPER_TRADE_FALSE.py     │
│   └─ reads live_signals/signals_*.csv            │
└──────────────────────────────────────────────────┘
```

---

## 5) Workflows & Commands

### A) One-Time Setup: Authentication
```bash
cd eqidv1
# Ensure api_key.txt exists with: api_key api_secret username password totp_secret
python authentication.py
# Outputs: request_token.txt, access_token.txt, NSE_BSE_instruments.csv
```

### B) Update Historical Data
```bash
cd eqidv1

# Full multi-TF update (5m + 15m)
python trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py

# 15m only (lighter, faster)
python trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py
```

### C) Run Backtest
```bash
cd eqidv1
python avwap_combined_runner.py
# or from refactored module:
python -m avwap_v11_refactored.avwap_combined_runner
```
**Outputs**: `outputs/avwap_longshort_trades_ALL_DAYS_*.csv`, analytics charts in `outputs/charts/`

### D) Live Signal Generation (continuous, runs during market hours)
```bash
cd eqidv1

# Preferred: CSV analyser with runner-parity scan flow
python eqidv1_live_combined_analyser_csv.py --verbose

# Alternative: parquet output
python eqidv1_live_combined_analyser_parquet.py

# Quick signal peek (prints top 30 signals per 15m slot)
python eqidv1_live_fetch_n_latestsignalprint.py
```

### E) Scheduled Data Refresh (run in separate terminals)
```bash
cd eqidv1

# Terminal 1: 15m data refresh every 15 minutes during market hours
python eqidv1_eod_scheduler_for_15mins_data.py --buffer-sec 75

# Terminal 2: EOD flush at 15:40 IST
python eqidv1_eod_scheduler_for_1540_update.py
```

### F) Trade Execution
```bash
cd eqidv1

# Paper mode (safe, recommended first)
python avwap_trade_execution_PAPER_TRADE_TRUE.py

# Real mode (requires authentication first!)
python authentication.py
python avwap_trade_execution_PAPER_TRADE_FALSE.py
```

### Full Live Deployment Flow (4 terminals)
```
Terminal 1: python authentication.py                           # auth (once)
Terminal 2: python eqidv1_eod_scheduler_for_15mins_data.py     # data refresh
Terminal 3: python eqidv1_live_combined_analyser_csv.py        # signal generation
Terminal 4: python avwap_trade_execution_PAPER_TRADE_TRUE.py   # execution (paper)
```

---

## 6) Known Issues & Bugs

### FIXED (in this review)

1. **`allow_signal_today()` wrong keyword argument** — was `allow_signal_today(state, ticker, sid=today_str)`, now correctly checks both SHORT and LONG caps separately with proper positional args. Fixed in `eqidv1_live_combined_analyser.py` and `eqidv1_live_combined_analyser_parquet.py`.

2. **Duplicate `_generate_signal_id` definition** — removed shadowed 3-arg version in `eqidv1_live_combined_analyser_csv.py`, keeping only the correct 5-arg version.

3. **`sys.path` manipulation with wrong relative path** — `eqidv1_live_trading_signal_15m_v11_combined_parquet.py` now uses `_ROOT` (the file's own directory) instead of the incorrect `_ROOT / "backtesting" / "eqidv1"`.

4. **`filtered_stocks_MIS.py` uses `set` instead of `list`** — converted to sorted list for deterministic ordering across Python runs.

### REMAINING (lower severity)

5. **Hardcoded XPath selectors in `authentication.py`** (LOW) — Zerodha UI changes will break the Selenium login flow. Consider switching to `By.NAME` or `By.CSS_SELECTOR` for more robust selectors.

6. **No error handling for missing `api_key.txt` / `request_token.txt`** (LOW) — `authentication.py` will crash with an unhelpful error if files are missing.

7. **Code duplication in `eqidv1_live_trading_signal_15m_v11_combined_parquet.py`** (LOW) — duplicates AVWAP v11 logic inline instead of importing refactored modules. Recommend refactoring to use `avwap_v11_refactored/` like the CSV/Parquet analysers.

8. **Real trade executor lacks order rejection handling** (MODERATE for production) — `avwap_trade_execution_PAPER_TRADE_FALSE.py` doesn't retry or notify on order failures. Consider adding Telegram/Email alerts.

---

## 7) Key Differences from eqidv2/eqidv3

| Feature | eqidv1 | eqidv2 | eqidv3 |
|---------|--------|--------|--------|
| ML filtering | No | Yes | Yes (enhanced) |
| Position sizing | Fixed | ML confidence-based | ML confidence + ATR vol cap |
| Feature set | N/A | 5 legacy features | 30 features (6 groups) |
| Model | N/A | LogReg/LightGBM | LightGBM + calibration |
| Risk controls | Basic (cap per ticker/day) | ML-gated | Full (kill-switch, max positions, etc.) |
