# ETFPOSV1 — Multi-TF ETF Positional/Intraday LONG Strategy

ETFPOSV1 is a **LONG-only** positional/intraday strategy for **Indian ETFs** (primarily gold/silver ETFs). It uses a multi-timeframe approach: weekly trend filter + daily confirmation + 15-minute entry timing.

---

## 1) Architecture Overview

```
Data Ingestion → Signal Generation → Target Evaluation → Portfolio Simulation
     ↓                  ↓                   ↓                    ↓
  Parquet ETFs     Weekly + Daily      15m candle target     Cash-constrained
  (15m/daily/wk)    + 15m filters       evaluation (+N%)     position sizing
```

**Key differences from eqidv1/v2/v3:**
- **ETF-focused** (SILVERBEES, GOLDBEES, etc.) — not broad equity universe
- **LONG-only** — no SHORT signals
- **Multi-timeframe** — weekly EMA filter → daily confirmation → 15m entry
- **No ML pipeline** — purely rule-based
- **No AVWAP** — uses EMA crossover + RSI + Stochastic signals

---

## 2) Strategy Logic (ALGO-SM1 v7)

### Weekly Filter
- `weekly_ok = EMA_50_W > EMA_200_W` (bullish weekly trend)

### Daily Context
- Daily indicators merged for trend confirmation

### 15-Minute Entry Conditions
- **Signal mask**: Combination of EMA crossover, RSI, and Stochastic conditions on 15m candles
- **No ticker lock**: Multiple signals per ticker allowed simultaneously
- **Target**: Fixed percentage target (e.g., +1% or +2%) on 15m candle data

### Portfolio Simulation
- Fixed per-trade sizing from `MAX_CAPITAL_RS`
- Cash-constrained: only takes new trades if capital available
- Unrealised (open) trades kept with MTM at last available close
- Reports both realised and unrealised P&L

---

## 3) File-by-File Map

### Backtesting
| File | Role |
|------|------|
| `algosm1_trading_signal_profit_analysis_etf_15m_v7_parquet.py` | Main backtest: multi-TF signal generation + target evaluation + portfolio sim |
| `algosm1_trading_signal_profit_analysis_etf_15m_v7.py` | Earlier CSV-based version of the backtest |
| `algosm1_trading_signal_profit_analysis_etf_15m_v7_parquet_old.py` | Archived older parquet version |

### Live Signal Generation
| File | Role |
|------|------|
| `etf_live_trading_signal_15m_v7_parquet.py` | Live 15m signal checker (scheduled runner, dual checks per slot) |
| `etf_live_trading_signal_15m_v7_parquet_fulltest.py` | Full test variant of live signal checker |
| `etf_live_trading_signal_15m_v7_parquet_fulltest_today.py` | Today-only variant |
| `etf_get_signals_to_csv.py` | Export signals to CSV for downstream consumption |

### Data Ingestion
| File | Role |
|------|------|
| `algosm1_trading_data_continous_run_historical_alltf_v3_parquet_etfsonly.py` | Multi-TF ETF parquet builder (15m + daily + weekly) |
| `etf_eod_daily_weekly_scheduler_for_15mins_data.py` | Periodic 15m + daily + weekly update scheduler |
| `etf_eod_daily_weekly_scheduler_for_daily_1540_update.py` | 15:40 IST EOD flush for ETFs |

### Universe & Auth
| File | Role |
|------|------|
| `etf_filtered_etfs.py` | ETF universe: `{'SILVERBEES', 'GOLDBEES'}` |
| `etf_filtered_etfs_all.py` | Expanded ETF universe |
| `authentication.py` | Broker session bootstrap (Zerodha/Kite) |
| `zerodha_kite_export.py` | Kite data export utility |

---

## 4) Workflows

### A) Run Backtest
```bash
cd etfposv1
python algosm1_trading_signal_profit_analysis_etf_15m_v7_parquet.py
```
Outputs: `out_etfposv1/multi_tf_signals_*.parquet`, daily counts, target evaluation parquets

### B) Update Historical Data
```bash
python algosm1_trading_data_continous_run_historical_alltf_v3_parquet_etfsonly.py
```

### C) Live Signal Generation
```bash
# Start before market opens (~09:10 IST)
python etf_live_trading_signal_15m_v7_parquet.py
```
Outputs: `out_live_checks/YYYYMMDD/checks_*.parquet`, `out_live_signals/YYYYMMDD/signals_*.parquet`

### D) Export Signals to CSV
```bash
python etf_get_signals_to_csv.py
```

---

## 5) Known Issues & Bugs

### MODERATE

1. **`etf_filtered_etfs.py` uses `set` instead of `list`** — `selected_etfs = {'SILVERBEES', 'GOLDBEES'}`. Non-deterministic ordering; minimal impact with only 2 ETFs but could matter if expanded.

2. **`etf_filtered_etfs_all.py` also uses `set`** — same issue with larger universe.

3. **Hardcoded parquet directory names** — Data directories (`etf_indicators_15min_pq/`, `etf_indicators_daily_pq/`, `etf_indicators_weekly_pq/`) are hardcoded as relative paths. Script must be run from the correct working directory.

4. **No SL/risk management** — Strategy uses fixed target exit but no explicit stop-loss. Unrealised positions can accumulate indefinitely.

### LOW

5. **Duplicated `_require_pyarrow()` helper** — Same function copied across multiple files instead of shared from a common module.

6. **Archived files** (`*_old.py`) — Dead code that should be removed or moved to an archive directory.

---

## 6) Key Configuration

Default constants in `algosm1_trading_signal_profit_analysis_etf_15m_v7_parquet.py`:

```python
DIR_15M   = "etf_indicators_15min_pq"
DIR_DAILY = "etf_indicators_daily_pq"
DIR_WEEK  = "etf_indicators_weekly_pq"
TARGET_PCT = 1.0   # % target for evaluation
MAX_CAPITAL_RS = 500_000  # total capital for portfolio sim
```
