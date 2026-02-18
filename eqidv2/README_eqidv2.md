# EQIDV2 — ML-Filtered AVWAP Rejection Intraday Strategy (Equities)

EQIDV2 extends eqidv1 by adding an **ML meta-label filter** over the same AVWAP v11 rule-based signals. Trades are gated by a trained model's `p_win` prediction and sized by confidence.

---

## 1) Architecture Overview

```
Data Ingestion → Signal Generation → ML Filter/Size → Live Execution
     ↓                  ↓                  ↓               ↓
  Parquet 15m      AVWAP v11 rules    LightGBM/LogReg  Paper/Real orders
  (historical)    (impulse + reject)   p_win gating    (Zerodha/Kite)
```

---

## 2) What eqidv2 Adds Over eqidv1

| Feature | eqidv1 | eqidv2 |
|---------|--------|--------|
| Signal generation | AVWAP v11 rules | Same AVWAP v11 rules |
| ML gating | None | p_win threshold (take/skip) |
| Position sizing | Fixed | Confidence-based multiplier |
| Feature set | N/A | Legacy 5-feature set |
| Training | N/A | Walk-forward LightGBM/LogReg |
| Labeling | N/A | Triple-barrier R_net method |

---

## 3) Strategy Logic

Same as eqidv1 (see `eqidv1/README_eqidv1.md`) for entry/exit rules, with the addition of:

### ML Meta-Label Filter
- After a raw AVWAP signal is generated, `ml_meta_filter.py` predicts `p_win` (probability of profitable trade)
- **Gate**: Only take trades where `p_win >= threshold` (default: ~0.62)
- **Size**: Confidence multiplier `m = clip(0.7, 1.2, f(p_win))` scales position size
- **Fallback**: If model not available, uses heuristic sigmoid scoring

### 5 Legacy Features
| Feature | Description |
|---------|-------------|
| `quality_score` | Signal quality from AVWAP v11 rules |
| `atr_pct` | ATR as percentage of close price |
| `rsi_centered` | RSI centered around 50 |
| `adx_norm` | ADX normalized |
| `side` | +1 (LONG) / -1 (SHORT) |

### Risk Controls (via MetaFilterConfig)
- Max open positions: 3
- Max trades/day: 10, per ticker/day: 1
- Daily loss kill-switch: -1.0R or -0.8% capital
- No entry after 14:30 IST
- EOD force exit, min 4 bars left in day

---

## 4) File-by-File Map

### ML Pipeline
| File | Role |
|------|------|
| `ml_meta_filter.py` | Core ML infrastructure: feature builder, `MetaLabelFilter` inference, confidence sizing |
| `eqidv2_meta_label_triple_barrier.py` | Build labeled dataset: R_net labels from candle data with slippage model |
| `eqidv2_meta_train_walkforward.py` | Walk-forward training: LightGBM/LogReg, calibration, threshold optimization |
| `eqidv2_ml_backtest.py` | Standalone ML filter over trade CSVs |
| `avwap_ml_backtest_runner.py` | Full backtest runner: RAW vs ML comparison with stats + charts |

### Signal Generation
| File | Role |
|------|------|
| `avwap_live_signal_generator.py` | ML-gated live signal scanner (15m candles) |
| `eqidv2_live_combined_analyser.py` | Live combined signal evaluation |
| `eqidv2_live_combined_analyser_csv.py` | Same with CSV bridge for trade executors |
| `eqidv2_live_combined_analyser_parquet.py` | Same with parquet output |
| `eqidv2_live_trading_signal_15m_v11_combined_parquet.py` | Standalone live 15m signal scanner |
| `eqidv2_live_fetch_n_latestsignalprint.py` | Quick fetch latest signal and print |
| `eqidv2_daily_combined_analyser_csv.py` | Daily batch analyser |

### Strategy Engines (avwap_v11_refactored/)
| File | Role |
|------|------|
| `avwap_v11_refactored/avwap_common.py` | Shared `StrategyConfig`, indicators, IO helpers, metrics |
| `avwap_v11_refactored/avwap_short_strategy.py` | SHORT rule engine |
| `avwap_v11_refactored/avwap_long_strategy.py` | LONG rule engine |
| `avwap_v11_refactored/avwap_combined_runner.py` | Combined backtest orchestrator |

### Trade Execution
| File | Role |
|------|------|
| `avwap_trade_execution_PAPER_TRADE_TRUE.py` | Paper trade simulator (Watchdog, LTP polling, concurrent threads) |
| `avwap_trade_execution_PAPER_TRADE_FALSE.py` | Real-order executor (Zerodha/Kite, MARKET + LIMIT TP / SL-M) |
| `authentication.py` | Broker session bootstrap (Selenium + TOTP) |

### Data Ingestion
| File | Role |
|------|------|
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` | Multi-TF historical parquet builder |
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py` | 15m-only variant |
| `eqidv2_eod_scheduler_for_15mins_data.py` | Periodic 15m update scheduler (with monkey-patch fixes) |
| `eqidv2_eod_scheduler_for_1540_update.py` | 15:40 IST EOD flush |

### Universe
| File | Role |
|------|------|
| `filtered_stocks.py` | Curated stock universe (~400 stocks, list) |
| `filtered_stocks_MIS.py` | MIS-focused universe (~1045 stocks, sorted list) |

---

## 5) Data Flow Architecture

```
Zerodha Kite API
     ↓
trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py
     ↓ (OHLCV + indicators)
stocks_indicators_15min_eq/  &  stocks_indicators_5min_eq/
     ↓
┌────┴──────────────────────────────────────────────────────┐
│                                                            │
│  Backtesting:                                             │
│   avwap_combined_runner.py → outputs/trades.csv            │
│                    ↓                                       │
│   eqidv2_meta_label_triple_barrier.py → meta_dataset.csv   │
│                    ↓                                       │
│   eqidv2_meta_train_walkforward.py → models/meta_model.pkl │
│                    ↓                                       │
│   avwap_ml_backtest_runner.py → RAW vs ML comparison       │
│                                                            │
│  Live:                                                    │
│   eqidv2_live_combined_analyser_csv.py                     │
│     + ml_meta_filter.py (p_win gate + sizing)              │
│     → live_signals/signals_YYYY-MM-DD.csv                  │
│                    ↓                                       │
│   avwap_trade_execution_PAPER_TRADE_TRUE.py                │
│     → live_signals/paper_trades_YYYY-MM-DD.csv             │
└────────────────────────────────────────────────────────────┘
```

---

## 6) Workflows & Commands

### A) Build ML Dataset
```bash
cd eqidv2

# 1. Generate candidate trades via backtest
python avwap_combined_runner.py

# 2. Label trades with triple-barrier R_net
python eqidv2_meta_label_triple_barrier.py \
  --trades-csv outputs/avwap_longshort_trades_ALL_DAYS.csv \
  --candles-dir stocks_indicators_5min_eq \
  --out-csv meta_dataset.csv \
  --horizon-bars 6 --bar-minutes 5
```

### B) Train ML Model
```bash
cd eqidv2
python eqidv2_meta_train_walkforward.py \
  --dataset-csv meta_dataset.csv \
  --out-model models/meta_model.pkl \
  --out-features models/meta_features.json \
  --out-report outputs/meta_train_report.json \
  --train-days 60 --test-days 10
```

### C) Run ML-Filtered Backtest
```bash
cd eqidv2

# Quick filter
python eqidv2_ml_backtest.py \
  --input-csv outputs/base_trades.csv \
  --output-csv outputs/ml_filtered_trades.csv \
  --threshold 0.62

# Full RAW vs ML comparison
python avwap_ml_backtest_runner.py \
  --ml-threshold 0.62 \
  --model-path models/meta_model.pkl \
  --features-path models/meta_features.json
```

### D) Live Signal Generation
```bash
cd eqidv2

# ML-gated live signals (continuous)
python avwap_live_signal_generator.py \
  --ml-threshold 0.62 \
  --model-path models/meta_model.pkl \
  --features-path models/meta_features.json

# Single scan
python avwap_live_signal_generator.py --run-once
```

### E) Trade Execution
```bash
cd eqidv2
python authentication.py              # first, authenticate
python avwap_trade_execution_PAPER_TRADE_TRUE.py   # paper
python avwap_trade_execution_PAPER_TRADE_FALSE.py  # real
```

### Full Live Deployment Flow (5 terminals)
```
Terminal 1: python authentication.py                            # auth (once)
Terminal 2: python eqidv2_eod_scheduler_for_15mins_data.py      # data refresh
Terminal 3: python eqidv2_live_combined_analyser_csv.py         # signal generation
Terminal 4: python avwap_live_signal_generator.py --ml-threshold 0.62  # ML gate
Terminal 5: python avwap_trade_execution_PAPER_TRADE_TRUE.py    # execution
```

### Full ML Training Pipeline (run once, retrain monthly)
```
Step 1: python avwap_combined_runner.py                         # generate raw trades
Step 2: python eqidv2_meta_label_triple_barrier.py [args]       # label trades
Step 3: python eqidv2_meta_train_walkforward.py [args]          # train model
Step 4: python avwap_ml_backtest_runner.py [args]               # validate
```

---

## 7) Known Issues & Bugs

### FIXED (in this review)

1. **`allow_signal_today()` wrong keyword argument** — was `allow_signal_today(state, ticker, sid=today_str)`, now correctly checks both SHORT and LONG caps separately. Fixed in `eqidv2_live_combined_analyser.py` and `eqidv2_live_combined_analyser_parquet.py`.

2. **Duplicate `_generate_signal_id` definition** — removed shadowed 3-arg version in `eqidv2_live_combined_analyser_csv.py`, keeping only the correct 5-arg version.

3. **`sys.path` manipulation with wrong relative path** — `eqidv2_live_trading_signal_15m_v11_combined_parquet.py` now uses `_ROOT` directly instead of the incorrect `_ROOT / "backtesting" / "eqidv2"`.

4. **`filtered_stocks_MIS.py` uses `set` instead of `list`** — converted to sorted list for deterministic ordering.

### REMAINING (lower severity)

5. **Hardcoded XPath selectors in `authentication.py`** (LOW) — brittle against Zerodha UI changes.

6. **ML heuristic fallback uses hardcoded weights** (LOW) — `ml_meta_filter.py` fallback sigmoid may diverge from trained model behavior.

7. **Threading race conditions in paper trade executor** (MODERATE) — concurrent threads access shared `executed_signals_dict` without explicit locking.

8. **Real trade executor lacks order rejection handling** (MODERATE for production) — no retry or notification on order failures.

9. **Hard-coded position sizes** (LOW) — `POSITION_SIZE_RS_SHORT = 50,000`, `POSITION_SIZE_RS_LONG = 100,000` in backtest runner. Should be configurable.

---

## 8) Key Configuration

### MetaFilterConfig (ml_meta_filter.py)
```python
MetaFilterConfig(
    model_path="eqidv2/models/meta_model.pkl",
    feature_path="eqidv2/models/meta_features.json",
    pwin_threshold=0.62,
    risk_per_trade_pct=0.20,
    conf_mult_min=0.7,
    conf_mult_max=1.2,
    max_open_positions=3,
    max_trades_per_day=10,
    max_trades_per_ticker_per_day=1,
    daily_loss_kill_R=-1.0,
    no_entry_after_ist="14:30",
    slippage_bps=3.0,
    commission_bps=2.0,
)
```

---

## 9) Key Differences: eqidv1 vs eqidv2 vs eqidv3

| Feature | eqidv1 | eqidv2 | eqidv3 |
|---------|--------|--------|--------|
| ML filtering | No | Yes (legacy 5 features) | Yes (30 features, 6 groups) |
| Model | N/A | LightGBM/LogReg | LightGBM + calibration (isotonic/sigmoid) |
| Position sizing | Fixed | Confidence multiplier | Confidence + ATR vol cap |
| Labeling | N/A | Triple-barrier | Triple-barrier R_net (net of costs) |
| Risk controls | Basic | ML-gated | Full (kill-switch, max positions, force exit) |
| Threshold optimization | N/A | Basic | Profit-based on OOF predictions |
