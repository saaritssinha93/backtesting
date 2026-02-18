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
| `eqidv2_live_trading_signal_15m_v11_combined_parquet.py` | Live 15m combined AVWAP signal scanner |
| `eqidv2_live_combined_analyser.py` | Live combined signal evaluation |
| `eqidv2_live_combined_analyser_csv.py` | Same with CSV bridge for trade executors |
| `eqidv2_live_combined_analyser_parquet.py` | Same with parquet output |
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
| `avwap_trade_execution_PAPER_TRADE_TRUE.py` | Paper trade simulator |
| `avwap_trade_execution_PAPER_TRADE_FALSE.py` | Real-order executor (Zerodha/Kite) |
| `authentication.py` | Broker session bootstrap |

### Data Ingestion
| File | Role |
|------|------|
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` | Multi-TF historical parquet builder |
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py` | 15m-only variant |
| `eqidv2_eod_scheduler_for_15mins_data.py` | Periodic 15m update scheduler |
| `eqidv2_eod_scheduler_for_1540_update.py` | 15:40 IST EOD flush |

### Universe
| File | Role |
|------|------|
| `filtered_stocks.py` | Curated stock universe |
| `filtered_stocks_MIS.py` | MIS-focused universe |

---

## 5) Workflows

### A) Build ML Dataset
```bash
# 1. Generate candidate trades via backtest
cd eqidv2
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
python eqidv2_meta_train_walkforward.py \
  --dataset-csv meta_dataset.csv \
  --out-model models/meta_model.pkl \
  --out-features models/meta_features.json \
  --out-report outputs/meta_train_report.json \
  --train-days 60 --test-days 10
```

### C) Run ML-Filtered Backtest
```bash
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
python authentication.py              # first, authenticate
python avwap_trade_execution_PAPER_TRADE_TRUE.py   # paper
python avwap_trade_execution_PAPER_TRADE_FALSE.py  # real
```

---

## 6) Known Issues & Bugs

### CRITICAL

1. **`allow_signal_today()` wrong keyword argument** (`eqidv2_live_combined_analyser.py:942`, `eqidv2_live_combined_analyser_parquet.py:942`)
   - **Bug**: `allow_signal_today(state, ticker, sid=today_str)` — `sid` is not a valid parameter name; function expects `(state, ticker, side, today, cap_per_day)`.
   - **Impact**: `TypeError` at runtime — live analyser crashes.
   - **Fix**: Use positional args or correct keyword names.

2. **Duplicate `_generate_signal_id` definition** (`eqidv2_live_combined_analyser_csv.py:1114–1123`)
   - Two functions with the same name; the 3-arg version is shadowed by a 5-arg version.

### MODERATE

3. **`filtered_stocks_MIS.py` uses `set` instead of `list`** — non-deterministic ordering across runs.

4. **Hardcoded XPath selectors in `authentication.py`** — brittle against Zerodha UI changes.

---

## 7) Key Configuration

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
