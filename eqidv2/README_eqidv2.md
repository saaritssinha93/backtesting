# EQIDV2 — Strategy v1: AVWAP Rejection + ML Filter + Sizing

This README documents the `eqidv2` folder after the **Strategy v1** upgrade — an end-to-end ML-filtered intraday trading pipeline for Indian equities (NSE cash market, 5-min/15-min timeframes).

---

## 1) Architecture Overview

The pipeline has five stages:

```
Data Ingestion -> Signal Generation -> ML Labeling & Training -> ML-Filtered Backtest -> Live Execution
```

1. **Data ingestion** — 5m/15m parquet builders + EOD schedulers
2. **Signal generation** — AVWAP long/short rejection scans (15m candles)
3. **ML labeling & training** — Triple-barrier R_net labels, 30-feature extraction, walk-forward LightGBM
4. **ML-filtered backtest** — p_win gating + confidence-based position sizing with ATR vol cap
5. **Live execution** — Paper and real-order executors with ML gate

---

## 2) Strategy v1 — What Changed

### Labeling (Section 5)
- **R_net based**: Label = 1 if `R_net >= +0.05R` (net-positive after costs), else 0
- `R_net = (PnL_net) / SL_distance` in R-multiple units
- Slippage (3 bps/side) + commission (2 bps/side) baked into label computation
- Default horizon: N=6 bars (30 min on 5-min TF)

### Features (Section 7)
Expanded from 5 legacy features to **30 features** across 6 groups:

| Group | Features | Description |
|-------|----------|-------------|
| A: Price & Volatility | `ret_1/2/3`, `atr_val`, `tr_val`, `atr_pctile_50`, `bb_width`, `bb_position`, `range_to_atr` | Log returns, ATR percentile, Bollinger metrics |
| B: Trend & Structure | `ema20_slope`, `ema50_slope`, `adx_val`, `di_plus/minus/diff`, `vwap_dist_atr`, `avwap_dist_atr` | EMA slopes, directional strength, VWAP distance |
| C: Volume & Liquidity | `vol_zscore_20`, `dollar_vol`, `illiquidity_proxy` | Volume z-score, dollar volume, Amihud proxy |
| D: Time & Context | `minute_bucket`, `bars_left_in_day`, `gap_pct` | Time-of-day encoding, session remaining |
| E: AVWAP Rejection | `touch_depth`, `rejection_body_ratio`, `upper_wick_ratio`, `consec_below_avwap`, `pullback_from_low20_atr` | Rejection quality metrics |
| F: Side | `side` | Encoded as +1 (LONG) / -1 (SHORT) |

### Model (Section 9)
- **LightGBM** (300 trees, depth 5, balanced) with LogisticRegression fallback
- Walk-forward validation with purging + embargo
- Optional isotonic/sigmoid calibration
- **Profit-based threshold optimization** on OOF predictions (maximizes mean R_net of taken trades)

### Sizing (Section 10)
- Confidence multiplier: `m = clip(0.7, 1.2, 0.7 + (p - T)/(T_upper - T))`
- ATR volatility cap: if `atr_pctile > 80th`, cap `m <= 1.0`
- Position size: `qty = (risk_pct * capital / stop_distance) * m`

### Risk Controls (Section 11)
Configurable via `MetaFilterConfig`:
- Max open positions: 3
- Max trades/day: 10, per ticker/day: 1
- Daily loss kill-switch: -1.0R or -0.8% capital
- No entry after 14:30 IST
- EOD force exit, min 4 bars left in day

---

## 3) File-by-File Map

### ML Pipeline (Strategy v1)

| File | Role |
|------|------|
| `ml_meta_filter.py` | Core ML infrastructure: 30-feature builder, `MetaLabelFilter` inference, confidence sizing, risk config |
| `eqidv2_meta_label_triple_barrier.py` | Build labeled dataset: R_net labels from candle data with slippage model |
| `eqidv2_meta_train_walkforward.py` | Walk-forward training: LightGBM/LogReg, calibration, threshold optimization |
| `eqidv2_ml_backtest.py` | Standalone ML filter over trade CSVs (v1 features, vol-capped sizing) |
| `avwap_ml_backtest_runner.py` | Full backtest runner: RAW + ML comparison with detailed stats and charts |

### Signal Generation

| File | Role |
|------|------|
| `avwap_live_signal_generator.py` | ML-gated live signal scanner (15m candles, skip logging) |
| `eqidv2_live_trading_signal_15m_v11_combined_parquet.py` | Live 15m combined AVWAP signal scanner |
| `eqidv2_live_combined_analyser.py` | Live combined signal evaluation |

### Strategy Engines

| File | Role |
|------|------|
| `avwap_combined_runner.py` | Monolithic AVWAP long+short backtest runner (legacy) |
| `avwap_v11_refactored/avwap_common.py` | Shared config, helpers, indicators |
| `avwap_v11_refactored/avwap_long_strategy.py` | Refactored LONG strategy scan + exits |
| `avwap_v11_refactored/avwap_short_strategy.py` | Refactored SHORT strategy scan + exits |
| `avwap_v11_refactored/avwap_combined_runner.py` | Refactored orchestrator |

### Trade Execution

| File | Role |
|------|------|
| `avwap_trade_execution_PAPER_TRADE_TRUE.py` | Paper trade simulator |
| `avwap_trade_execution_PAPER_TRADE_FALSE.py` | Real-order executor (Zerodha/Kite) |
| `authentication.py` | Broker session bootstrap |

### Data Ingestion

| File | Role |
|------|------|
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` | Multi-timeframe historical parquet updater |
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py` | 15-minute focused updater |
| `eqidv2_eod_15min_data_stocks.py` | EOD 15m updater |
| `eqidv2_eod_scheduler_for_15mins_data.py` | Periodic 15m update scheduler |
| `eqidv2_eod_scheduler_for_1540_update.py` | Fixed-time (15:40) EOD scheduler |

### Config / Artifacts

| File | Role |
|------|------|
| `filtered_stocks.py` | Stock universe list |
| `models/meta_model.pkl` | Trained meta model (LightGBM or LogReg) |
| `models/meta_features.json` | Feature list (must be versioned with model) |
| `datasets/meta_dataset.csv` | Labeled dataset from triple-barrier |

---

## 4) Typical Workflows

### A) Build ML Dataset (one-time or periodic)

```bash
# Generate candidate trades via backtest
python eqidv2/avwap_combined_runner.py

# Label trades with triple-barrier R_net method
python eqidv2/eqidv2_meta_label_triple_barrier.py \
  --trades-csv outputs/avwap_longshort_trades_ALL_DAYS.csv \
  --candles-dir stocks_indicators_5min_eq \
  --out-csv meta_dataset.csv \
  --horizon-bars 6 \
  --bar-minutes 5
```

### B) Train ML Model

```bash
python eqidv2/eqidv2_meta_train_walkforward.py \
  --dataset-csv meta_dataset.csv \
  --out-model models/meta_model.pkl \
  --out-features models/meta_features.json \
  --out-report outputs/meta_train_report.json \
  --train-days 60 \
  --test-days 10 \
  --calibration isotonic
# Outputs optimal_threshold in report JSON
```

### C) Run ML-Filtered Backtest

```bash
# Quick filter over trade CSV
python eqidv2/eqidv2_ml_backtest.py \
  --input-csv outputs/base_trades.csv \
  --output-csv outputs/ml_filtered_trades.csv \
  --threshold 0.62

# Full backtest with RAW vs ML comparison + charts
python eqidv2/avwap_ml_backtest_runner.py \
  --ml-threshold 0.62 \
  --model-path models/meta_model.pkl \
  --features-path models/meta_features.json
```

### D) Live Signal Generation

```bash
# ML-gated live signals (runs continuously)
python eqidv2/avwap_live_signal_generator.py \
  --ml-threshold 0.62 \
  --model-path models/meta_model.pkl \
  --features-path models/meta_features.json

# Single scan (for cron/scheduler)
python eqidv2/avwap_live_signal_generator.py --run-once
```

### E) Execute Trades

```bash
# Paper mode
python eqidv2/avwap_trade_execution_PAPER_TRADE_TRUE.py

# Real mode (requires auth)
python eqidv2/avwap_trade_execution_PAPER_TRADE_FALSE.py
```

---

## 5) Key Configuration

### MetaFilterConfig (ml_meta_filter.py)

```python
MetaFilterConfig(
    model_path="eqidv2/models/meta_model.pkl",
    feature_path="eqidv2/models/meta_features.json",
    pwin_threshold=0.62,
    risk_per_trade_pct=0.20,
    conf_mult_min=0.7,
    conf_mult_max=1.2,
    atr_pctile_vol_cap=80.0,
    max_open_positions=3,
    max_trades_per_day=10,
    max_trades_per_ticker_per_day=1,
    daily_loss_kill_R=-1.0,
    no_entry_after_ist="14:30",
    slippage_bps=3.0,
    commission_bps=2.0,
)
```

### Backward Compatibility
- All files auto-detect **v1 (30-feature)** or **legacy (5-feature)** mode
- Legacy models with 5 features continue to work without changes
- New datasets produced by `eqidv2_meta_label_triple_barrier.py` include all 30 features

---

## 6) Notes

- The `meta_model.pkl` + `meta_features.json` pair must be versioned together
- Retrain periodically as market regime shifts (recommended: monthly walk-forward)
- LightGBM is preferred but falls back to LogisticRegression if `lightgbm` is not installed
- Keep credentials isolated in `authentication.py` and environment variables
- Prefer the refactored AVWAP runner (`avwap_v11_refactored/`) over the legacy monolithic one
