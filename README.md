# AVWAP Intraday Runner (Short + Long) — Beginner-Friendly Documentation

This repository contains an **Anchored VWAP (AVWAP)** backtesting / analysis setup with a **combined runner** that evaluates **SHORT**, **LONG**, and **COMBINED** results, plus a detailed PDF walkthrough meant for complete beginners.

If you're new to quant / backtesting, start with the PDF. It explains every concept and the end‑to‑end data flow with diagrams and flowcharts.

---

## What’s inside

### Core strategy files
- `avwap_common.py`  
  Shared helpers, constants, utilities used by both strategies.
- `avwap_short_strategy.py`  
  AVWAP **short** logic (signal rules, trade simulation, metrics).
- `avwap_long_strategy.py`  
  AVWAP **long** logic (signal rules, trade simulation, metrics).

### Orchestration / runner
- `avwap_combined_runner.py`  
  Runs short + long, merges results, produces summaries and outputs.
- `avwap_combined_runner_intraday_leverage.py` *(recommended for intraday)*  
  Same runner but with **intraday leverage** support (e.g., 5×) and **console log export** to `outputs/*.txt`.

### Data pipeline reference (optional / advanced)
- `algosm1_trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py`  
  Historical multi‑timeframe data builder (indicators, parquet/csv pipeline, etc.). Useful if you want to reproduce the data inputs.

### Documentation
- `AVWAP_v11_Code_Walkthrough.pdf`  
  A very detailed, beginner-friendly explanation with:
  - step-by-step program flow
  - diagrams & flowcharts
  - explanation of all major functions
  - how P&L, ROI, costs, and intraday leverage are computed

---

## Intraday leverage: how profit/loss is computed (important)

When you do intraday trading with leverage (example **5×**), your **capital** can stay ₹50,000 but your **exposure/notional** is ₹50,000 × 5 = ₹250,000.

The leverage-enabled runner keeps **both** metrics:

- **Price-return %** (unlevered): based on price movement and costs  
- **ROI% on capital** (levered): price-return % × leverage  
- **Rupee P&L** (levered rupees): computed on notional exposure

This gives you realistic intraday results while still reporting ROI against your actual capital/margin.

---

## Quick start

### 1) Install requirements
This project is plain Python. Create a virtual environment and install common dependencies:

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -U pip
pip install pandas numpy
```

> Note: Your environment may already include additional packages used by your broader ALGO-SM1 stack.

### 2) Run the leverage-enabled combined runner
```bash
python avwap_combined_runner_intraday_leverage.py
```

### 3) Outputs
The runner writes artifacts into an `outputs/` folder (created if missing), including:
- summaries (tables/CSVs depending on your runner settings)
- **a full console log** saved as: `outputs/avwap_combined_runner_YYYYMMDD_HHMMSS.txt`

---

## Recommended repo layout

A clean layout for GitHub:

```
.
├─ avwap_common.py
├─ avwap_short_strategy.py
├─ avwap_long_strategy.py
├─ avwap_combined_runner.py
├─ avwap_combined_runner_intraday_leverage.py
├─ algosm1_trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py
├─ outputs/
│  └─ (generated logs/results)
└─ docs/
   └─ AVWAP_v11_Code_Walkthrough.pdf
```

If you move the PDF into `docs/`, update the link below.

---

## Read the documentation

- **PDF Walkthrough:** `AVWAP_v11_Code_Walkthrough.pdf`

---

## Notes / disclaimers

- This code is for research/backtesting and educational use.
- Real trading involves slippage, brokerage, taxes, partial fills, and execution delays.
- Leverage magnifies both profits and losses—use risk controls.

---

## Contributing

If you want improvements (more plots, better logs, parameter sweep, walk‑forward validation), feel free to open an issue or PR.

---

## License

Add your preferred license (MIT / Apache-2.0 / proprietary) in a `LICENSE` file.
