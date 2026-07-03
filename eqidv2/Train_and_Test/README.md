# Train_and_Test — one pipeline: train/test → final_setup_conf → v11 backtest + v7 live

A single, consistent flow for tuning setups and shipping the result to **both** the
v11 full backtest and v7 live. One pool, one split, one config of record.

> **Layout note.** These entry scripts live in `Train_and_Test/`. The shared core
> (the v11 backtester, `candidate_scan`, `final_setup_conf.py`, the conf bootstrap,
> `eqidv2_conf_tier_c_live_scan`, `v6`, …) stays in the **repo root** because it is
> shared with the live stack and every backtester. Each script here adds the repo
> root to `sys.path` and imports the core **in place** — nothing is duplicated, and
> `--approve` edits the one root `final_setup_conf.py` (single source of truth).

```
 [1] build_unified_pool.py ─► outputs_ID_v11_unified_pool/
                              historical_all_available_pre_dedupe_live_candidates.csv
                              _manifest.json   (coverage, split, per-setup counts, basis)
                              option (i) basis per setup:
                                • native / non-conf  -> LIVE-GATED (post v8/research)
                                • readmit (10) + tier-c -> RAW (bypass), from cleanpool + tier-c CSVs
                                   │
 [2] run_train_test.bat <FAMILY> ─► setup_train_test.py --pool_dir <pool>
                              DYNAMIC window (train_test_window.py, rolls from today):
                              TEST = last 4 weeks,  TRAIN = the 3 months before TEST
                              writes proposals (per-setup train/test stats + chosen gate)
                                   │  (review, then) --approve
                                   ▼
 [3] <repo root>/final_setup_conf.py   ◄── the SINGLE config of record (16 active setups)
            │
            ├─► [4a] v11 FULL backtest:
            │      py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available \
            │             --selected_strategy_profile final_setup_conf --workers 8 \
            │             --start_date <D0> --end_date <D1> --out <out>
            │
            └─► [4b] v7 LIVE (paper or real):
                   set EQIDV2_USE_FINAL_SETUP_CONF=1   (bat/run_conf_paper_*.bat)
                   → eqidv2_final_conf_live_bootstrap installs the conf into the
                     scanner + 1-min entry engine (native through v8/research,
                     10 non-native readmitted, conf mask final — mirrors v11).
```

## Commands (run from the repo root)
```
# 1. (re)build the one pool  — fast merge of existing generated candidates
py -3.12 Train_and_Test\build_unified_pool.py
#    window is DYNAMIC by default (TEST=last 4w, TRAIN=3mo before); pin with --train-end/--test-start

# 2. tune a family on the unified pool (review only; no config change)
Train_and_Test\run_train_test.bat P
#    -> proposals under outputs_ID_v11_unified_pool\proposals\

# 3. accept (writes ONLY the OK setups of that family into the root final_setup_conf.py; backed up)
Train_and_Test\run_train_test.bat P --approve

# 4a. full backtest from the conf (net of cost)
py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available \
   --selected_strategy_profile final_setup_conf --workers 8 \
   --start_date 2025-10-01 --end_date <today> --out C:/TradingData/eqidv2/outputs_ID_v11_conf_full

# 4b. live (paper): launch the conf wrappers (sets EQIDV2_USE_FINAL_SETUP_CONF=1)
bat\run_conf_paper_signal_discovery.bat
bat\run_conf_paper_entry_engine.bat
bat\run_conf_paper_executor.bat
```

## Files in this folder
| File | Role |
|---|---|
| `train_test_window.py`  | dynamic TRAIN/TEST window from today (TEST=last 4w, TRAIN=3mo before); shared by the pool builder + tuner |
| `build_unified_pool.py` | merge all families into one (i)-correct pool + manifest |
| `setup_train_test.py`   | honest per-family train/test tuner; `--approve` writes root `final_setup_conf.py` |
| `run_train_test.bat`    | wrapper: tune a family against the unified pool |
| `train_test_conf.py`    | EVALUATE the 16-setup conf book on the dynamic window (existing gates, no re-search) → per-setup + book TRAIN vs TEST |
| `final_setup_conf.py`   | **mirror (read-only)** of the root config of record; for reference only — the tuner/backtest/live import the **root** file. Refresh: `cp ../final_setup_conf.py Train_and_Test/` |
| `validate_conf_tier_c_parity.py` | Tier-C live detectors vs current research sources (stale-source vs port-bug) |
| `diff_conf_entries_vs_v11.py`    | diff live entries vs the v11 same-day backtest |
| `README.md`             | this file |

## Notes / guarantees
- **Single source of truth:** only the root `final_setup_conf.py` decides the live/backtest book;
  written only via `setup_train_test.py --approve` (review-gated).
- **Same gating in backtest and live** (verified): conf mask bit-identical; pre-momentum
  features identical; native through v8/research, 10 non-native readmitted (option i).
- **Tier-C parity** validated 100% vs current research sources (`validate_conf_tier_c_parity.py`).
- **Dynamic window + freshness:** TEST is the most recent 4 weeks (rolls forward daily), so the pool
  must be **rebuilt with fresh candidates** or the latest TEST days will be empty (the manifest's
  `date_max` shows coverage; if it lags `today`, the tail of TEST has no rows). Pin a fixed window with
  `--train_start/--train_end/--test_start/--test_end` (tuner) or `--train-end/--test-start` (builder).
- **Honest promotion defaults:** the tuner now targets a modest, trade-rich TRAIN PF band
  (`1.40..1.70`) instead of chasing PF>2, requires `min_train_trades=50`,
  `min_test_trades=20`, `oos/is>=0.65`, `minhalf_pf>=1.10`, and bans the most common
  overfit vectors from per-setup search by default: `market_ret_pct`,
  `market_abs_ret_pct`, `signal_minute`, and `notional`.
- **Regenerate cadence:** rebuild the pool when new sessions are added (manifest records coverage
  + split). The native basis reuses `outputs_ID_v11_traintest_pool`; the readmit/tier-c basis reuses
  `outputs_ID_v11_cleanpool` + `outputs_ID_v11_conf_tier_c_current`.
- **Costs:** the tuner and the backtest both judge **net of cost** (v6 cost model).
```
