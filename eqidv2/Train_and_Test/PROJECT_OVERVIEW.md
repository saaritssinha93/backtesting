# `Train_and_Test/` — Project Overview & Audit

> A complete, file-by-file audit of the `Train_and_Test/` folder inside the **eqidv2**
> 5-minute intraday trading repository. This document is the authoritative map of what
> the folder is, how each piece works, how data flows through it, and how to run it.

---

## Table of Contents

1. [Project-Level Picture](#1-project-level-picture)
   - 1.1 [Purpose & the problem it solves](#11-purpose--the-problem-it-solves)
   - 1.2 [High-level architecture](#12-high-level-architecture)
   - 1.3 [Tech stack & key dependencies](#13-tech-stack--key-dependencies)
   - 1.4 [Where this folder sits in the larger repo](#14-where-this-folder-sits-in-the-larger-repo)
   - 1.5 [Data flow / control flow](#15-data-flow--control-flow)
   - 1.6 [Entry points](#16-entry-points)
   - 1.7 [Setup & run instructions](#17-setup--run-instructions)
   - 1.8 [Key concepts & vocabulary](#18-key-concepts--vocabulary)
2. [Directory Structure & File Inventory](#2-directory-structure--file-inventory)
3. [Core Python Modules](#3-core-python-modules)
   - 3.1 [`train_test_window.py`](#31-train_test_windowpy)
   - 3.2 [`build_unified_pool.py`](#32-build_unified_poolpy)
   - 3.3 [`setup_train_test.py`](#33-setup_train_testpy)
   - 3.4 [`train_test_conf.py`](#34-train_test_confpy)
   - 3.5 [`aggressive_pf_tuner.py`](#35-aggressive_pf_tunerpy)
   - 3.6 [`walk_forward.py`](#36-walk_forwardpy)
   - 3.7 [`live_paper_holdout.py`](#37-live_paper_holdoutpy)
   - 3.8 [`validate_conf_tier_c_parity.py`](#38-validate_conf_tier_c_paritypy)
   - 3.9 [`diff_conf_entries_vs_v11.py`](#39-diff_conf_entries_vs_v11py)
   - 3.10 [`final_setup_conf.py` (mirror)](#310-final_setup_confpy-mirror)
4. [Batch / Wrapper Scripts](#4-batch--wrapper-scripts)
5. [Documentation Files](#5-documentation-files)
6. [Generated Artifacts (proposals, logs)](#6-generated-artifacts-proposals-logs)
7. [Cross-File Dependency Map](#7-cross-file-dependency-map)
8. [Gotchas, Assumptions & Lessons Learned](#8-gotchas-assumptions--lessons-learned)
9. [What Was Skipped & Why](#9-what-was-skipped--why)
10. [Changelog](#changelog)
11. [Execution-Realism & Methodology Deep-Dive](#11-execution-realism--methodology-deep-dive)

---

## 1. Project-Level Picture

### 1.1 Purpose & the problem it solves

`Train_and_Test/` is the **research-and-promotion harness** for the eqidv2 intraday strategy
book. The trading system fires intraday **setups** (named patterns like
`A_PULLBACK_C2_THEN_BREAK_C2_LOW`, `G_HIGHER_HIGH_BREAK`) on 5-minute bars of NSE (Indian)
equities, enters on the next 1-minute open, and exits on a stop-loss / target / end-of-day.

The problem this folder addresses: **how do you decide which setups, with which parameters
(filters, momentum gates, SL/target), are worth trading with real money — without fooling
yourself with overfit backtests?**

It solves this with **one disciplined train/test pipeline** that:

1. Merges every setup's candidate signals into **one pool** with the correct candidate
   "basis" per setup (so each setup is judged the way it actually runs in production).
2. Splits that pool into **TRAIN** (older) and **TEST** (most-recent, held-out) windows.
3. **Searches** each setup's tunable knobs on TRAIN to reach a profit-factor (PF) target,
   then **honestly judges** the result on the unseen TEST window with a strict anti-overfit
   acceptance gate.
4. Writes only the survivors into a **single config of record** (`final_setup_conf.py` in
   the repo root), which drives **both** the v11 full backtester **and** the v7 live stack.
5. Provides several **independent reality checks**: walk-forward multi-fold validation, the
   actual live paper-trade book, Tier-C live-detector parity, and a live-vs-backtest entry diff.

The folder is also a **research journal**: dated Markdown reports and run logs record what
was tried, what was promoted, what was rejected, and *why* — including the hard-won lesson
that **chasing PF > 2 produces overfit garbage** and the real objective is a robust,
trade-count-rich PF in the 1.4–1.7 band that survives out-of-sample.

### 1.2 High-level architecture

```
        ┌──────────────────────────────────────────────────────────────────┐
        │ REPO ROOT (shared/live core — imported in place, never moved)      │
        │  avwap_5min_ID_v11_backtesting.py   final_setup_conf.py (SoT)      │
        │  eqidv2_final_conf_live_bootstrap   eqidv2_conf_tier_c_live_scan   │
        │  nse_intraday_costs   walkforward_gate   v6 exit rules   candidate │
        │  scanners (v7), research_v11_tier123_new_setups, new_setups_scan…  │
        └───────────────▲───────────────────────────────────▲───────────────┘
                        │ sys.path.insert(root)              │ imports in place
        ┌───────────────┴───────────────────────────────────┴───────────────┐
        │ Train_and_Test/  (this folder — entry scripts + reports)           │
        │                                                                    │
        │  train_test_window.py ── dynamic TRAIN/TEST dates (shared)         │
        │  build_unified_pool.py ─ ONE pool + manifest (option-(i) basis)    │
        │  setup_train_test.py ─── per-family search; --approve writes SoT    │
        │  aggressive_pf_tuner.py  deep maxpf search (hypothesis generator)  │
        │  walk_forward.py ─────── multi-fold cross-validation               │
        │  train_test_conf.py ──── evaluate the existing book (no re-search) │
        │  live_paper_holdout.py ─ real live paper trades = truest OOS       │
        │  validate_conf_tier_c_parity.py / diff_conf_entries_vs_v11.py      │
        │  final_setup_conf.py ─── READ-ONLY MIRROR of the root SoT          │
        │  run_train_test.bat ──── family-tuning wrapper                     │
        │  + reports (*.md) + logs (*.log) + aggressive_pf_proposals/        │
        └────────────────────────────────────────────────────────────────────┘
                        │
                        ▼  reads/writes under
        C:\TradingData\eqidv2\  (DATA ROOT — pools, live signals, 1-min bars)
```

The **central design choice**: every script here lives in `Train_and_Test/` but imports the
**shared core in place** from the repo root by inserting the root into `sys.path`. Nothing is
duplicated, and `--approve` edits the **one** root `final_setup_conf.py`. This keeps a single
source of truth that simultaneously drives the v11 backtest and the v7 live stack.

### 1.3 Tech stack & key dependencies

| Layer | Detail |
|---|---|
| Language | Python 3.12 (`py -3.12` / `Python312\python.exe`) |
| Core libs | `pandas`, `numpy` |
| OS / shell | Windows 11; `.bat` wrappers; PowerShell/cmd |
| Data store | Local filesystem under `C:\TradingData\eqidv2\` (CSV + Parquet candidate pools, 1-min bars, live signal CSVs) |
| Shared repo modules (imported, not in this folder) | `avwap_5min_ID_v11_backtesting` (the v11 backtester + entry/exit/feature helpers), `eqidv2_final_conf_live_bootstrap` (conf→live installer, readmit provenance), `nse_intraday_costs` (cost model), `walkforward_gate` (`net_pnl_vectorized`, `_profit_factor`), `final_setup_conf` (config of record), `avwap_5min_ID_v7_candidate_scan`, `eqidv2_conf_tier_c_live_scan`, `research_v11_tier123_new_setups`, `new_setups_scan_v11`, `avwap_5min_ID_v2_backtesting` (raw setup detection definitions), `v6` exit rules |
| No package manifest | There is no `requirements.txt`/`pyproject.toml` in this folder; the environment is the repo's existing Python 3.12 install with pandas/numpy. |

### 1.4 Where this folder sits in the larger repo

The **README** states the layout rule explicitly: the **entry scripts** live in
`Train_and_Test/`; the **shared core** (v11 backtester, candidate scanners,
`final_setup_conf.py`, the live bootstrap, Tier-C live scan, `v6`) stays in the **repo root**
because it is shared with the live stack and every backtester. Each script adds the repo root
to `sys.path` and imports the core in place. `--approve` edits the one root
`final_setup_conf.py` (the single source of truth).

> **Critical distinction:** `Train_and_Test/final_setup_conf.py` is a **read-only mirror** for
> reference. The tuner, backtest, and live stack all import the **root** `../final_setup_conf.py`.
> Refresh the mirror with `cp ../final_setup_conf.py Train_and_Test/`.

### 1.5 Data flow / control flow

```
[1] build_unified_pool.py
      reads:  outputs_ID_v11_traintest_pool/ (live-gated candidates, post v8/research)
              outputs_ID_v11_cleanpool/      (raw pre-gate candidates)
              outputs_ID_v11_conf_tier_c_current/{tier123,new_setups}/*.csv
      writes: outputs_ID_v11_unified_pool/historical_all_available_pre_dedupe_live_candidates.csv
              outputs_ID_v11_unified_pool/_manifest.json
      basis (option i):  native/non-conf -> live-gated ; readmit(10)+tier-c -> raw
        │
        ▼
[2] setup_train_test.py --family X  (or run_train_test.bat X)
      load pool -> split TRAIN/TEST (train_test_window) ->
      attach 1-min entries -> per-setup search (mask/premom/exit/guard) ->
      family pipeline (guards -> premom -> dedupe -> mask -> resolve net of cost) ->
      ACCEPT/REJECT verdict -> proposal JSON + per-setup/weekly/monthly CSVs
        │  (review; if it passes) --approve
        ▼
[3] <repo root>/final_setup_conf.py   ← single config of record (SoT)
        │
        ├─► [4a] v11 FULL backtest  (avwap_5min_ID_v11_backtesting.py
        │         --selected_strategy_profile final_setup_conf --workers 8)
        │
        └─► [4b] v7 LIVE (paper/real)  (EQIDV2_USE_FINAL_SETUP_CONF=1 +
                  eqidv2_final_conf_live_bootstrap installs the conf into scanner + entry engine)

REALITY CHECKS (independent of the search):
   train_test_conf.py  -> evaluate the *existing* book gates on the dynamic window
   walk_forward.py     -> re-tune+test across many rolling folds (multi-fold robustness)
   aggressive_pf_tuner -> deep maxpf hypothesis generator (re-judge under robust gate!)
   live_paper_holdout  -> aggregate the ACTUAL live paper trades (truest OOS)
   validate_conf_tier_c_parity / diff_conf_entries_vs_v11 -> live↔backtest fidelity
```

### 1.6 Entry points

| Entry point | What it starts |
|---|---|
| `run_train_test.bat <FAMILY> [--approve]` | The standard per-family tune (wraps `setup_train_test.py`). |
| `py -3.12 setup_train_test.py --family A` | Tune one family directly. |
| `py -3.12 build_unified_pool.py` | (Re)build the unified candidate pool + manifest. |
| `py -3.12 train_test_conf.py` | Evaluate the current 16/12-setup book on the dynamic window. |
| `py -3.12 aggressive_pf_tuner.py` | Deep PF-maximizing search (review-only). |
| `py -3.12 walk_forward.py --setups ...` | Multi-fold walk-forward validation. |
| `py -3.12 live_paper_holdout.py` | Aggregate real live paper trades. |
| `py -3.12 validate_conf_tier_c_parity.py` | Tier-C live-detector parity check. |
| `py -3.12 diff_conf_entries_vs_v11.py --v11 ... --live ...` | Live-vs-backtest entry diff. |
| `train_test_window.py` (run directly) | Prints the current dynamic TRAIN/TEST window as JSON. |

All Python entry points use `if __name__ == "__main__": raise SystemExit(main())` (except the
two validators that use `sys.exit(main())`).

### 1.7 Setup & run instructions

**Prerequisites**

- Windows machine with **Python 3.12** at
  `C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe` (or `py -3.12`).
- `pandas`, `numpy` installed in that interpreter.
- The eqidv2 repo root present one directory up (so the shared core imports).
- The data root `C:\TradingData\eqidv2\` populated with the candidate pools and 1-min bars.

**Canonical workflow (run from the repo root):**

```bat
:: 1. (re)build the one pool — dynamic window by default (TEST=last 2w, TRAIN=3mo before)
py -3.12 Train_and_Test\build_unified_pool.py
::    pin a fixed window:
py -3.12 Train_and_Test\build_unified_pool.py --train-end 2026-05-31 --test-start 2026-06-01

:: 2. tune a family (review only; NO config change)
Train_and_Test\run_train_test.bat P

:: 3. accept (writes ONLY the OK setups of that family into root final_setup_conf.py; backed up)
Train_and_Test\run_train_test.bat P --approve

:: 4a. full backtest from the conf (net of cost)
py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available ^
   --selected_strategy_profile final_setup_conf --workers 8 ^
   --start_date 2025-10-01 --end_date <today> --out C:/TradingData/eqidv2/outputs_ID_v11_conf_full

:: 4b. live (paper): launch the conf wrappers (set EQIDV2_USE_FINAL_SETUP_CONF=1)
bat\run_conf_paper_signal_discovery.bat
bat\run_conf_paper_entry_engine.bat
bat\run_conf_paper_executor.bat
```

**Operational safety rules** (from `AGGRESSIVE_ITERATION_PROMPT.md` / `ANALYSIS_2026-06-22.md`):

- Run heavy jobs (scanner, v11 backtest, tuners) **after market close (> 15:30 IST)**, **≤ 8
  workers**, **one heavy job at a time** — heavy jobs can starve the live 5-minute feed and
  silently kill live signal generation.
- Verify the live feed heartbeat before/after. **Never** run the universe scanner or v11
  backtest during market hours.

**Environment variables**

| Var | Used by | Meaning |
|---|---|---|
| `EQIDV2_USE_FINAL_SETUP_CONF=1` | live wrappers | Make the v7 live stack install & trade the conf. |
| `EQIDV2_FINAL_CONF_TIER123_SOURCE_CSV` | `final_setup_conf.py`, `validate_conf_tier_c_parity.py` | Override the Tier123 source CSV path. |
| `EQIDV2_FINAL_CONF_NEW_SETUPS_SOURCE_CSV` | same | Override the new-setups source CSV path. |
| `EQIDV2_TIER_C_DATA_ROOT` | `validate_conf_tier_c_parity.py` | Override the 5-min live data root. |

### 1.8 Key concepts & vocabulary

| Term | Meaning |
|---|---|
| **Setup** | A named 5-minute pattern (e.g. `G_HIGHER_HIGH_BREAK`). The first letter is its **family** (A, B, C, D, E, G, L, P, V, S, T, MR…). Raw detection rules live in `avwap_5min_ID_v2_backtesting.py`. |
| **Candidate / pool** | A row = one setup firing on one ticker at one 5-min bar, with features. The pool is the universe the tuner searches over. |
| **Basis (option i)** | The candidate set a setup is scored on so it matches production: **native/non-conf → live-gated** (post v8/research filters); **readmit (10) + tier-c → raw** (they bypass v8/research live, so raw IS their faithful live basis). Scoring a *native* setup on raw numbers is a **pessimistic firehose**, not live-representative — confirm with the v11 conf backtest. |
| **PF (profit factor)** | Σ(wins) / Σ(losses), **net of NSE intraday cost**. |
| **TRAIN / TEST** | TRAIN = older in-sample window for searching; TEST = recent held-out out-of-sample window for judging. Dynamic default: TEST = last 2 weeks, TRAIN = the 3 months before. |
| **mask_terms** | AND-combined selected-strategy threshold filters on signal features (numeric `>=/<=/!=/==` or categorical string). |
| **pre_momentum_terms** | AND-combined 1-min pre-entry momentum gate (e.g. `pre2_mom_r>=0.55 & sig5_adx_calc>=26`). Often "the gate IS the edge". |
| **entry_guards** | Optional time-window / Top-N-per-slot guards. |
| **band vs maxpf objective** | `band` = reach `PF≥min` with the **most trades** (anti-overfit, default). `maxpf` = maximize robust PF subject to a trade floor (overfits by design; use only as a hypothesis generator). |
| **robust objective / min-half PF** | The **worse of the two TRAIN halves'** PF — a config must work in both halves, killing zero-loss in-sample pockets. |
| **day_block_p** | Bootstrap p-value over **daily** net sums (resampling whole days) — significance that the edge isn't one lucky day. Lower = better; gate wants `< 0.10`. |
| **oos/is ratio** | TEST PF ÷ TRAIN PF. A non-overfit edge keeps `≥ 0.55`. |
| **Accept gate** | TEST PF ≥ 1.30 AND TEST day_block_p < 0.10 AND oos/is ≥ 0.55 AND TEST n ≥ 8 AND TRAIN PF ≥ 1.50. |
| **`market_ret_pct` trap** | The dominant overfit vector — the search latches onto train-period market-return *bands* that don't exist in the test period. Banned in strict runs. |

---

## 2. Directory Structure & File Inventory

```
Train_and_Test/
├── README.md                          # the pipeline contract & commands
├── AGGRESSIVE_ITERATION_PROMPT.md     # reusable research prompt + guardrails
├── ANALYSIS_2026-06-22.md             # deep-dive: overfit + live-loss root cause
├── PROPOSED_FIX_2026-06-22.md         # the (applied) live-demotion fix
├── TRAINTEST_RESULTS_2026-06-22.md    # results of the aggressive PF run (A–G + strict reruns)
│
├── train_test_window.py               # dynamic TRAIN/TEST date computation (shared)
├── build_unified_pool.py              # build the ONE option-(i) pool + manifest
├── setup_train_test.py                # the core per-family tuner (--approve writes SoT)
├── train_test_conf.py                 # evaluate the existing conf book (no re-search)
├── aggressive_pf_tuner.py             # deep maxpf hypothesis generator
├── walk_forward.py                    # multi-fold walk-forward validation
├── live_paper_holdout.py              # aggregate real live paper trades
├── validate_conf_tier_c_parity.py     # Tier-C live-detector parity vs research sources
├── diff_conf_entries_vs_v11.py        # live entries vs v11 same-day backtest diff
├── final_setup_conf.py                # READ-ONLY MIRROR of the root config of record
├── run_train_test.bat                 # family-tuning wrapper
│
├── aggressive_pf_proposals/           # output of aggressive_pf_tuner.py
│   ├── <SETUP>.json   (16 per-setup proposals)
│   ├── _summary.json  (all setups, one array)
│   └── run.log        (the console transcript)
│
├── band_iteration.log                 # loose band run (TRAIN 03-01..05-31 / TEST 06-01..06-15)
├── band_iteration_strict.log          # strict band run (market_ret banned, tighter gate)
├── band_iteration_strict_v2.log       # strict + extended 3-week holdout (TEST..06-22)
├── conf_traintest_pinned.log          # train_test_conf.py transcript (pinned window)
├── walk_forward_now.log               # walk-forward OOM traceback (memory failure)
├── walk_forward_honest.log            # queued-waiting-for-close stub
└── __pycache__/                       # compiled bytecode (generated; ignored)
```

**File inventory (non-generated, by size):**

| File | Bytes | Type |
|---|---|---|
| `final_setup_conf.py` | 105,088 | Python (mirror config) |
| `setup_train_test.py` | 47,258 | Python (core tuner) |
| `walk_forward.py` | 11,695 | Python |
| `aggressive_pf_tuner.py` | 10,410 | Python |
| `build_unified_pool.py` | 9,045 | Python |
| `validate_conf_tier_c_parity.py` | 7,252 | Python |
| `train_test_conf.py` | 5,328 | Python |
| `diff_conf_entries_vs_v11.py` | 5,473 | Python |
| `live_paper_holdout.py` | 3,204 | Python |
| `train_test_window.py` | 1,729 | Python |
| `run_train_test.bat` | 1,162 | Batch |
| `README.md` / `*.md` | 5–7 KB each | Docs |

---

## 3. Core Python Modules

### 3.1 `train_test_window.py`

- **Path:** `Train_and_Test/train_test_window.py`
- **Purpose:** Compute the **dynamic** TRAIN/TEST date windows so the out-of-sample TEST is
  always the most recent slice and TRAIN is the period before it. Shared by the pool builder
  and the tuner so both agree on the split.
- **Structure:** one function `compute_windows(today=None, test_weeks=2, train_months=3) -> dict`.
- **How it works:**
  - `test_end = today`; `test_start = today − 2 weeks + 1 day` (a 14-day window ending today).
  - `train_end = test_start − 1 day` (adjacent, no overlap).
  - `train_start = train_end − 3 months + 1 day`.
  - Returns `{"train": (start,end), "test": (start,end), "today": ..., "policy": ...}` with
    dates formatted `YYYY-MM-DD`.
- **Inputs/outputs:** input optional `today` (defaults to `pd.Timestamp.today()`), `test_weeks`,
  `train_months`; output a dict of date-string tuples. Pure function, no side effects.
- **Dependencies:** `pandas` only. Imported by `setup_train_test.py` and `build_unified_pool.py`.
- **How to use:** `py -3.12 Train_and_Test\train_test_window.py` prints the current window as JSON.
- **Gotchas:** The window **rolls forward every day**. If the pool's `date_max` lags `today`,
  the tail of TEST has **no candidates** → fake/empty holdout. Always check the manifest
  `date_max`, or pin the window explicitly.

### 3.2 `build_unified_pool.py`

- **Path:** `Train_and_Test/build_unified_pool.py`
- **Purpose:** Produce the **single candidate pool** that `setup_train_test.py` reads,
  combining each setup's candidates on the **production-faithful basis (option i)**.
- **Structure:** helpers `_read_live_gated`, `_read_raw`, `_read_tier_c`; `main()`.
- **How it works:**
  1. Computes the dynamic window (for the manifest split) and parses CLI args
     (`--train-end`, `--test-start`, `--native-basis`, `--extra-raw`, `--tier123-csv`,
     `--newsetups-csv`).
  2. `readmit = boot.readmit_setups()` — the 10 non-native conf setups whose live basis is raw
     (pulled from the conf's provenance, so membership stays in sync with the conf).
  3. **Basis assembly:**
     - `--native-basis raw` (default): take **all** setups from the raw pool(s) **except**
       the setups the tier-c CSVs supply, tag `_basis="raw"`. (Used when the live-gated pool is
       stale; the conf gate is the binding filter and live adds v8/research on top = a
       conservative subset.)
     - `--native-basis live_gated` (option-(i) split): live-gated base **minus** readmit
       setups (`_basis="live_gated"`) **plus** raw candidates for the readmit setups
       (`_basis="raw_readmit"`).
     - Tier-c CSVs (tier123 + new_setups) are always appended (`_basis="tier_c"`).
  4. Union the frames on a shared column schema; parse `signal_time_ist` → IST-naive `_sig`;
     drop rows with no timestamp; **dedupe** on `["ticker","side","setup","signal_time_ist"]`.
  5. Write `historical_all_available_pre_dedupe_live_candidates.csv` and `_manifest.json`
     (built time, row counts, `date_min`/`date_max`, the train/test split counts, basis policy,
     readmit list, rows by basis, rows by setup, setup count).
- **Inputs:** the three source pools/CSVs under `C:\TradingData\eqidv2\`. **Outputs:** the
  unified pool CSV + manifest under `outputs_ID_v11_unified_pool/`.
- **Dependencies:** `pandas`, `glob`; `eqidv2_final_conf_live_bootstrap` (readmit), sibling
  `train_test_window`. Consumed by every tuner/validator that reads the unified pool.
- **How to use:** `py -3.12 build_unified_pool.py [--train-end D --test-start D] [--native-basis raw|live_gated]`.
- **Gotchas:** Raw layout may be **chunked** (`chunk_*/...raw_candidates.csv`) or flat — both
  globbed. The 2026-06-22 session ran it with the strict pinned window and reports
  255,590 rows / 30 setups (train 238,956 / test 16,634; later extended to test 17,969).

### 3.3 `setup_train_test.py`

- **Path:** `Train_and_Test/setup_train_test.py` — **the heart of the folder** (~930 lines).
- **Purpose:** Honest **per-family** train/test tuner. For one family at a time, with every
  other family blocked, it searches each setup's tunable knobs on TRAIN to hit a PF band, then
  judges the family on TEST with the strict accept gate. `--approve` writes the survivors into
  the root `final_setup_conf.py`.
- **Key module-level config (overridable via CLI):**
  - `TRAIN`, `TEST` = the dynamic window (from `compute_windows()`).
  - `POOL_DIR` = `outputs_ID_v11_traintest_pool` by default (override with `--pool_dir`).
  - PF band `TRAIN_PF_MIN=1.50`, `TRAIN_PF_MAX=2.00`.
  - Accept gate: `TEST_MIN_NET_PF=1.30`, `TEST_MAX_DAY_BLOCK_P=0.10`,
    `MIN_TEST_TRAIN_PF_RATIO=0.55`, `MIN_TRAIN_TRADES=15`, `MIN_TEST_TRADES=8`.
  - Search grids: `SL_GRID`, `TGT_GRID`, `QUANTILES`/`FINE_QUANTILES`, `MAX_MASK_TERMS=2`,
    `MAX_PREMOM_TERMS=1`.
  - `SIGNAL_FEATURES` (16 mask features incl. `market_ret_pct`) and `PREMOM_FEATURES` (8 wired).
  - `GUARD_GRID` — only `A_MOD_BREAK_C1_HIGH` has time/Top-N guard options.
  - `OBJECTIVE = "band"` (anti-overfit default).
- **Major functions (control flow):**
  1. **Pool load & prep** — `_family_setups()` derives a family's setups from v6 exit rules ∪
     the conf book ∪ whatever is in the pool (so newly-mined setups are tunable).
     `load_pool()` (via `_read_one_pool`) reads the unified/per-day CSVs **with
     `low_memory=False`** *(fixed 2026-06-23 — was silently inferring mixed/object dtypes and
     NaN-ing valid feature values in the mask filters; see [Changelog](#changelog))*, dedupes,
     normalizes ticker/side/setup, parses timestamps, adds `_day`/`_slot`, and derives
     selected-strategy features via `v11._selected_strategy_features`. `split_train_test()`
     splits by `_day`.
  2. **Entry / resolution / pre-momentum** (all `lru_cache`d for speed):
     - `_entry()` — next-1-min entry after the 5-min signal, paper slippage, qty from notional.
     - `_resolve_net()` — resolve SL/Tgt on 1-min OHLC to EOD, return **net** P&L via
       `walkforward_gate.net_pnl_vectorized` (v6 cost model).
     - `_premom()` — compute the v11 pre-entry momentum feature vector at entry.
     - `attach_entries()` — vectorized entry attach; drops rows with no fill; adds `notional`.
  3. **Parameterised pipeline stages** — `apply_guards`, `apply_mask_terms` (mirrors the
     canonical conf mask: numeric `>=/<=/!=/==` + categorical string), `apply_premom_terms`,
     `dedupe_family` (best score per (slot,ticker) then one ticker/day across the family),
     `resolve_book` (net per row by each setup's exit).
  4. **Metrics** — `_pf` (net PF), `_day_block_p` (10k-bootstrap daily-block p-value).
  5. **Per-setup search** — `search_setup()`:
     - splits TRAIN into two **halves** by calendar median day (for the robust objective);
     - over the guard × SL × TGT grid, selects mask terms then optional pre-momentum terms;
     - **`band`** objective uses `_greedy_to_band` (add the **fewest** terms to reach `PF≥min`
       while keeping the **most** trades; return `[]` if already in band); **`maxpf`** uses
       `_greedy_maxobj` (maximize the **worse-half** PF subject to the trade floor).
     - keeps the pre-momentum gate only if it improves the robust objective and keeps enough
       trades; picks the best qualifying config (band: most trades, tie-break toward band
       middle; maxpf: highest robust PF).
  6. **Family assembly & evaluation** — `eval_family()` runs the full pipeline (guards+premom →
     family dedupe → mask → resolve) and returns `{trades, net_pf, net_pnl, day_block_p, book, net}`.
     `book_detail()`/`summarize_detail()`/`weekly_summary()` produce per-trade, per-setup, and
     ISO-week breakdowns. `accept_verdict()` applies the gate and returns `(ok, reason_string)`.
  7. **`main()`** — parses ~25 CLI args (window pins, `--objective`, term limits,
     `--fine_quantiles`, strict gate overrides, `--exclude_features` to ban the regime-band
     vector), runs the search, prints detailed TRAIN/TEST tables + monthly/weekly nets, writes
     a `proposal_family_<X>.json` + CSVs, and on `--approve` (only if the family passes) calls
     `_write_final_conf()`.
  8. **`_write_final_conf()`** *(rewritten 2026-06-23 — see [Changelog](#changelog))* — merges
     this family's OK setups into the root conf **safely**: (a) writes a **timestamped backup**
     (`final_setup_conf.py.bak_<YYYYMMDD_HHMMSS>`) first; (b) **splices only the approved setups'
     blocks in place** inside the `FINAL_SETUP_CONF` literal via `ast` (replace existing keys,
     insert new ones), **preserving** `RESEARCH_WATCH_CONF`, the `_LIVE_DEMOTION` transform, the
     module docstring/imports, and every other setup's `detection`/`provenance` docs byte-for-byte
     — and preserving the *approved* setup's own non-tuned keys (`side`, `detection`, …) while
     overwriting only `exit`/`mask_terms`/`pre_momentum_terms`/`entry_guards` + merging provenance;
     (c) **verifies** the edited file compiles *and* executes (the demotion transform runs at
     import) before committing, and on **any** failure **restores the backup and aborts**. Helpers:
     `_serialize_setup_block()`, `_merged_setup_block()`, and a `_legacy_full_regenerate()` used
     only if the literal can't be located.
- **Inputs:** the unified pool, the shared v11 helpers, the cost config. **Outputs:** proposal
  JSON + CSVs under `<pool>/proposals/`, and (on `--approve`) the rewritten root conf.
- **Dependencies:** `avwap_5min_ID_v11_backtesting` (entry/exit/feature helpers + `er` resolver
  + `v6` + `EXCLUDED_SETUPS`/`ENTRY_SHADOW_SETUPS`), `walkforward_gate`, `nse_intraday_costs`,
  `final_setup_conf`, sibling `train_test_window`. **Imported by** `train_test_conf.py`,
  `aggressive_pf_tuner.py`, `walk_forward.py` (they reuse its primitives by setting module-level
  globals like `tt.OBJECTIVE`, `tt.POOL_DIR`, `tt.SIGNAL_FEATURES`).
- **How to use:** `py -3.12 setup_train_test.py --family A [--approve]`; many knobs documented
  in `--help` (e.g. `--objective band --train_pf_min 2.0 --max_mask_terms 3 --max_premom_terms 2
  --fine_quantiles --exclude_features market_ret_pct,market_abs_ret_pct`).
- **Gotchas:**
  - `--approve` is now an **in-place, backup-first, verified splice** (2026-06-23): it preserves
    `RESEARCH_WATCH_CONF`, the `_LIVE_DEMOTION_2026_06_22` transform, and all other setups' docs,
    and auto-restores on failure. ⚠️ One residual edge case: **re-approving a currently-demoted
    setup** (one named in `_LIVE_DEMOTION_*`) will re-write its block but the import-time transform
    will still move it to `RESEARCH_WATCH_CONF` — to genuinely re-promote, also remove its entry
    from the demotion block. (Previously `--approve` rewrote the whole file and silently dropped
    the research/demotion blocks — that hazard is fixed.)
  - **native* basis caveat:** evaluating a native setup on the raw pool is pessimistic — confirm
    with the v11 conf backtest before accepting/rejecting.
  - The cached entry/premom resolution holds 1-min bars in memory; running over many setups/folds
    can OOM (see `walk_forward_now.log`).

### 3.4 `train_test_conf.py`

- **Path:** `Train_and_Test/train_test_conf.py`
- **Purpose:** **Evaluate** the `final_setup_conf` book on the dynamic window using each setup's
  **existing** gate — **no re-search**. Confirms how the live book itself scores TRAIN vs TEST.
- **Structure:** `_conf_to_config()` (map a conf entry → the tuner's config dict), `_m()`
  (unpack metrics), `main()`.
- **How it works:** points `tt.POOL_DIRS` at the unified pool, optionally pins the window, builds
  per-setup configs from `fc.FINAL_SETUP_CONF`, loads + entry-attaches the pool, then for each
  setup runs `tt.eval_family({name: cfg}, tr/te)` in isolation and prints a `basis` (readmit vs
  native*) + side + TRAIN/TEST `n/PF/netRs` row; finally the **family-deduped BOOK** line plus
  the oos/is ratio and test day_block_p.
- **Inputs/outputs:** reads the unified pool + the conf; prints a table (no files written).
- **Dependencies:** `setup_train_test` (reused pipeline), `final_setup_conf`,
  `eqidv2_final_conf_live_bootstrap` (readmit provenance).
- **How to use:** `py -3.12 Train_and_Test\train_test_conf.py [--pool_dir <dir>] [--setups A,B]
  [--train_start/--train_end/--test_start/--test_end]`.
- **Gotchas:** native* rows are raw/ungated (pessimistic, not live-faithful — the printed footer
  says so). Tier-c setups with data ending 05-29 print a "no TEST candidates" note. The
  `conf_traintest_pinned.log` shows the post-demotion 12-setup book scoring BOOK PF 0.79
  TRAIN / 0.88 TEST on the pinned window (net loser on the raw/pessimistic basis).

### 3.5 `aggressive_pf_tuner.py`

- **Path:** `Train_and_Test/aggressive_pf_tuner.py`
- **Purpose:** A **deep, multi-restart** search that pushes TRAIN PF as high as *robustly*
  possible (`maxpf`) across the **full 40 pre-momentum feature universe** + the full mask
  universe, as a **hypothesis generator**. It honestly reports TEST and labels each result
  `ROBUST` / `OVERFIT` / `BELOW_TARGET` / `NO_CONFIG`. **No `--approve`** (review only).
- **Structure:** `PREMOM_ALL` (the 40 pre-entry features), `_mask_universe()` (signal features
  + any numeric pool column with >50% non-null and >5 distinct values), `_cfg_of()`, `main()`.
- **How it works:** sets aggressive globals on the shared tuner (`tt.OBJECTIVE="maxpf"`,
  `FORCE_PREMOM=True`, denser quantiles 5–95% step 5%, larger SL/TGT grids, configurable
  `--max-mask`/`--max-premom`/`--min-trades`). For each setup it does `--restarts` random
  feature-order restarts of `tt.search_setup` (greedy is order-sensitive, so restarts escape
  local optima), keeps the best TRAIN PF, evaluates it on TEST, and writes a per-setup JSON +
  `_summary.json` + `run.log`. Verdict: `ROBUST` needs `train_pf≥target AND test_pf≥1.30 AND
  oos/is≥0.55 AND test_n≥5`.
- **Inputs/outputs:** reads the unified pool + conf; writes `aggressive_pf_proposals/`.
- **Dependencies:** `setup_train_test`, `final_setup_conf`, `eqidv2_final_conf_live_bootstrap`.
- **How to use:** `py -3.12 Train_and_Test\aggressive_pf_tuner.py [--restarts 6 --target-pf 2.0
  --min-trades 20 --max-mask 4 --max-premom 3 --max-secs-per-setup 1200 --setups ...]` — **after
  market close**. The 2026-06-22 run used `--restarts 2 --max-mask 3 --max-premom 2` on a pinned
  window.
- **Gotchas:** **By design it overfits** — the documented 2026-06-22 result was **0/16 ROBUST**
  (every setup hit TRAIN PF = ∞ on a ~20-trade zero-loss pocket → TEST ≈ 0). Its verdicts must be
  **re-judged under the robust band gate** before trusting. Large setups are sampled to
  `--max-candidates` (default 8000) to bound the premom search.

### 3.6 `walk_forward.py`

- **Path:** `Train_and_Test/walk_forward.py`
- **Purpose:** **Multi-fold walk-forward** validation — the fix for the single-window overfit
  trap. Roll many anchored/expanding folds; require a setup's edge to hold in **most** folds.
- **Structure:** `lean_load_pool()` (chunked, memory-lean load of only the wanted setups),
  `make_folds()` (anchored expanding folds), `walk_setup()` (per-fold re-tune+test), `verdict()`,
  `main()`. `BAN_FEATURES = {market_ret_pct, market_abs_ret_pct}`.
- **How it works:** configures the shared search strict (`OBJECTIVE="band"`, regime-band features
  banned, fine quantiles). For each fold, TRAIN = all history up to the fold start, TEST = the
  next `--test_weeks`; it **re-runs the same band search on each fold's TRAIN** then scores it on
  that fold's unseen TEST. Aggregates over folds with test trades: `frac_test_pos` (fraction with
  test PF ≥ 1.3) and median test PF. **Verdict:** `ROBUST` if ≥`--min_folds` evaluated AND
  ≥60% test-positive AND median test PF ≥ 1.30; else `FRAGILE` / `DEAD` / `INSUFFICIENT_DATA`.
- **Inputs/outputs:** unified pool; writes `walk_forward_results/<setup>.json` + `_summary.json`.
- **Dependencies:** `setup_train_test`. **How to use:** `py -3.12 Train_and_Test/walk_forward.py
  --setups A_MOD_BREAK_C1_LOW,B_HUGE_RED_FAILED_BOUNCE,... [--test_weeks 4 --step_weeks 4
  --min_train_weeks 12 --min_folds 4]` — heavy, **after close**.
- **Gotchas:** `walk_forward_now.log` shows a real **`numpy._core._exceptions._ArrayMemoryError`
  OOM** while attaching entries (resolving 1-min bars across full history is memory-heavy) — the
  reason `lean_load_pool` exists, but per-setup entry attach can still blow up. `walk_forward_honest.log`
  is just a "queued, waiting for market close (≥15:32 IST)" stub — i.e. it had not run yet.

### 3.7 `live_paper_holdout.py`

- **Path:** `Train_and_Test/live_paper_holdout.py`
- **Purpose:** The **truest out-of-sample check** — aggregate the **actual live conf paper
  trades** per setup + total (net of cost). No backtest pool can fake it; it reflects exactly
  what the live book did (overlay leaks and all).
- **Structure:** `_pf()`, `load_live()`, `report()`, `main()`.
- **How it works:** globs `C:\TradingData\eqidv2\live_signals\paper_trades_<date>_id_5min_v7.csv`
  in the date window (default `2026-06-16 .. today`, the conf-live era), tolerates ragged rows,
  keeps resolved trades (`outcome` in TARGET/SL/EOD/TIME/TRAIL/STOP/EXIT or non-zero net), and
  prints overall `n/days/net/win%/PF` plus a per-setup table sorted by net.
- **Inputs/outputs:** reads the live paper-trade CSVs; prints a report (no files written).
- **Dependencies:** `pandas`, `numpy` only.
- **How to use:** `py -3.12 Train_and_Test\live_paper_holdout.py [--start 2026-06-16 --end <today>]`.
- **Gotchas:** This is what exposed the **backtest-vs-live disconnect**: setups with backtest
  test-PF 6.88 / 3.82 (`P_PDH_BREAK_RETEST_LONG`, `L_RS_LEADER_VWAP_HOLD`) **lost money live**
  and were demoted. Requires the live paper CSVs to exist for the window.

### 3.8 `validate_conf_tier_c_parity.py`

- **Path:** `Train_and_Test/validate_conf_tier_c_parity.py`
- **Purpose:** Validate the conf-mode **Tier-C live detectors** against the **current research
  sources**, separating "stale source CSV" from "live-port bug". The scanner-source CSVs are
  historical artifacts; if the 5-min data/feature prep changes, an old CSV row may no longer be
  emitted by the current detector.
- **Structure:** slot/key normalizers; `_sample()`; reference caches that memoize per-ticker
  research re-scans (`_tier123_reference_cache`, `_newsetups_reference_cache`); `main()`.
  Targets: 3 tier123 setups (`E_ORB_RETEST_HOLD_LONG`, `V_RECLAIM_PULLBACK_LONG`,
  `P_PDH_BREAK_RETEST_LONG`) + 1 new-setup (`L_RS_LEADER_VWAP_HOLD`).
- **How it works:** three checks per sampled CSV row — (1) **source-current**: is the row still
  emitted by the current research scanner at that ticker/setup/slot; (2) **live parity**: the
  causal live detector `tc._scan_conf_tier_c_ticker_slot` emits the same setup at the same slot
  (fast path: a live hit proves reproducibility; misses fall back to a full-ticker research
  rescan to tell stale-source from port-bug); (3) **causality**: the live detector never
  future-stamps a candidate. Prints per-setup `source_current / stale_source / live_hit /
  live_miss / current_recall%` and an overall `PASS`/`REVIEW` (needs 0 stale, recall ≥ threshold,
  0 causal violations).
- **Inputs/outputs:** reads the tier123/new-setups source CSVs + 5-min live data; prints + exits
  `0` (PASS) or `1` (REVIEW). **Dependencies:** `avwap_5min_ID_v7_candidate_scan`,
  `eqidv2_conf_tier_c_live_scan`, `new_setups_scan_v11`, `research_v11_tier123_new_setups`.
- **How to use:** `py -3.12 Train_and_Test\validate_conf_tier_c_parity.py [--sample-per-setup 60
  --min-current-recall-pct 95]`. Building the research market context is a one-time ~minute cost.
- **Gotchas:** Heavy research re-scans; uses `_eq_live2` data root by default. README notes
  Tier-C parity was validated 100% vs current sources.

### 3.9 `diff_conf_entries_vs_v11.py`

- **Path:** `Train_and_Test/diff_conf_entries_vs_v11.py`
- **Purpose:** Diff **which entries fired** live (conf path) vs the **v11 same-day backtest**
  (Phase-4 / §F). Checks WHICH entries fired, not at what price — fill price/timing diffs are
  expected and ignored.
- **Structure:** flexible column pickers (`TICKER_COLS`, `SIDE_COLS`, `SETUP_COLS`, `TS_COLS`),
  `_key()` (build the join key, floor the timestamp to 5-min), `_load_live_frames()` (glob,
  skip `entry_rows_raw_candidates_*` audit files + empties), `main()`.
- **How it works:** builds `(ticker, side, setup, 5-min bar)` keys for v11 and live, outer-merges
  with an indicator, and reports matched / v11-only (live missed) / live-only (extra) overall and
  per setup, plus entry recall (matched ÷ v11).
- **Inputs:** `--v11 <trades.csv>` and `--live "<glob>"`. **Outputs:** printed report.
  **Dependencies:** `pandas` only.
- **How to use:** `py -3.12 diff_conf_entries_vs_v11.py --v11 <v11_ID_trades.csv> --live
  "C:/TradingData/eqidv2/entry_engine_1min_v5_ID/audit/entry_rows_<YYYYMMDD>_*.csv"`. Use
  `entry_rows_<D8>_*.csv` (not `*<D8>*`, which also matches the raw-candidate audit).
- **Gotchas:** v11-only beyond known Tier-C recall (`V_RECLAIM ~75%`) or executor caps
  (20-position / dedup / F&O / freshness) warrants investigation.

### 3.10 `final_setup_conf.py` (mirror)

- **Path:** `Train_and_Test/final_setup_conf.py` — **a read-only MIRROR** of the repo-root
  config of record. The tuner/backtest/live import the **root** file; refresh with
  `cp ../final_setup_conf.py Train_and_Test/`.
- **Purpose:** The declarative **config of record** — the gate of record consumed by the v11
  backtester and v7 live. Self-describing: each setup carries its raw detection rules (reference
  only), tuned exit/mask/pre-momentum/guards, and full provenance/evidence.
- **Structure:**
  - Header constants: `COST_BASIS`, `TIER123_SCAN_SOURCE_CSV`, `NEW_SETUPS_SCAN_SOURCE_CSV`
    (env-overridable), and `ACCEPT_GATE` (the numeric accept thresholds).
  - **`FINAL_SETUP_CONF`** — the active book (16 setups as authored; **12 after** the demotion
    transform runs at import). Each entry: `side`, `detection` (reason_tag, scan window,
    conditions, common gate, feature defs), `exit {sl_pct, tgt_pct}`, `mask_terms`,
    `pre_momentum_terms` (+ `pre_momentum_missing_action`), `entry_guards`, `entry_model`,
    `exit_model`, and a rich `provenance` (train/test trades+PF+halves, day_block_p,
    exit/threshold robustness, monthly, diagnosis, gate_status, caveats).
  - **`RESEARCH_WATCH_CONF`** — `enabled: False`, **never traded**. Deeply-diagnosed setups with
    no validated edge, each with `best_found`/`evidence`, `why_rejected`, and a
    `revalidation_trigger`. Includes the VWAP-databug casualties (`T_TREND_DAY_EMA_STAIR_SHORT`,
    `S_UPTHRUST_TRAP_FADE`), churn/cost sinks (`E_ORB_BREAKOUT_*`), and assorted REJECTs.
  - **`_LIVE_DEMOTION_2026_06_22`** — a transform at module end that **`pop`s 4 live-losing
    setups out of `FINAL_SETUP_CONF` and into `RESEARCH_WATCH_CONF`** (`enabled=False`) with a
    `live_demotion` provenance note. Reversible by deleting the block.
- **The 16 authored active setups** (per the module docstring): `A_PULLBACK_C2_THEN_BREAK_C2_LOW`,
  `B_AVWAP_RECLAIM_REVERSAL`, `B_HUGE_C1_CLOSE_RECLAIM_BREAK`, `D_EMA20_REJECTION`,
  `E_VWAP_LOSE_EARLY_SHORT`, `G_HIGHER_HIGH_BREAK`, `L_DOUBLE_BOTTOM_VWAP`,
  `L_PRESSURE_BURST_VWAP`, `L_RS_LEADER_VWAP_HOLD`, `P_PDH_BREAK_RETEST_LONG`,
  `E_ORB_RETEST_HOLD_LONG`, `V_RECLAIM_PULLBACK_LONG`, `B_HUGE_RED_FAILED_BOUNCE`,
  `C_OR_BREAKDOWN`, `A_MOD_BREAK_C1_LOW`, `G_LOWER_LOW_BREAK`.
- **The 4 demoted** (→ research-watch on 2026-06-22): `P_PDH_BREAK_RETEST_LONG` (−14,497 / 40t /
  PF 0.25), `L_RS_LEADER_VWAP_HOLD` (−6,619 / 13t / PF 0.15), `V_RECLAIM_PULLBACK_LONG` (−1,937 /
  3t / PF 0.00), `E_ORB_RETEST_HOLD_LONG` (−1,442 / 5t / PF 0.01) → **active book 16 → 12**.
- **Example schema** (`G_HIGHER_HIGH_BREAK`): exit `0.90/2.50`, `mask_terms: []`,
  `pre_momentum_terms: [pre2_mom_r>=0.55, sig5_adx_calc>=26]`, `pre_momentum_missing_action:
  "block"`, a `dropped_production_gate` note, and provenance recording train PF 2.38 (halves
  2.47/2.30) / test PF 2.66 (n=8, day_block_p 0.005), exit/threshold robustness, monthly
  positivity, and `gate_status: STRONG_PROBATION`.
- **Dependencies:** `os` only (for env-var source-CSV overrides). Imported by the tuner,
  `train_test_conf`, `aggressive_pf_tuner`, the v11 backtester, and the live bootstrap (the
  **root** copy).
- **Gotchas:**
  - **This file is a mirror** — editing it does **not** affect the live/backtest book; edit the
    root copy (via `--approve` or a reviewed manual diff) and re-mirror.
  - `setup_train_test.py --approve` **regenerates** `FINAL_SETUP_CONF` as JSON and would drop
    both `RESEARCH_WATCH_CONF` and the `_LIVE_DEMOTION` transform — re-apply them after approve.
  - Many setups are flagged **sample-limited / probation** ("adopt, do not size up"), with
    explicit caveats: raw-pool gating coverage (L family), scanner-enriched feature wiring
    (`L_PRESSURE_BURST_VWAP`), thin test counts, and the corrected-VWAP rescue/rejection history.

---

## 4. Batch / Wrapper Scripts

### `run_train_test.bat`

- **Path:** `Train_and_Test/run_train_test.bat`
- **Purpose:** Convenience wrapper to tune one family against the unified pool.
- **How it works:** resolves its own folder (`%~dp0`), sets `POOL=...outputs_ID_v11_unified_pool`,
  prefers the explicit Python 3.12 exe (falls back to `py -3.12`), and runs
  `setup_train_test.py --family %1 --pool_dir "%POOL%" %2 %3 %4`. Usage:
  `run_train_test.bat <FAMILY> [--approve]`.
- **Gotchas:** Passes through up to three extra args (`%2 %3 %4`). Default pool is the **unified**
  pool (whereas `setup_train_test.py`'s built-in default is `outputs_ID_v11_traintest_pool`).

---

## 5. Documentation Files

| File | Role / key content |
|---|---|
| **`README.md`** | The pipeline contract: the one-pool→split→tune→conf→backtest+live flow, the layout rule (scripts here, shared core in root), the command cheat-sheet, the file table, and guarantees (single source of truth, same gating in backtest & live, Tier-C parity, dynamic-window freshness, ≤8-worker cost discipline). |
| **`AGGRESSIVE_ITERATION_PROMPT.md`** | A reusable, battle-tested research prompt. Encodes the **non-negotiable guardrails**: anti-overfit is the whole game (use `band`, not `maxpf`); mind the candidate basis; fix data before tuning; the live paper book is the truest holdout; live-feed safety (after close, ≤8 workers); segregation/safety of record. Plus a per-setup deep-analysis recipe and a report template (sections A–G). |
| **`ANALYSIS_2026-06-22.md`** | The deep-dive journal entry. TL;DR: PF>2 in training is fake (maxpf 0/16 survived); the live conf paper book lost **−Rs 29,053 (PF 0.25)** over 06-16..06-22; the **root cause is config overfit, not a wiring leak** (the 06-16 non-conf trades were a one-day bootstrap artifact). Tables of the aggressive search, the live holdout, the corrected root-cause, data gaps, and the action plan (P0 demote, P1 regen+band, P2 stop chasing PF>2). |
| **`PROPOSED_FIX_2026-06-22.md`** | The concrete, **applied** fix: demote the 4 overfit live-losers from `FINAL_SETUP_CONF` → `RESEARCH_WATCH_CONF` via the reversible `_LIVE_DEMOTION_2026_06_22` transform (book 16→12, backup `final_setup_conf.py.bak_20260622_demote`). Status, apply procedure, and reversal documented. Takes effect on next process start. |
| **`PROPOSED_ROOT_COST_ALIGNMENT.md`** | A reviewable, **NOT-applied** proposal (2026-06-23) to align the v11/v6 backtester's resolution onto the tuner+live cost/sizing basis (non-breaking, default-OFF `--cost_model`/`--slippage_bps`). Documents the flat-bps-vs-statutory, no-slippage-vs-15bps, and **Rs 50k-vs-100k notional** mismatches, the exact diff, validation, and rollback. Awaiting sign-off. |
| **`TRAINTEST_RESULTS_2026-06-22.md`** | Results of the aggressive iteration → robust-PF≥2 → honest-test run (TRAIN 03-01..05-31, TEST 06-01..06-15). **Verdict: PF≥2 is reachable in TRAIN almost everywhere but survives TEST for only the D family** (`D_AVWAP_LOSE_REVERSAL` short, train 2.32 → test 1.83/1.99) — and even that **collapses to test 1.14 once `market_ret_pct` is banned (strict)**. Four-way confirmation (maxpf 0/16; loose band 1/7; strict band 0/7; strict+extended 0/7) that **no robust PF≥2 exists on this data via threshold tuning**. |

---

## 6. Generated Artifacts (proposals, logs)

### `aggressive_pf_proposals/`

- **17 JSON proposals** (16 per-setup `<SETUP>.json` + `_summary.json`) + `run.log`, output by
  `aggressive_pf_tuner.py` on the 2026-06-22 maxpf run (window TRAIN 03-01..05-31 / TEST
  06-01..06-15, `--restarts 2 --max-mask 3 --max-premom 2`).
- **Schema** per proposal: `setup`, `basis` (readmit/native*), `verdict`, `sampled`, `train_pf`,
  `train_n`, `train_net`, `test_pf`, `test_n`, `test_net`, `oos_is_ratio`, `sl`, `tgt`, `guard`,
  `mask_terms`, `premom_terms`, `secs`. (JSON uses literal `Infinity` — non-standard JSON.)
- **Result:** **0/16 ROBUST — every setup OVERFIT.** Typical pattern: `train_pf = Infinity`
  (≈20-trade zero-loss in-sample pocket) → `test_pf` 0.0–6.84 with tiny test n. The gates the
  search latched onto are curve-fits (e.g. `market_ret_pct` bands, `vwap_dist_atr` double-terms),
  exactly the overfit vectors the docs warn about. `run.log` is the console transcript of the run.

### Run logs

| Log | What it captures |
|---|---|
| `band_iteration.log` | The **loose** band run per family (A–L) on the pinned window. Shows the per-setup chosen gate, TRAIN/TEST per-setup tables, monthly + weekly nets. Illustrates the `market_ret_pct`-band overfit (e.g. A_MOD_BREAK_C1_LOW gate `market_ret_pct∈[-1.25,-1.11]` → TRAIN 2.02, TEST collapse). |
| `band_iteration_strict.log` | The **strict** rerun: `market_ret_pct`/`market_abs_ret_pct` **banned**, `mask≤2 premom≤1`, floor 25, tighter accept gate (`test≥1.5 p≤0.05 ratio≥0.65 n≥10`). 0/7 ACCEPT. |
| `band_iteration_strict_v2.log` | Strict + **extended 3-week holdout** (TEST 06-01..06-22 on regenerated data). Still 0/7. |
| `conf_traintest_pinned.log` | `train_test_conf.py` transcript of the **post-demotion 12-setup book** on the pinned window: BOOK 0.79 TRAIN / 0.88 TEST PF (raw/pessimistic basis), oos/is 1.11, test day_block_p 0.798. |
| `walk_forward_now.log` | A walk-forward attempt that **OOM-crashed** (`_ArrayMemoryError`) while attaching 1-min entries across full history — documents the memory limit. |
| `walk_forward_honest.log` | A one-line "queued, waiting for market close" stub (the run was deferred). |

### `__pycache__/`

Compiled `.pyc` bytecode for the modules — **generated, safe to ignore/delete.**

---

## 7. Cross-File Dependency Map

```
train_test_window.py ──────────┐ (compute_windows)
                               ├──► setup_train_test.py ◄── final_setup_conf.py (root, via _family_setups/_write_final_conf)
build_unified_pool.py ─────────┘        │  imports: avwap_5min_ID_v11_backtesting (v11, er, v6),
   imports: eqidv2_final_conf_live_bootstrap (readmit),    walkforward_gate, nse_intraday_costs
            train_test_window                              │
                                                           │ reused as `tt` by:
                              ┌────────────────────────────┼───────────────────────────────┐
                  train_test_conf.py        aggressive_pf_tuner.py            walk_forward.py
                  (+final_setup_conf,        (+final_setup_conf,              (sets tt globals;
                   +boot readmit)             +boot readmit)                   lean_load_pool)

live_paper_holdout.py        ── pandas/numpy only (reads live_signals CSVs)
validate_conf_tier_c_parity.py ── avwap_5min_ID_v7_candidate_scan, eqidv2_conf_tier_c_live_scan,
                                   new_setups_scan_v11, research_v11_tier123_new_setups
diff_conf_entries_vs_v11.py  ── pandas only (CSV diff)
run_train_test.bat           ── invokes setup_train_test.py
```

**Shared-core modules imported from the repo root (not in this folder):**

| Module | Provides |
|---|---|
| `avwap_5min_ID_v11_backtesting` (`v11`) | Entry/exit/feature helpers (`_load_1m_with_open`, `_first_1m_entry`, `_normalise_ts`, `_fmt_ist`, `_selected_strategy_features`, `_pre_entry_momentum_features_v11`), the `er` resolver, `v6.SETUP_EXIT_RULES`, `EXCLUDED_SETUPS`/`ENTRY_SHADOW_SETUPS`, `V7_*` constants. |
| `eqidv2_final_conf_live_bootstrap` (`boot`) | `readmit_setups()` (the 10 non-native conf setups = pool basis provenance); the live installer that mirrors the conf into the scanner + entry engine. |
| `nse_intraday_costs` | `CostConfig` (the v6 statutory NSE intraday cost model). |
| `walkforward_gate` (`wfg`) | `net_pnl_vectorized`, `_profit_factor`. |
| `final_setup_conf` | The config of record (root copy). |
| `avwap_5min_ID_v7_candidate_scan`, `eqidv2_conf_tier_c_live_scan`, `research_v11_tier123_new_setups`, `new_setups_scan_v11`, `avwap_5min_ID_v2_backtesting` | Tier-C live detectors + research scanners + raw setup-detection definitions. |

---

## 8. Gotchas, Assumptions & Lessons Learned

1. **The mirror trap.** `Train_and_Test/final_setup_conf.py` is **read-only reference**. Edits do
   nothing live; edit the **root** copy and re-mirror. The README and module docstring both warn.
2. **`--approve` clobbering hand-authored blocks — FIXED 2026-06-23.** It now backs up first,
   splices only the approved setups in place (preserving `RESEARCH_WATCH_CONF` + the
   `_LIVE_DEMOTION` transform + all docs), and restores on failure. Residual edge case:
   re-approving a *currently-demoted* setup still needs the demotion-block entry removed by hand
   to truly re-promote. (See [Changelog](#changelog).)
3. **Native* basis silently inverts conclusions.** A native setup scored on the raw pool is a
   pessimistic firehose, not its live basis. Never accept/reject a native setup on raw numbers —
   confirm with the v11 conf backtest. Readmit setups *are* faithful on raw.
4. **Dynamic window + stale data = fake holdout.** If the pool `date_max` lags `today`, TEST's
   tail is empty. Check the manifest; pin the window when data is stale (tier-c CSVs ended 05-29,
   raw cleanpool 06-10, the live-gated pool frozen ~Jan-28).
5. **Chasing PF>2 produces overfit garbage.** Quadruple-confirmed on this data (maxpf 0/16; loose
   band 1/7; strict band 0/7; strict+extended 0/7). The honest objective is `band` (PF 1.4–1.7 at
   max trades) with a significant `day_block_p` and `oos/is ≥ 0.55`. The `maxpf` tooling is a
   hypothesis generator only.
6. **`market_ret_pct` is the dominant overfit vector.** The search latches onto train-period
   market-return bands that don't exist in test. Ban it (`--exclude_features`) in strict runs;
   `walk_forward.py` bans it by default.
7. **The live paper book is the truest OOS.** Backtest test-PF 6.88/3.82 setups lost money live
   (`P_PDH`, `L_RS_LEADER`) → demoted. Always cross-check with `live_paper_holdout.py`.
8. **Live-feed starvation is real and dangerous.** Heavy jobs starved the 5-min feed and crashed
   the live stack on 2026-06-22. Run heavy jobs after close, ≤8 workers, one at a time; verify the
   feed heartbeat.
9. **Memory:** the cached 1-min entry/premom resolution is memory-heavy; `walk_forward.py` OOM'd
   even with `lean_load_pool`. Restrict setups / windows for heavy validation.
10. **Death-by-cost scalps.** The prime live failure mode was tiny-n overfit gates + ultra-tight
    scalp exits (`P_PDH` 0.5/0.6 SL/Tgt at ~13 trades/day, PF 0.25). Prefer wider R:R + trade
    floors.
11. **Non-standard JSON:** the aggressive proposals use literal `Infinity`/`NaN` — they parse with
    Python's `json` (which allows them) but not strict JSON parsers.
12. **Sample-limited book.** Most active setups are explicitly "probation — adopt, do not size up";
    several carry raw-pool/scanner-feature wiring caveats recorded in their provenance.

---

## 9. What Was Skipped & Why

| Skipped | Why |
|---|---|
| `__pycache__/*.pyc` (14 files) | Compiled bytecode, regenerated automatically — no source value. |
| Full byte-by-byte read of the three large `band_iteration*.log` files (33–37 KB) | Their structure (per-family chosen gate + TRAIN/TEST tables + monthly/weekly nets) was read and characterized from representative sections; the conclusions are fully captured in `TRAINTEST_RESULTS_2026-06-22.md` and §6. No unique facts beyond those summaries. |
| The 14 individual small `aggressive_pf_proposals/<SETUP>.json` (other than one sampled) | They are identical in schema to `_summary.json`, which contains all 16 records in full — already read and tabulated in §6. |
| The middle ~600 lines of `final_setup_conf.py` (per-setup detection/provenance bodies) | The structure, schema, the full active/research/demoted setup lists, and a representative full setup block were read; the remaining entries follow the identical documented schema. Every active setup name, side, and the demotion set are captured. |
| Repo-root shared-core modules (`avwap_5min_ID_v11_backtesting.py`, the live bootstrap, scanners, etc.) | They live **outside** `Train_and_Test/` (the audit scope) and are large; this document maps **what** each provides and **how** this folder uses it (see §7), which is sufficient for understanding the folder. |

---

---

## Changelog

Code changes applied to this folder, newest first. This document is kept in sync with them.

### 2026-06-23 (batch 5) — root cost-alignment APPLIED + book-level regime switch

- **G4 root edit APPLIED (after sign-off, default-OFF).** Added `--cost_model {flat_bps,statutory}`
  + `--slippage_bps` to `avwap_5min_ID_v11_backtesting.py` via module globals
  `_V11_COST_MODEL`/`_V11_SLIPPAGE_BPS` set only by `main()`. `statutory` resolves P&L like the
  tuner+live (per-trade NSE costs on the **Rs 100k live notional** + per-leg spread), fixing both the
  cost-formula and the **2× notional** mismatch (v11 had been on Rs 50k `v6.EFFECTIVE_NOTIONAL`).
  **Validated:** parses; on import the defaults are `flat_bps`/0 → **the live stack (which imports
  the module) is byte-identical and unaffected**; statutory math correct; CLI registers. **Pending:**
  the post-close full-backtest reconciliation run. See [`PROPOSED_ROOT_COST_ALIGNMENT.md`](PROPOSED_ROOT_COST_ALIGNMENT.md).
- **§11.4 book-level regime switch (opt-in, default off).** New `_apply_regime_align` + `--regime_align`
  / `--regime_band` in the tuner: a single "don't fight the tape" filter (drop LONGs in a down market /
  SHORTs in an up market, by causal `market_ret_pct`), expressing regime **once** for the whole book
  instead of letting each setup curve-fit a narrow `market_ret_pct` band (the dominant overfit vector,
  which the per-setup search still bans). Verified: strict sign alignment + the neutral-band widening.

(batch 4's "not applied" proposal is now applied — see above.)

### 2026-06-23 (batch 3) — cost-model unification (G4) + portfolio overlay (G5) (`setup_train_test.py`)

- **G4 — cost-model selector + reconciliation.** Refactored the resolver into a
  cost-model-agnostic, cached `_resolve_full` (returns exit time / outcome / slipped exit
  price) + a `_trade_net(model=…)` that nets under either **`statutory`** (default, per-trade
  NSE) or **`flat_bps`** (v6/v11's flat `cost_bps` on the fixed Rs 50k notional, +3 bps on SL).
  New CLI `--cost_model`, `--cost_bps`. `--cost_model flat_bps --slippage_bps 0` makes the tuner
  judge with ~the same cost function as the v11 backtest. Every run prints a **cost
  reconciliation** (TEST book net under both models). Verified: `flat_bps` TARGET=420, SL=405
  (exact); statutory=446.5 on the same trade — confirming v6's 16 bps > pure statutory (~10.7 bps)
  but < the tuner's 15 bps/leg spread. *Open (needs sign-off):* aligning the **v11/v6 root** cost
  path to statutory+spread so tuner/v11/live share one model.
- **G5 — portfolio overlay (position cap + daily-loss stop).** `eval_family` now runs
  `_apply_portfolio_overlay`: an entry-time-ordered event simulation enforcing a concurrent-position
  cap (`--max_positions`, default 20 = live executor) and a daily-loss kill-switch (`--daily_loss_rs`,
  default off; blocks new entries once a day's realized net ≤ −limit, open positions run to exit).
  Makes the simulated book the deployable (first-come, capped) book. Verified on synthetic
  overlap (cap drops the 3rd of 3 overlapping at cap=2) and kill-switch (blocks same-day post-loss
  entry, resumes next day) cases. ⚠️ The default cap of 20 is inherited by `train_test_conf.py` /
  `walk_forward.py` / `aggressive_pf_tuner.py` (they import `tt`); it rarely binds for a single
  family but can trim the all-setup BOOK line in `train_test_conf` — intended realism. Set
  `--max_positions 0` to disable.

### 2026-06-23 (batch 2) — execution-realism, FDR gate, hygiene (`setup_train_test.py`, `aggressive_pf_tuner.py`)

Implements the deep-audit items [§11](#11-execution-realism--methodology-deep-dive) G1, G2, G3 and
L1–L4. **All validated** (unit + synthetic tests under Python 3.12; the heavy full-tuner run is left
for after market close per the live-feed-safety rule).

- **G2 — bid-ask spread / exit slippage (default ON, 15 bps/leg).** New `SLIPPAGE_BPS = 15.0`
  applies adverse per-leg slippage to **both** the entry (`_entry`) and the **exit**
  (`_exit_with_slippage`, used in `_resolve_net` and `book_detail`) fills — the leg the resolver +
  statutory cost model previously ignored. CLI `--slippage_bps`. ⚠️ This **re-rates every setup more
  conservatively** (the tight 0.5–0.6 % scalps become losers — the intended correction); the tuner
  is now stricter than the v11 backtester (which still uses its flat `cost_bps`) → see open item G4.
- **G1 — entry latency knob.** New `ENTRY_LATENCY_MIN` (default 0 = parity) pushes the intended
  entry later in `_entry` to model the live feed+engine lag. CLI `--entry_latency_min` for
  stress (+1/+2 min). Verified: `_exit_with_slippage('LONG',100)=99.85`, `('SHORT',100)=100.15`.
- **G3 — Benjamini-Hochberg FDR multiple-testing gate.** After the per-setup search, each OK
  setup's TEST trades get a bootstrap p(mean net > 0) (`walkforward_gate._bootstrap_p_gt_zero`),
  then BH-FDR (`walkforward_gate._benjamini_hochberg`) is applied across the family; FDR-insignificant
  setups are dropped (`status="DROP_FDR"`) from the approved book unless `--no_fdr`. CLI `--fdr_alpha`
  (default 0.10). Verified: significant winner kept, noisy loser dropped.
- **L1 — `apply_premom_terms` operator coverage.** Now handles `>= <= > < == !=` and **fails closed**
  on an unknown op (was silently treating everything non-`>=` as `<=`).
- **L2 — `dedupe_family` tie-break.** Score ties (e.g. tier-c rows with no `quality_score`) now break
  by **earliest signal time** (first-come, matching the live executor), then setup name — removing the
  old alphabetical-by-setup bias. Verified on synthetic ties.
- **L3 — balanced train half-split.** `search_setup` now splits the two robustness halves at the
  whole-day point whose cumulative **trade** count is closest to 50/50 (was the median **day**, which
  skewed when trades/day varied), both halves non-empty by construction. Verified across
  heavy-first / heavy-last / even / single-day cases.
- **L4 — strict JSON.** New `_json_sanitize()` maps non-finite floats → `null`; applied to the tuner's
  proposal JSON and to all `aggressive_pf_tuner.py` proposal writes (no more `Infinity`/`NaN` literals).

*(G4 and G5 referenced here as "next" were completed in batch 3, above.) Still open: the v11/v6
root-side cost alignment (needs sign-off), L5 (multi-seed bootstrap), and the strategy-structure
items in §11.4.*

### 2026-06-23 (batch 1) — safer `--approve` + pool-read dtype fix (`setup_train_test.py`)

Two fixes from the [§8 audit](#8-gotchas-assumptions--lessons-learned), both in
[`setup_train_test.py`](setup_train_test.py):

1. **`--approve` no longer clobbers the config of record (critical).** `_write_final_conf()` was
   rewriting the **entire** root `final_setup_conf.py` as generated JSON with **no backup** — one
   `--approve` permanently destroyed `RESEARCH_WATCH_CONF`, the `_LIVE_DEMOTION_2026_06_22`
   transform, the env-var source constants, and every setup's `detection`/`provenance` docs.
   It now:
   - writes a **timestamped backup** (`final_setup_conf.py.bak_<YYYYMMDD_HHMMSS>`) **before** any
     write (the README's "(backed up)" claim is now actually true);
   - **splices only the approved setups' blocks in place** inside the `FINAL_SETUP_CONF` literal
     using `ast` (replace existing keys, insert new ones), preserving everything else
     byte-for-byte and preserving the approved setup's own `side`/`detection`/non-tuned keys
     (overwriting only `exit`/`mask_terms`/`pre_momentum_terms`/`entry_guards` + merging provenance);
   - **verifies the result compiles *and* executes** (the demotion transform runs at import)
     before committing, and **restores the backup + aborts** on any failure;
   - falls back to a `_legacy_full_regenerate()` only if the literal can't be located (still
     backed up). New helpers: `_serialize_setup_block()`, `_merged_setup_block()`.
   - **Validated** against a copy of the real 12-active / 18-research config: replace preserves
     `detection`/`side`/old provenance, insert works, `RESEARCH_WATCH_CONF` and the demotion
     transform survive and still function, and the compile+exec gate catches bad edits.
   - ⚠️ Residual edge case: re-approving a *currently-demoted* setup also needs its entry removed
     from `_LIVE_DEMOTION_*` to truly re-promote (the transform still moves it otherwise).

2. **Pool reads now use `low_memory=False` (`_read_one_pool`).** The wide, sparsely-populated
   feature columns were inferring as mixed/object dtype (the `DtypeWarning` in every run log),
   which made `pd.to_numeric(..., errors="coerce")` in the mask filters silently NaN-out valid
   values and drop rows. Now consistent with `build_unified_pool.py` and `walk_forward.lean_load_pool`.

*Not yet addressed (from the §8 analysis): `apply_premom_terms` op coverage, the walk-forward
`lru_cache` OOM, `dedupe_family` missing-score tie-break, non-standard `Infinity` JSON in
proposals, the slippage-stress / live-fill calibration, longer default TEST window, and the
fixed-config walk-forward mode.*

---

## 11. Execution-Realism & Methodology Deep-Dive

> A deeper audit (2026-06-23) that traced the actual P&L-determining logic in the **shared
> core** the harness calls — entry timing, exit resolution, costs, the gate features, and the
> promotion statistics — to explain the backtest-PF-2 → live-PF-0.25 cliff. **Headline: the core
> is sound on *causality* (no look-ahead), so the cliff is driven by *methodology* and
> *execution realism*, not a lookahead bug.** Findings are ordered by performance impact.

### 11.1 What is already correct (verified — do not "fix" these)

| Component | File / location | Verdict |
|---|---|---|
| **Intrabar exit resolution** | `v17D_exit_resolver.resolve()` | **Conservative & correct.** When SL and target both fall inside one 1-min bar it resolves as **SL** (pessimistic), not target. EOD exits at the last bar's close. This rules out the classic backtest-inflation bug. |
| **Pre-entry momentum gate** | `avwap_5min_ID_v11_backtesting._pre_entry_momentum_features_v11()` | **Causal.** 1-min features slice `index < entry` (`cutoff = entry_ts.floor("min")`); 5-min signal-bar features take the last bar `<= signal_ts`. No future data. |
| **Selected-strategy mask features** | `_selected_strategy_features()` | Derived from the signal bar; mirrored bit-identically in the live bootstrap (per project memory). |
| **Statutory cost model** | `nse_intraday_costs.intraday_equity_costs()` + `walkforward_gate.net_pnl_vectorized()` | Full NSE intraday stack (brokerage cap, STT sell-side, exch/SEBI/IPFT, stamp buy-side, GST), with the long/short asymmetry. The vectorized path is asserted equal to the scalar reference. |

### 11.2 The high-impact gaps (ordered by likely P&L effect)

> **Status (2026-06-23):** **G1 ✅, G2 ✅, G3 ✅, G4 ✅, G5 ✅ implemented** (see [Changelog](#changelog)).

**G1 — Entry latency is not modeled (largest realism gap for scalps). ✅ IMPLEMENTED** — `ENTRY_LATENCY_MIN` / `--entry_latency_min`.
`_first_1m_entry` enters at **`signal_ts + 1 min` open**. But in live, the 5-min bar isn't available until ~45–60 s after its close (the feed write race in project memory), plus the entry engine's lag — so live entries land **1–2 minutes later** than the backtest. For the tight-target scalps (e.g. `P_PDH` 0.5/0.6) the move is often gone or reversed by then. This is a prime suspect for the live underperformance.
*Proposed change:* add a configurable entry-latency (`enter at signal + N minutes open`, default model the real feed+lag) and a **latency-stress mode** (+1/+2 min) in the tuner; reject any setup whose edge dies at +2 min.

**G2 — No bid-ask spread / exit slippage (overstates a near-breakeven book). ✅ IMPLEMENTED** — `SLIPPAGE_BPS=15`/leg, entry+exit, `--slippage_bps`.
Entry gets a flat 5 bps (`V7_PAPER_SLIPPAGE_PCT`), but the **exit fills at the exact SL/target price** and the cost model charges only statutory + brokerage — **no half-spread on either leg**. Illiquid small-caps with 0.5 % targets routinely have 5–30 bps spreads; ignoring them flatters every PF and especially the scalps.
*Proposed change:* add a per-trade spread/slippage charge (configurable half-spread bps per side, or ATR/price-scaled) on **both** entry and exit; stress at 1×/2×/3×. An edge that dies at 2× spread is not deployable.

**G3 — The rigorous promotion gate exists but the tuner bypasses it (the overfit leak). ✅ IMPLEMENTED (in-file BH-FDR)** — `--fdr_alpha`/`--no_fdr`; full `run_gate` routing still a future option.
[`walkforward_gate.run_gate`](../walkforward_gate.py) is a proper gate: **purged** walk-forward folds with **embargo**, per-trade NET cost, bootstrap p>0, an IS/OOS overfit flag, **and Benjamini-Hochberg FDR correction across all setups** (exactly the multiple-testing fix for "we searched 16 setups and 1 passed"). Its own demo correctly REJECTs noise and downgrades overfit traps to PROBATION. **But `setup_train_test.py` only imports its `net_pnl_vectorized`/`_profit_factor` helpers** and re-implements a *single-window* train/test + a simpler day-block bootstrap with **no FDR correction**; `walk_forward.py` is a third, separate reimplementation that also skips FDR/`run_gate`. The single most valuable structural change is to **route promotion through `run_gate`** (or at minimum apply BH-FDR across every setup/family/objective searched in a session).

**G4 — Cost-model duality between tuner and v11 backtest. ✅ IMPLEMENTED (selector + reconciliation).**
The tuner judges net via the full **statutory** model (+ G2 spread); the v11 backtester resolves net via `v6._net_pnl_rs` — a **flat** `cost_bps` (default 16) on a fixed Rs 50k notional, +3 bps on SL. New `--cost_model {statutory,flat_bps}` lets the tuner judge with **v11's exact cost function** (`--cost_model flat_bps --slippage_bps 0` ≈ v11, modulo integer-qty rounding + v11's 5 bps asymmetric entry slip), and every run prints a **cost reconciliation** line (TEST book net under both models, side by side). Measured insight: pure statutory ≈ Rs 53/trade < v6's flat Rs 80 (16 bps) — i.e. v6 already bakes in ~5 bps of slippage buffer, but **less** than the tuner's new 15 bps/leg spread. *Still ideal (needs sign-off):* bring the **v11/v6 root** path onto the statutory+spread model so all three (tuner, v11, live) share one cost — left as a reviewable root edit, not done unilaterally → see [`PROPOSED_ROOT_COST_ALIGNMENT.md`](PROPOSED_ROOT_COST_ALIGNMENT.md). **That review also surfaced a 2× *notional* mismatch:** v11's historical resolution computes P&L on `v6.EFFECTIVE_NOTIONAL` = **Rs 50k**, while the tuner and live size at `V7_SIGNAL_NOTIONAL_RS` = **Rs 100k** — a pure sizing inconsistency (independent of cost philosophy) that the proposal also fixes.

**G5 — No portfolio/capital constraints in the research book. ✅ IMPLEMENTED (event-driven overlay).**
`eval_family` now applies `_apply_portfolio_overlay`: an entry-time-ordered simulation that enforces a **concurrent-position cap** (`--max_positions`, default **20** = the live executor limit; rarely binds for a single family, binds at book level) and a **daily-loss kill-switch** (`--daily_loss_rs`, default off — once a day's *realized* net ≤ −limit, new entries that day are blocked; open positions still run to exit). This makes the simulated book the **deployable** book (first-come + capped), tends to cut max-DD, and closes part of the backtest-vs-live selection gap. Verified on synthetic overlap/kill-switch cases.

### 11.3 Smaller logic notes (lower impact, still worth fixing)

> **Status (2026-06-23): L1–L4 ✅ implemented; L5 open.**

| # | Where | Note | Status |
|---|---|---|---|
| L1 | `apply_premom_terms` ([setup_train_test.py](setup_train_test.py)) | Was only `>=`/`<=`; other ops silently became `<=`. Now handles `>= <= > < == !=` and **fails closed** on unknown ops. | ✅ |
| L2 | `dedupe_family` | Score ties (e.g. tier-c rows with no `quality_score`) resolved **alphabetically by setup**. Now breaks by **earliest signal time** (first-come, matches live), setup name last. | ✅ |
| L3 | `search_setup` half-split | Was the **median day** (skewed when trades/day vary). Now the whole-day split closest to a 50/50 **trade-count** balance, both halves non-empty. | ✅ |
| L4 | proposal JSON (tuner + `aggressive_pf_tuner`) | Emitted `Infinity`/`NaN` literals (break strict parsers). New `_json_sanitize()` maps them → `null`. | ✅ |
| L5 | `_day_block_p` / bootstraps | Fixed seed (deterministic, fine) but single-seed point estimate; for borderline calls average a few seeds. | open |

### 11.4 Strategy-structure recommendations (for real, deployable edge)

1. **Lean into the momentum/trend-confirmation gate, not threshold scalps.** The only mechanism that generalized in the whole record is the pre-momentum + ADX gate (G/L/D families) with **wider R:R** (e.g. `G_HIGHER_HIGH_BREAK` 0.9/2.5 held: train 2.38 / test 2.66, p 0.005). The losers are ultra-tight scalps (`P_PDH` 0.5/0.6 = death-by-cost). Bias the book toward trend-confirmed entries with target ≥ ~2× SL.
2. **One explicit, slow regime switch at the book level — ban per-setup `market_ret_pct` bands. ✅ MECHANISM IMPLEMENTED** (`--regime_align`/`--regime_band`, default off): a single "don't fight the tape" filter (drop LONGs in a down market / SHORTs in an up market via causal `market_ret_pct`) expressed once for the whole book, while the per-setup search still bans `market_ret_pct`. *Next:* tune/validate the band (or swap the signal for NIFTY-VWAP / ADX-breadth) on the aligned-cost numbers.
3. **Promote on walk-forward + FDR, never a single peeked window.** Adopt `run_gate` semantics; require survival across band + strict + multi-fold + live-paper before sizing. The four-way confirmation the docs already do informally should be the codified bar.
4. **Add a daily-loss kill-switch and per-day trade cap to the book. ✅ IMPLEMENTED** via the G5 overlay (`--daily_loss_rs` + `--max_positions`). Both reduce tail DD and close part of the backtest-vs-live gap (live is implicitly capped).
5. **Calibrate the fill model against reality.** Extend `diff_conf_entries_vs_v11.py` from "which entries fired" to "**modeled vs realized live fill price**", and set `V7_PAPER_SLIPPAGE_PCT` + the new spread charge from that measured distribution. This closes the loop the live holdout keeps exposing.

### 11.5 Suggested implementation order

1. ✅ **G2 + G1 (cost/latency realism)** — `--slippage_bps` (15 default) + `--entry_latency_min`.
2. ✅ **G3 (BH-FDR across the family's setups)** — `--fdr_alpha` / `--no_fdr`.
3. ✅ **G5 (position cap + daily stop in the book)** — `--max_positions` (20) / `--daily_loss_rs`.
4. ✅ **G4 + L1–L4** — cost-model selector + reconciliation, and the hygiene fixes.

**Remaining:** align the **v11/v6 root** cost path to statutory+spread (reviewable root edit, needs
sign-off); **L5** (multi-seed bootstrap); and the **strategy-structure** work in §11.4 (momentum/ADX
gates with wider R:R, one book-level regime switch, promote on walk-forward + FDR + live-paper).
**Next operational step:** run a post-close `run_train_test.bat <FAMILY>` to see the new realistic
numbers (15 bps/leg + FDR + position cap), then re-judge the book.

---

*End of audit. This document covers every file in `Train_and_Test/` (and its `aggressive_pf_proposals/`
subfolder); generated bytecode and out-of-scope repo-root modules are noted in §9. §11 extends the
audit into the shared-core execution path that determines realized P&L.*
