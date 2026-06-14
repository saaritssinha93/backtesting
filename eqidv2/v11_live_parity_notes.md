# v11 Live-Parity Notes — `v11_ID_backtesting.py`
*Last updated: 2026-06-11*

This document accompanies **`v11_ID_backtesting.py`**, the V7-Live parity backtest
entry point. It explains exactly how parity is achieved, what was reused vs.
omitted, the dependency map, the data contract, run commands, and how to validate
one date against the live V7 paper-trade logs.

> **Parity contract:** the backtester must produce the **same entry trades at the
> same 5-minute signal timestamps** as live V7. Exits are evaluated on 1-minute
> data and *may differ* (the live executor fills on real-time LTP and runs an
> intraday monitor; the backtest walks historical 1-minute OHLC).

---

## 1. V7 Live files inspected (end-to-end trace)

Traced from dashboard/scheduler → execution:

| # | Stage | File(s) inspected |
|---|---|---|
| 1 | Dashboard / launch config | `log_dashboard_server.py`, `bat/run_eqidv2_signal_discovery_v7_5min_id_persistent.bat`, `bat/run_eqidv2_entry_engine_1min_v5_id.bat`, `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat` |
| 2 | 5-min scan / candidate creation | `avwap_5min_ID_v7_candidate_scan.py` → `avwap_5min_ID_v2_backtesting.py` (`_scan_day`, `_prepare_5m`, `add_catalog` setup definitions, `_passes_common`) |
| 3 | Signal discovery pipeline | `eqidv2_signal_discovery_v7_5min_id_persistent.py` (`add_live_ranker_scores`, `apply_v8_live_gate`, `apply_research_live_filters`, `_filter_entry_window`) |
| 4 | V11 overlay / Tier123 | `eqidv2_v11_live_overlay.py` (`apply_live_candidate_overlay`) |
| 5 | 1-min entry engine | `eqidv2_entry_engine_1min_v5_id.py` (entry guards, pre-entry momentum gates, T+1 next-1m-open, sizing) |
| 6 | Executor (exits) | `avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py` |
| 7 | Exit rules / metrics | `avwap_5min_ID_v6_backtesting.py` (`SETUP_EXIT_RULES`, `_metrics`, `_summary_text`, `DATA_1M_DIR`) |
| 8 | Exit price-path resolver | `v17D_exit_resolver.py` (`resolve()`, EOD cutoff 15:20 IST) |
| 9 | Costs | `nse_intraday_costs.py` |
| 10 | **Canonical replay engine** | `avwap_5min_ID_v11_backtesting.py` — already invokes #2–#8 on historical bars |

---

## 2. Logic copied into `v11_ID_backtesting.py`

**Design decision (critical):** Parity is achieved by **invoking the live modules
directly**, not by hand-copying their logic. The canonical adaptation that drives
the live decision tree on historical bars is `avwap_5min_ID_v11_backtesting.py`
(the "engine"); `v11_ID_backtesting.py` is the clean wrapper around it. See §3 for
why a literal self-contained copy was rejected.

What `v11_ID_backtesting.py` itself contains in-house:

1. **The exact live env-var strategy config**, copied **verbatim** from the two V7
   `.bat` files into `V7_LIVE_STRATEGY_ENV`, and applied **before importing any
   live module** (the live modules read these at import/call time). This is what
   makes the imported V7 logic behave identically to live. Includes:
   - entry window `09:30–14:30`, `ENTRY_LAG_MIN=1`, selection mode `v8_setup_compatible`
   - `V8_GATE=1` + accepted-rules path, `V11_TIER123=1`, `RESEARCH_FILTERS=1` (active)
   - anti-chase (`close_loc>0.97`, `vwap_dist_atr>3.50`), `B_AVWAP_RECLAIM_RANKER_MIN=0.65`, `L_TREND_PULLBACK_PROBATION_BLOCK=1`
   - `SHORT_FOCUS=0` (both sides) + exempt setups
   - full `EARLY_MODE` sub-config (gate min score 95, per-side 4, per-slot 8, blocked setups, ORB/GAP/VWAP early thresholds)
   - `UNCOVERED_FALLBACK` (11:05–13:55, ranker≥0.65, quality≥125, allowed setups)
   - entry engine: `ENTRY_LAG_MIN=1`, `MAX_DELAY_MIN=3`, `PRE_MOMENTUM_GATES=1`, `MISSING_ACTION=block`
2. **Backtest orchestration** that builds the engine's args and dispatches to the
   live pipeline (`_run_historical_full_day` / `_run_historical_all_available`).
3. **A new live-log parity check** (`parity_check`) comparing backtest entries vs
   the V7 live `paper_trades_<date>_id_5min_v7.csv` logs.
4. UTF-8 stream safety + Python ≥3.10 guard + production logging/fail-safe errors.

Logic reused (by import) from the engine, unchanged:
the full pipeline scan → ranker → v8 gate → v11 overlay → research filters →
entry-window → one-ticker-per-day dedupe → 1-min entry engine (guards +
pre-momentum) → `v17D_exit_resolver.resolve` exits, and the canonical output
writers (`_build_v11_id_trades`, `_write_outputs`, `_write_parity_debug_csv`).

---

## 3. Intentionally NOT copied, and why

| Not copied | Why |
|---|---|
| **A literal self-contained copy of all live logic** | ~4,000 lines across 8 modules. A copy **drifts** from the live source the moment live changes, silently breaking parity. Importing the live modules is the only way to *guarantee* "same entries". Documented as a deliberate parity-safety decision. |
| Live operational env vars (scan workers, feed-gate timing, `POST_SLOT_DELAY`, restart/heartbeat/logging) | Live-feed-timing only; they do not change *which* trades fire on complete historical bars. |
| Live data dirs (`stocks_indicators_5min_eq_live`, set in the entry-engine bat) | Backtest uses the historical store `_eq_live2` (see §5 risk #1). |
| Executor live-LTP fill, OCO monitor, trailing-stop daemon, MTM brake, 20-position cap, per-day throttle | Live **execution** layer. Backtest fills next-1m-open and resolves exits on 1-min OHLC. These affect *which live trades actually execute* and *exit prices*, not the 5-min entry signal. (Risk #4.) |
| ADV 1% cap, F&O ban-as-of-date filter, NIFTY-regime SHORT halving | Require as-of-date external data not in the parquet store; conservatively omitted (may **over-admit** vs live). (Risk #3.) |
| ~~v11 `selected_strategy` profiles~~ | **CORRECTED 2026-06-11:** the v11 **overlay** admits its setup-universe *per the production profile*, so it IS part of live — parity runs the **production default profile**, not `none` (see §4 #1). The post-entry mask portion may be research-only, but using the production profile reproduces live entries. |

---

## 4. Assumptions

1. **`selected_strategy_profile=<production default>` + `ab_gate_profile=quality_top_slot`**
   is the engine configuration that reproduces **live entries**. This was
   *empirically established* by the parity_check, which falsified the initial
   `none` assumption: on 2026-06-10, `none` matched **0/2** live entries (the live
   `E_VWAP_LOSE_EARLY_SHORT` trades are overlay-admitted and the overlay needs the
   production profile), while the production profile matched **2/2**. The
   production default is `engine.SELECTED_STRATEGY_DEFAULT_PROFILE`
   (`production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced`).
2. The engine's pipeline functions, called with the live env config set, equal
   the live scanner's behavior (same module code, same constants).
3. The 5-minute "signal slot" for comparison = the candidate's signal timestamp
   floored to 5 minutes; live's `signal_datetime` is the same slot.
4. T+1 entry: the first available 1-minute bar strictly after the 5-minute signal
   close, within `MAX_DELAY_MIN=3` (engine `_first_1m_entry`).
5. Pre-entry momentum features recomputed from historical 1-minute parquet equal
   (closely) the live engine's real-time computation; missing features → block
   (live `PRE_ENTRY_MOMENTUM_MISSING_ACTION=block`).

---

## 5. Known mismatch risks (live vs backtest)

1. **Data store divergence (highest).** Live signal discovery reads
   `stocks_indicators_5min_eq_live`; the backtest reads `…_eq_live2`. If the two
   stores differ for a session, candidates differ. **Mitigation:** run
   `v7_causality_audit.py` against `_eq_live2` and spot-check bar-equality vs
   `_eq_live` for N ticker-days. Override with `--data_5m_dir` to point at the
   live store for strict same-day parity.
2. **Pre-entry momentum feature divergence.** Live computes ADX/RSI/window
   features from real-time 1-min fetch; backtest recomputes from parquet over a
   possibly different bar range → borderline candidates can flip.
3. **Omitted live admission constraints** (ADV cap, F&O ban-as-of-date, regime
   sizing). Backtest may admit entries live would have skipped → **V11-only**
   extras in the parity report.
4. **Execution layer not modeled** (LTP fill vs next-1m-open, 20-position cap,
   MTM brake, per-day throttle, OCO/trailing exits). Affects exit prices and
   whether a live signal *actually executed*; not the 5-min entry decision. Live
   that hit a position cap shows as **V11-only**.
5. **Live intraday feed-timing race.** Live can miss a slot if the 5-min bar
   wasn't fully written (feed-completion gating). Backtest always has complete
   bars → can show **V11-only** entries that live raced past. (See
   `project_signal_discovery_v7_feed_race`.)
6. **Same-bar setup collapse / overlay-profile coupling.** The overlay is invoked
   with `profile` (here `none`); live's overlay profile may admit a slightly
   different setup-universe → occasional setup mismatch.
7. **Live paper-writer ragged CSV** (a row with an extra field). The parity
   loader reads with `on_bad_lines="warn"` (skips + warns) so it never crashes,
   but a skipped row would show as a false **LIVE-only-missing**. Fix the writer
   upstream for exactness.
8. **Forming-candle / timezone**: all timestamps normalized to `Asia/Kolkata`;
   the scan uses closed 5-min bars only (no lookahead). EOD cutoff 15:20 IST.

---

## 6. Exact dependency path map

| Module / artifact | Path |
|---|---|
| Entry point (this file) | `v11_ID_backtesting.py` |
| Canonical replay engine | `avwap_5min_ID_v11_backtesting.py` |
| Live signal discovery | `eqidv2_signal_discovery_v7_5min_id_persistent.py` |
| Candidate scan | `avwap_5min_ID_v7_candidate_scan.py` |
| Setup detection / features | `avwap_5min_ID_v2_backtesting.py` |
| V11 overlay / Tier123 | `eqidv2_v11_live_overlay.py` |
| 1-min entry engine | `eqidv2_entry_engine_1min_v5_id.py` |
| Exit rules / metrics | `avwap_5min_ID_v6_backtesting.py` |
| Exit price-path resolver | `v17D_exit_resolver.py` |
| Cost model | `nse_intraday_costs.py` |
| Live strategy config (env) | `bat/run_eqidv2_signal_discovery_v7_5min_id_persistent.bat`, `bat/run_eqidv2_entry_engine_1min_v5_id.bat` |
| V8 accepted rules | `C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv` |
| 5-min data (entry signals) | `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2` |
| 1-min data (exit sim) | `C:\TradingData\eqidv2\stocks_indicators_1min_eq` |
| Live candidate JSON | `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json` |
| Live paper-trade logs | `C:\TradingData\eqidv2\live_signals\paper_trades_<date>_id_5min_v7.csv` |
| Default output dir | `C:\TradingData\eqidv2\outputs_ID_v11_5min` |

**TODO (if missing):** if `outputs_ID_v8_5min_research_restore\accepted_rules.csv`
is absent, the v8 gate falls back to its default; confirm it exists for strict
parity (the script logs a warning, does not crash).

---

## 7. Required data columns

**5-minute parquet** (`<TICKER>_stocks_indicators_5min.parquet`): `date`, `open`,
`high`, `low`, `close`, `volume`, `ATR`, `VWAP`, `EMA_20`, `EMA_50`, `EMA_200`,
`ADX`, `RSI`, `Stoch_%K`, `Upper_Band`, `Lower_Band`, `bb_width_pct`,
`bb_width_mean100`. Derived in `_prepare_5m`: `close_loc`, `body_pct`, `vol_ratio`
(`volume/Volume_SMA20`), `atr_pct`, `vwap_dist_atr`, `range`, `day_value_so_far_rs`,
wicks. Min required for a usable bar: OHLCV + ATR + VWAP + ADX + RSI.

**1-minute parquet** (`<TICKER>_stocks_indicators_1min.parquet`): `date`, `open`,
`high`, `low`, `close`, `volume`, `ADX`, `RSI` (ADX/RSI used by the pre-entry
momentum gate; OHLC used for entry fill + exit resolution).

All `date` columns are coerced to tz-aware `Asia/Kolkata`.

---

## 8. How to run

```bat
:: single trading day (full V7 pipeline + 1-min exits + per-candidate audit)
py -3.12 v11_ID_backtesting.py --mode backtest --date 2026-06-10 --parity-debug ^
    --out C:\TradingData\eqidv2\outputs_ID_v11_5min --workers 8

:: date range
py -3.12 v11_ID_backtesting.py --mode backtest --start_date 2026-01-01 --end_date 2026-06-10 --workers 8

:: strict same-day parity: point 5-min data at the LIVE store
py -3.12 v11_ID_backtesting.py --mode backtest --date 2026-06-10 ^
    --data_5m_dir C:\TradingData\eqidv2\stocks_indicators_5min_eq_live
```
Notes: `--workers 8` max on the shared live machine (protects the live feed/Spyder).
Outputs land in `--out`: `v11_ID_trades.csv`, `v11_ID_summary.csv`,
`v11_ID_setup_summary.csv`, `v11_ID_daily_summary.csv`, and (with `--parity-debug`)
`v11_ID_parity_debug.csv`.

`v11_ID_trades.csv` columns: `date, symbol, side, setup_name, entry_time,
entry_price, entry_source_timeframe, exit_time, exit_price, exit_reason, pnl,
pnl_pct, quantity, reason, filters_passed, filters_failed, source_5min_file,
source_1min_file`.

---

## 9. How to validate V7 Live vs V11 parity (one date)

```bat
:: 1) run the backtest for the date (writes the book + parity-debug)
py -3.12 v11_ID_backtesting.py --mode backtest --date 2026-06-10 --parity-debug --out <DIR>

:: 2) compare backtest entries vs the live paper-trade log for that date
py -3.12 v11_ID_backtesting.py --mode parity_check --date 2026-06-10 --out <DIR>
```
`parity_check` keys both books on `(symbol, side, setup, 5-min-signal-slot)` and
writes `v11_ID_parity_vs_live_<date>.csv` + a console summary with:

1. **BOTH** — entries present in both (parity hits).
2. **LIVE_ONLY_missing_in_v11** — live took it, backtest didn't (investigate:
   feed timing, data store, a momentum-gate flip).
3. **V11_ONLY_absent_in_live** — backtest took it, live didn't (usually an omitted
   live constraint: position cap, ADV/ban, regime — risks §5 #3/#4).
4. **TIMESTAMP_MISMATCH** — same symbol/side/setup, different 5-min slot.
5. **SYMBOL/SETUP_LIVE_ONLY** / **SYMBOL/SETUP_V11_ONLY** — symbol/setup present one side only.
6. **live→v11 match rate** — fraction of live entries reproduced by the backtest.

For each mismatch, open `v11_ID_parity_debug.csv` and filter on the symbol/slot to
read the per-candidate **fate + reason** across every pipeline stage (ranked →
v8_gate → research_filter → entry_engine → accepted/rejected). That reason string
tells you exactly which filter diverged.

**Target:** `LIVE_ONLY_missing_in_v11 == 0` (backtest reproduces every live entry).
`V11_ONLY` > 0 is expected and acceptable — those are entries live would have taken
but for execution constraints not modeled in backtest (§5).
