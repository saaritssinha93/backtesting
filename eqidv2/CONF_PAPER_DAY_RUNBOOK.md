# Conf paper-day runbook — first end-to-end conf vs v11 (Phase 4 §F)

Goal: run the 16-setup conf live on **paper** for one session, watch the MTM brake
in observe mode, then diff the day's entries against the v11 same-day backtest.
Nothing here touches real capital. `D` = the trading date, e.g. `2026-06-16`.
`D8` = the same date without dashes, e.g. `20260616`.

## 0. Pre-open (any time before 09:15 IST)
```
cd c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2
set EQIDV2_USE_FINAL_SETUP_CONF=1 && py -3.12 conf_live_preflight.py
```
Expect **FAIL=0** (one WARN: live-executor brake wiring, fine for paper). If a P0-19
line FAILs, the paper/live caps drifted — fix before running.

## 1. Launch the conf paper stack (replaces the normal 3 launchers for the day)
Run these three (Task Scheduler or three terminals). They set the conf flag and
call the existing launchers, so all the usual env (SHORT_FOCUS=0, feed gating, etc.)
still applies; the bootstrap un-blocks the conf setups (incl. E_ORB_RETEST_HOLD_LONG,
C_OR_BREAKDOWN) and the Tier-C live detectors run automatically.
```
bat\run_conf_paper_signal_discovery.bat
bat\run_conf_paper_entry_engine.bat
bat\run_conf_paper_executor.bat
```
Do **not** run the normal launchers at the same time (one scanner/engine/executor).

## 2. Confirm conf is active (first slot, ~09:20)
In the logs, expect:
- scanner: `final_setup_conf ACTIVE (...): 16 setups (match-v11): 6 native through v8+overlay+research, 10 non-native readmitted; conf mask is the final selection`
- scanner: `conf tier-c scan added N candidates` (Tier-C firing)
- entry engine: `final_setup_conf ACTIVE (...): pre-momentum gates + exit levels from conf`
- paper executor: brake ON, 20-position cap (P0-19), and any `[RISK.BRAKE.MTM][OBSERVE]` lines

## 3. During the session — watch the MTM brake (observe mode)
`[RISK.BRAKE.MTM][OBSERVE] would block ...` lines show when the MTM-aware brake
WOULD act (it does not act yet). Sanity-check the trigger fires on the right shape
(open MTM near -Rs10k, or throttle/per-setup-cap). Only after this looks right do
you enable acting (`EQIDV2_BRAKE_MTM_ACT=1`, then later flatten) — uncomment in
`bat\run_conf_paper_executor.bat`.

## 4. EOD — produce the v11 reference for the same day
```
py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available ^
  --selected_strategy_profile final_setup_conf --workers 8 ^
  --start_date D --end_date D ^
  --out C:/TradingData/eqidv2/outputs_ID_v11_conf_paper_D
```

## 5. Diff entries (live vs v11)
Cleanest entry-parity (engine-selected rows, before executor caps):
```
py -3.12 diff_conf_entries_vs_v11.py ^
  --v11 C:/TradingData/eqidv2/outputs_ID_v11_conf_paper_D/v11_ID_trades.csv ^
  --live "C:/TradingData/eqidv2/entry_engine_1min_v5_ID/audit/entry_rows_D8_*.csv"
```
Use `entry_rows_D8_*.csv` only. Do not use `entry_rows_*D8*.csv`, because that
also catches `entry_rows_raw_candidates_*` files. The diff tool skips empty
selected-entry CSVs safely.

Realistic executed comparison (expect live ⊆ v11 due to 20-pos/dedup/F&O/freshness):
```
py -3.12 diff_conf_entries_vs_v11.py ^
  --v11 C:/TradingData/eqidv2/outputs_ID_v11_conf_paper_D/v11_ID_trades.csv ^
  --live "C:/TradingData/eqidv2/live_signals/paper_trades_D_id_5min_v7.csv"
```

## 6. Read the result
- High entry recall on the engine-selected diff = the migration reproduces v11 live.
- Tier-C source parity should already be clean (`validate_conf_tier_c_parity.py`
  passed 240/240 sampled current-source scans after the 2026-06-15 source rebuild).
  Treat `v11-only` rows as real misses to investigate: data/feed timing,
  forming-candle timing, market context, or entry-engine freshness.
- `live-only` (extras) should be ~0 — anything there means the live gate admitted
  something v11 didn't; investigate.

## 7. Revert (end of experiment)
Stop the three conf paper processes; relaunch the normal `bat\run_eqidv2_*` launchers.
The flag defaults OFF, so the normal stack is unchanged. Nothing persisted.

## Not yet enabled (deliberately)
- `EQIDV2_BRAKE_MTM_ACT` (brake actually blocks) — enable after watching observe logs.
- `EQIDV2_BRAKE_FLATTEN_ON_BREACH` (flatten open positions) — last, after act is trusted.
- Live executor (PAPER_TRADE_FALSE) brake wiring — mirror the paper wiring after the
  paper observe period; only then is real capital on the table.
