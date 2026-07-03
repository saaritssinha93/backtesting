# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — POOL_RECREATION_REPORT

_Generated 2026-07-02. Research-only; no live trades; no final_setup_conf.py edits._

## Raw data sources used

| source | role | span |
|---|---|---|
| `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv` | master rows, basis=`raw` (pre-gate candidate scan) | sessions .. 2026-06-24 |
| `C:\TradingData\eqidv2\outputs_ID_v11_conf_fresh_20260629\historical_all_available_days\<day>\raw_candidates.csv` | gap-fill for sessions the master lacks | 2026-06-17/18/19/23 |
| fresh v11 `historical_all_available` generation (this campaign, 8 workers) | tail sessions | 2026-06-25..2026-07-02 |
| `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2` | 5-min bars driving the fresh scan | through 2026-07-02 |
| `C:\TradingData\eqidv2\stocks_indicators_1min_eq` (+ live-raw supplement) | 1-min bars for entry fills and SL/target/EOD exit resolution | full 375-bar sessions through 2026-07-01; **2026-07-02 truncated ~09:30 IST** |

Basis cross-check before mixing: on overlapping days master vs conf_fresh raw scans agree within
1–4 rows (2026-06-15: 290 vs 293; 2026-06-12: 282 vs 285) — same raw basis.

## Recreated pool

- Path: `Train_and_Test/setup_pf_1_4_full_loop/A_MOD_CLOSE_CONTINUATION_BREAK/pools/pool_full/historical_all_available_pre_dedupe_live_candidates.csv`
- Manifest: `pools/pool_full/_manifest.json`; tail generation artifacts under `pools/_tail_raw_gen/`
- Rows: **7,354** (master 6,729 + gap-fill 268 + fresh tail 357), deduped on (ticker, side, setup, signal_time_ist)
- Sessions: **80** (2026-03-02 .. 2026-07-02); symbols: **1,044** distinct (scan universe ~1,280)

## Requested vs actual windows

| window | requested | actual sessions used | rows | symbols |
|---|---|---|---|---|
| TRAIN | 2026-03-01 .. 2026-05-30 | **58 sessions, 2026-03-02 .. 2026-05-29** | 5,649 | 999 |
| TEST | 2026-06-01 .. 2026-07-02 | **21 sessions, 2026-06-01 .. 2026-07-01** | 1,701 | 726 |

- FIT = first 60% of TRAIN sessions (35 sessions, 2026-03-02..2026-04-22); VAL = last 40% (23 sessions, 2026-04-23..2026-05-29) — exact lists printed by the baseline run.

## Missing dates and explanations

| date | status |
|---|---|
| 2026-03-01, 2026-05-30/31, weekend dates | weekend — not sessions |
| 2026-03-03, 03-26, 03-31, 04-03, 04-14, 05-01, 05-28 | not present in any candidate source (exchange holidays / no-data days); setup also fires 0 signals on some valid sessions |
| 2026-06-25 | session generated (948 raw candidates) but **0 signals for this setup** — legitimate: non-BEAR day, same-bar collapse gives the bar to A_MOD_BREAK_C1_HIGH (241 rows) |
| 2026-06-26 | **no 5-min data available** in `_eq_live2` — excluded by the availability indexer |
| 2026-07-02 | 5-min signals generated (kept in pool) but **excluded from TEST**: 1-min exit data truncated at ~09:30 IST, so SL/target/EOD exits cannot be simulated honestly |

## Per-TEST-day signal counts (raw)

209, 5, 68, 4, 193, 2, 13, 3, 4, 282, 290, 1, 8, 74, 81, 2, 105, 4, 256, 92, 5
(2026-06-01 .. 2026-07-01 in session order)

## Data quality issues / structural notes

1. **Same-bar collapse residual**: the shared candidate scan keeps ONE candidate per (ticker, 5-min bar)
   by quality score with alphabetical tie-break (`avwap_5min_ID_v7_candidate_scan._dedupe_candidate_frame`).
   A_MOD_BREAK_C1_HIGH (regime != BEAR) shadows this setup on non-BEAR bars, so **96.8% of this
   setup's rows are BEAR-regime** — it is structurally a bear-day continuation LONG. Day counts are
   therefore extremely bimodal (1–5 signals on quiet/bull days, 200–440 on bear days).
2. `ranker_score`, `rsi`, `adx`, `macd_hist` columns are empty for this setup's raw rows — excluded
   from the searchable feature set (see PARAMETER_INVENTORY.md); wick features are recomputed by
   `load_pool` from signal OHLC.
3. 5-minute data used for signal generation; 1-minute data used for entry fill (next 1-min open
   + 15 bps/leg slippage) and realistic SL/target/EOD exit simulation; statutory NSE intraday costs.

## Rerun commands

```
cd <repo root>
py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available --start_date 2026-06-25 --end_date 2026-07-02 --workers 8 --out Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\pools\_tail_raw_gen
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\build_pool_amccb.py
```
