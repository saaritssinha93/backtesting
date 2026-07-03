# B_HUGE_RED_FAILED_BOUNCE (SHORT) — POOL_RECREATION_REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Raw data sources used

- **5-minute signal generation:** production clean-pool scanner (`avwap_5min_ID_v11_backtesting.py --mode historical_all_available`, ab-gate enabled so A_*/B_* probation setups appear in the raw scan) on data root `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`.
- **1-minute exit realism:** `C:\TradingData\eqidv2\stocks_indicators_1min_eq` (+ live raw 1-min fallback merge inside `v11._load_1m_with_open`), exits resolved to 15:20 IST.
- **Cost model:** statutory NSE intraday costs (`nse_intraday_costs`) + 15 bps/leg adverse slippage on entry AND exit (repo default for this book).
- Harvested RAW-candidate segments (cross-source determinism verified on shared dates by the A_MOD campaigns — the shared scanner emits identical row sets per setup):

  - `cleanpool_chunks`: 2722 rows
  - `unified_recent_raw`: 45 rows
  - `conf_fresh_20260629`: 1160 rows
  - `fresh_scan_20260625_20260701`: 61 rows

## Requested vs actual windows

- requested TRAIN: `2026-03-01 .. 2026-05-30`
- actual TRAIN: `2026-03-02 .. 2026-05-29` (**53 completed sessions**)
- requested TEST: `2026-06-01 .. 2026-07-02`
- actual TEST: `2026-06-01 .. 2026-06-30` (**20 completed sessions**)

- 2026-07-02 (today) is EXCLUDED: the 5-min feed is complete but the EOD 1-min sync has not run, so SL/target exits cannot be simulated realistically for it yet.
- 2026-05-30 / 2026-05-31 are Sat/Sun; last May session is 2026-05-29.
- weekdays inside the window with NO session data (exchange holiday or no-data): `2026-03-03, 2026-03-12, 2026-03-26, 2026-03-31, 2026-04-03, 2026-04-08, 2026-04-14, 2026-04-17, 2026-04-21, 2026-05-01, 2026-05-20, 2026-05-28, 2026-06-11, 2026-06-26, 2026-07-01, 2026-07-02`

## Pool contents

- rows (pre-dedupe basis, cross-source deduped): **1998**
- symbols: **820**
- TRAIN rows: 1303 across 53 sessions (median 24/session)
- TEST rows: 695 across 20 sessions (median 33/session)
- per-setup pool file: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_RED_FAILED_BOUNCE\pools\B_HUGE_RED_FAILED_BOUNCE\historical_all_available_pre_dedupe_live_candidates.csv`

## Session coverage (per session)

| session | window | raw rows | tickers |
|---|---|---|---|
| 2026-03-02 | TRAIN | 39 | 36 |
| 2026-03-04 | TRAIN | 24 | 24 |
| 2026-03-05 | TRAIN | 39 | 37 |
| 2026-03-06 | TRAIN | 19 | 19 |
| 2026-03-09 | TRAIN | 13 | 12 |
| 2026-03-10 | TRAIN | 23 | 18 |
| 2026-03-11 | TRAIN | 41 | 38 |
| 2026-03-13 | TRAIN | 44 | 38 |
| 2026-03-16 | TRAIN | 12 | 12 |
| 2026-03-17 | TRAIN | 28 | 27 |
| 2026-03-18 | TRAIN | 22 | 22 |
| 2026-03-19 | TRAIN | 59 | 52 |
| 2026-03-20 | TRAIN | 27 | 26 |
| 2026-03-23 | TRAIN | 20 | 20 |
| 2026-03-24 | TRAIN | 4 | 4 |
| 2026-03-25 | TRAIN | 13 | 12 |
| 2026-03-27 | TRAIN | 25 | 25 |
| 2026-03-30 | TRAIN | 28 | 25 |
| 2026-04-01 | TRAIN | 17 | 16 |
| 2026-04-02 | TRAIN | 5 | 3 |
| 2026-04-06 | TRAIN | 4 | 4 |
| 2026-04-07 | TRAIN | 18 | 18 |
| 2026-04-09 | TRAIN | 28 | 26 |
| 2026-04-10 | TRAIN | 10 | 10 |
| 2026-04-13 | TRAIN | 1 | 1 |
| 2026-04-15 | TRAIN | 13 | 13 |
| 2026-04-16 | TRAIN | 34 | 32 |
| 2026-04-20 | TRAIN | 23 | 23 |
| 2026-04-22 | TRAIN | 32 | 29 |
| 2026-04-23 | TRAIN | 19 | 18 |
| 2026-04-24 | TRAIN | 34 | 33 |
| 2026-04-27 | TRAIN | 20 | 18 |
| 2026-04-28 | TRAIN | 47 | 42 |
| 2026-04-29 | TRAIN | 7 | 7 |
| 2026-04-30 | TRAIN | 29 | 26 |
| 2026-05-04 | TRAIN | 34 | 32 |
| 2026-05-05 | TRAIN | 26 | 24 |
| 2026-05-06 | TRAIN | 29 | 28 |
| 2026-05-07 | TRAIN | 21 | 19 |
| 2026-05-08 | TRAIN | 31 | 29 |
| 2026-05-11 | TRAIN | 22 | 21 |
| 2026-05-12 | TRAIN | 56 | 55 |
| 2026-05-13 | TRAIN | 3 | 3 |
| 2026-05-14 | TRAIN | 5 | 5 |
| 2026-05-15 | TRAIN | 37 | 32 |
| 2026-05-18 | TRAIN | 4 | 4 |
| 2026-05-19 | TRAIN | 18 | 16 |
| 2026-05-21 | TRAIN | 30 | 28 |
| 2026-05-22 | TRAIN | 5 | 5 |
| 2026-05-25 | TRAIN | 32 | 31 |
| 2026-05-26 | TRAIN | 35 | 30 |
| 2026-05-27 | TRAIN | 48 | 42 |
| 2026-05-29 | TRAIN | 46 | 39 |
| 2026-06-01 | TEST | 70 | 58 |
| 2026-06-02 | TEST | 11 | 11 |
| 2026-06-03 | TEST | 32 | 27 |
| 2026-06-04 | TEST | 16 | 14 |
| 2026-06-05 | TEST | 75 | 56 |
| 2026-06-08 | TEST | 51 | 31 |
| 2026-06-09 | TEST | 40 | 23 |
| 2026-06-10 | TEST | 13 | 10 |
| 2026-06-12 | TEST | 37 | 20 |
| 2026-06-15 | TEST | 48 | 29 |
| 2026-06-16 | TEST | 35 | 35 |
| 2026-06-17 | TEST | 9 | 9 |
| 2026-06-18 | TEST | 39 | 34 |
| 2026-06-19 | TEST | 27 | 25 |
| 2026-06-22 | TEST | 28 | 22 |
| 2026-06-23 | TEST | 83 | 76 |
| 2026-06-24 | TEST | 20 | 20 |
| 2026-06-25 | TEST | 6 | 6 |
| 2026-06-29 | TEST | 38 | 36 |
| 2026-06-30 | TEST | 17 | 17 |

## Data quality notes

- The pool is RAW candidates (pre-gate) from the production detector — the campaign tunes the same object the v11/live conf gate would consume.
- Weekdays with no session in any root (holiday/no-data) are listed above.
- Entry attachment drops rows with no next-1-min bar within 3 minutes of the 5-min signal (same rule as production).
- Rerun: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_RED_FAILED_BOUNCE\scripts\recreate_pool.py` (fresh-scan segment: see pools/_fresh_scan.log for the exact scanner command).