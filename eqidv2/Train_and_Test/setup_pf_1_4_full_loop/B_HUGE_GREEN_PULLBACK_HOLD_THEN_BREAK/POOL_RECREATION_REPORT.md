# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — POOL_RECREATION_REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Raw data sources used

- **5-minute signal generation:** RESEARCH-MODE rerun of the production scanner (`avwap_5min_ID_v11_backtesting.py --mode historical_all_available`, EQIDV2_SIGNAL_DISCOVERY_V7_SELECTION_MODE=research_catalog to bypass the v8-exit-rule allowlist that drops catalog detectors; detector itself UNMODIFIED in candidate_scan.v2._scan_day) on data root `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`.
- **1-minute exit realism:** `C:\TradingData\eqidv2\stocks_indicators_1min_eq` (+ live raw 1-min fallback merge inside `v11._load_1m_with_open`), exits resolved to 15:20 IST.
- **Cost model:** statutory NSE intraday costs (`nse_intraday_costs`) + 15 bps/leg adverse slippage on entry AND exit (repo default for this book).
- Single-source research scan (no cross-source merge needed):

  - `research_scan_catalog_20260301_20260701`: 772 rows

## Requested vs actual windows

- requested TRAIN: `2026-03-01 .. 2026-05-30`
- actual TRAIN: `2026-03-02 .. 2026-05-29` (**38 completed sessions**)
- requested TEST: `2026-06-01 .. 2026-07-02`
- actual TEST: `2026-06-01 .. 2026-06-30` (**13 completed sessions**)

- 2026-07-02 (today) is EXCLUDED: the 5-min feed is complete but the EOD 1-min sync has not run, so SL/target exits cannot be simulated realistically for it yet.
- 2026-05-30 / 2026-05-31 are Sat/Sun; last May session is 2026-05-29.
- weekdays inside the window with NO session data (exchange holiday or no-data): `2026-03-03, 2026-03-09, 2026-03-12, 2026-03-18, 2026-03-25, 2026-03-26, 2026-03-31, 2026-04-01, 2026-04-03, 2026-04-07, 2026-04-08, 2026-04-10, 2026-04-13, 2026-04-14, 2026-04-15, 2026-04-17, 2026-04-20, 2026-04-21, 2026-04-29, 2026-05-01, 2026-05-13, 2026-05-15, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-22, 2026-05-28, 2026-06-02, 2026-06-08, 2026-06-10, 2026-06-11, 2026-06-17, 2026-06-22, 2026-06-24, 2026-06-25, 2026-06-26, 2026-07-01, 2026-07-02`

## Pool contents

- rows (pre-dedupe basis, cross-source deduped): **772**
- symbols: **493**
- TRAIN rows: 596 across 38 sessions (median 13/session)
- TEST rows: 176 across 13 sessions (median 13/session)
- per-setup pool file: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK\pools\B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK\historical_all_available_pre_dedupe_live_candidates.csv`

## Session coverage (per session)

| session | window | raw rows | tickers |
|---|---|---|---|
| 2026-03-02 | TRAIN | 36 | 32 |
| 2026-03-04 | TRAIN | 6 | 6 |
| 2026-03-05 | TRAIN | 8 | 8 |
| 2026-03-06 | TRAIN | 28 | 28 |
| 2026-03-10 | TRAIN | 7 | 7 |
| 2026-03-11 | TRAIN | 18 | 17 |
| 2026-03-13 | TRAIN | 15 | 15 |
| 2026-03-16 | TRAIN | 2 | 2 |
| 2026-03-17 | TRAIN | 16 | 11 |
| 2026-03-19 | TRAIN | 20 | 18 |
| 2026-03-20 | TRAIN | 10 | 10 |
| 2026-03-23 | TRAIN | 15 | 15 |
| 2026-03-24 | TRAIN | 7 | 7 |
| 2026-03-27 | TRAIN | 38 | 34 |
| 2026-03-30 | TRAIN | 29 | 25 |
| 2026-04-02 | TRAIN | 14 | 13 |
| 2026-04-06 | TRAIN | 6 | 6 |
| 2026-04-09 | TRAIN | 9 | 9 |
| 2026-04-16 | TRAIN | 30 | 27 |
| 2026-04-22 | TRAIN | 22 | 21 |
| 2026-04-23 | TRAIN | 1 | 1 |
| 2026-04-24 | TRAIN | 37 | 35 |
| 2026-04-27 | TRAIN | 7 | 7 |
| 2026-04-28 | TRAIN | 1 | 1 |
| 2026-04-30 | TRAIN | 13 | 11 |
| 2026-05-04 | TRAIN | 11 | 11 |
| 2026-05-05 | TRAIN | 20 | 18 |
| 2026-05-06 | TRAIN | 46 | 43 |
| 2026-05-07 | TRAIN | 36 | 35 |
| 2026-05-08 | TRAIN | 7 | 6 |
| 2026-05-11 | TRAIN | 3 | 3 |
| 2026-05-12 | TRAIN | 16 | 15 |
| 2026-05-14 | TRAIN | 2 | 2 |
| 2026-05-21 | TRAIN | 29 | 25 |
| 2026-05-25 | TRAIN | 1 | 1 |
| 2026-05-26 | TRAIN | 12 | 12 |
| 2026-05-27 | TRAIN | 1 | 1 |
| 2026-05-29 | TRAIN | 17 | 17 |
| 2026-06-01 | TEST | 14 | 13 |
| 2026-06-03 | TEST | 13 | 12 |
| 2026-06-04 | TEST | 1 | 1 |
| 2026-06-05 | TEST | 18 | 17 |
| 2026-06-09 | TEST | 2 | 2 |
| 2026-06-12 | TEST | 34 | 32 |
| 2026-06-15 | TEST | 24 | 23 |
| 2026-06-16 | TEST | 2 | 2 |
| 2026-06-18 | TEST | 8 | 8 |
| 2026-06-19 | TEST | 12 | 12 |
| 2026-06-23 | TEST | 15 | 14 |
| 2026-06-29 | TEST | 28 | 23 |
| 2026-06-30 | TEST | 5 | 5 |

## Data quality notes

- The pool is RAW candidates (pre-gate) from the production detector — the campaign tunes the same object the v11/live conf gate would consume.
- Weekdays with no session in any root (holiday/no-data) are listed above.
- Entry attachment drops rows with no next-1-min bar within 3 minutes of the 5-min signal (same rule as production).
- Rerun: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK\scripts\recreate_pool.py` (fresh-scan segment: see pools/_fresh_scan.log for the exact scanner command).