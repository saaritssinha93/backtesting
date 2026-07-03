# B_HUGE_FAILED_BOUNCE (SHORT) — POOL_RECREATION_REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Raw data sources used

- **5-minute signal generation:** RESEARCH-MODE rerun of the production scanner (`avwap_5min_ID_v11_backtesting.py --mode historical_all_available`, EQIDV2_SIGNAL_DISCOVERY_V7_SELECTION_MODE=research_catalog to bypass the v8-exit-rule allowlist that drops catalog detectors; detector itself UNMODIFIED in candidate_scan.v2._scan_day) on data root `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`.
- **1-minute exit realism:** `C:\TradingData\eqidv2\stocks_indicators_1min_eq` (+ live raw 1-min fallback merge inside `v11._load_1m_with_open`), exits resolved to 15:20 IST.
- **Cost model:** statutory NSE intraday costs (`nse_intraday_costs`) + 15 bps/leg adverse slippage on entry AND exit (repo default for this book).
- Single-source research scan (no cross-source merge needed):

  - `research_scan_catalog_20260301_20260701`: 2932 rows

## Requested vs actual windows

- requested TRAIN: `2026-03-01 .. 2026-05-30`
- actual TRAIN: `2026-03-02 .. 2026-05-29` (**58 completed sessions**)
- requested TEST: `2026-06-01 .. 2026-07-02`
- actual TEST: `2026-06-01 .. 2026-07-01` (**22 completed sessions**)

- 2026-07-02 (today) is EXCLUDED: the 5-min feed is complete but the EOD 1-min sync has not run, so SL/target exits cannot be simulated realistically for it yet.
- 2026-05-30 / 2026-05-31 are Sat/Sun; last May session is 2026-05-29.
- weekdays inside the window with NO session data (exchange holiday or no-data): `2026-03-03, 2026-03-26, 2026-03-31, 2026-04-03, 2026-04-14, 2026-05-01, 2026-05-28, 2026-06-26, 2026-07-02`

## Pool contents

- rows (pre-dedupe basis, cross-source deduped): **2932**
- symbols: **941**
- TRAIN rows: 2003 across 58 sessions (median 30/session)
- TEST rows: 929 across 22 sessions (median 38/session)
- per-setup pool file: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\pools\B_HUGE_FAILED_BOUNCE\historical_all_available_pre_dedupe_live_candidates.csv`

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
| 2026-03-12 | TRAIN | 18 | 16 |
| 2026-03-13 | TRAIN | 44 | 38 |
| 2026-03-16 | TRAIN | 19 | 19 |
| 2026-03-17 | TRAIN | 28 | 27 |
| 2026-03-18 | TRAIN | 31 | 31 |
| 2026-03-19 | TRAIN | 59 | 52 |
| 2026-03-20 | TRAIN | 27 | 26 |
| 2026-03-23 | TRAIN | 20 | 20 |
| 2026-03-24 | TRAIN | 13 | 13 |
| 2026-03-25 | TRAIN | 65 | 64 |
| 2026-03-27 | TRAIN | 25 | 25 |
| 2026-03-30 | TRAIN | 28 | 25 |
| 2026-04-01 | TRAIN | 27 | 26 |
| 2026-04-02 | TRAIN | 5 | 3 |
| 2026-04-06 | TRAIN | 11 | 10 |
| 2026-04-07 | TRAIN | 25 | 25 |
| 2026-04-08 | TRAIN | 49 | 46 |
| 2026-04-09 | TRAIN | 28 | 26 |
| 2026-04-10 | TRAIN | 57 | 49 |
| 2026-04-13 | TRAIN | 24 | 22 |
| 2026-04-15 | TRAIN | 47 | 46 |
| 2026-04-16 | TRAIN | 36 | 34 |
| 2026-04-17 | TRAIN | 54 | 49 |
| 2026-04-20 | TRAIN | 41 | 40 |
| 2026-04-21 | TRAIN | 63 | 57 |
| 2026-04-22 | TRAIN | 32 | 29 |
| 2026-04-23 | TRAIN | 46 | 43 |
| 2026-04-24 | TRAIN | 34 | 33 |
| 2026-04-27 | TRAIN | 20 | 18 |
| 2026-04-28 | TRAIN | 49 | 44 |
| 2026-04-29 | TRAIN | 134 | 119 |
| 2026-04-30 | TRAIN | 31 | 27 |
| 2026-05-04 | TRAIN | 51 | 46 |
| 2026-05-05 | TRAIN | 45 | 41 |
| 2026-05-06 | TRAIN | 30 | 29 |
| 2026-05-07 | TRAIN | 24 | 22 |
| 2026-05-08 | TRAIN | 34 | 32 |
| 2026-05-11 | TRAIN | 22 | 21 |
| 2026-05-12 | TRAIN | 56 | 55 |
| 2026-05-13 | TRAIN | 18 | 18 |
| 2026-05-14 | TRAIN | 30 | 29 |
| 2026-05-15 | TRAIN | 30 | 27 |
| 2026-05-18 | TRAIN | 16 | 15 |
| 2026-05-19 | TRAIN | 18 | 18 |
| 2026-05-20 | TRAIN | 31 | 31 |
| 2026-05-21 | TRAIN | 22 | 20 |
| 2026-05-22 | TRAIN | 41 | 37 |
| 2026-05-25 | TRAIN | 29 | 28 |
| 2026-05-26 | TRAIN | 38 | 32 |
| 2026-05-27 | TRAIN | 51 | 46 |
| 2026-05-29 | TRAIN | 29 | 27 |
| 2026-06-01 | TEST | 38 | 38 |
| 2026-06-02 | TEST | 16 | 16 |
| 2026-06-03 | TEST | 21 | 18 |
| 2026-06-04 | TEST | 31 | 27 |
| 2026-06-05 | TEST | 51 | 46 |
| 2026-06-08 | TEST | 91 | 79 |
| 2026-06-09 | TEST | 23 | 20 |
| 2026-06-10 | TEST | 56 | 53 |
| 2026-06-11 | TEST | 65 | 57 |
| 2026-06-12 | TEST | 26 | 25 |
| 2026-06-15 | TEST | 23 | 23 |
| 2026-06-16 | TEST | 36 | 36 |
| 2026-06-17 | TEST | 61 | 54 |
| 2026-06-18 | TEST | 39 | 34 |
| 2026-06-19 | TEST | 27 | 25 |
| 2026-06-22 | TEST | 40 | 32 |
| 2026-06-23 | TEST | 82 | 75 |
| 2026-06-24 | TEST | 20 | 20 |
| 2026-06-25 | TEST | 68 | 59 |
| 2026-06-29 | TEST | 38 | 36 |
| 2026-06-30 | TEST | 17 | 17 |
| 2026-07-01 | TEST | 60 | 56 |

## Data quality notes

- The pool is RAW candidates (pre-gate) from the production detector — the campaign tunes the same object the v11/live conf gate would consume.
- Weekdays with no session in any root (holiday/no-data) are listed above.
- Entry attachment drops rows with no next-1-min bar within 3 minutes of the 5-min signal (same rule as production).
- Rerun: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\scripts\recreate_pool.py` (fresh-scan segment: see pools/_fresh_scan.log for the exact scanner command).