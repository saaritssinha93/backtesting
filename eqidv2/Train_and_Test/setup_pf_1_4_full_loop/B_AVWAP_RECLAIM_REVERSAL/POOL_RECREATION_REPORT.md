# B_AVWAP_RECLAIM_REVERSAL (LONG) — POOL_RECREATION_REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Raw data sources used

- **5-minute signal generation:** production clean-pool scanner (`avwap_5min_ID_v11_backtesting.py --mode historical_all_available`, ab-gate enabled so A_*/B_* probation setups appear in the raw scan) on data root `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`.
- **1-minute exit realism:** `C:\TradingData\eqidv2\stocks_indicators_1min_eq` (+ live raw 1-min fallback merge inside `v11._load_1m_with_open`), exits resolved to 15:20 IST.
- **Cost model:** statutory NSE intraday costs (`nse_intraday_costs`) + 15 bps/leg adverse slippage on entry AND exit (repo default for this book).
- Harvested RAW-candidate segments (cross-source determinism verified on shared dates by the A_MOD campaigns — the shared scanner emits identical row sets per setup):

  - `cleanpool_chunks`: 7129 rows
  - `unified_recent_raw`: 177 rows
  - `conf_fresh_20260629`: 3997 rows
  - `fresh_scan_20260625_20260701`: 276 rows

## Requested vs actual windows

- requested TRAIN: `2026-03-01 .. 2026-05-30`
- actual TRAIN: `2026-03-04 .. 2026-05-29` (**52 completed sessions**)
- requested TEST: `2026-06-01 .. 2026-07-02`
- actual TEST: `2026-06-01 .. 2026-07-01` (**22 completed sessions**)

- 2026-07-02 (today) is EXCLUDED: the 5-min feed is complete but the EOD 1-min sync has not run, so SL/target exits cannot be simulated realistically for it yet.
- 2026-05-30 / 2026-05-31 are Sat/Sun; last May session is 2026-05-29.
- weekdays inside the window with NO session data (exchange holiday or no-data): `2026-03-02, 2026-03-03, 2026-03-11, 2026-03-26, 2026-03-27, 2026-03-31, 2026-04-03, 2026-04-14, 2026-04-24, 2026-05-01, 2026-05-12, 2026-05-21, 2026-05-28, 2026-06-26, 2026-07-02`

## Pool contents

- rows (pre-dedupe basis, cross-source deduped): **6965**
- symbols: **1058**
- TRAIN rows: 4606 across 52 sessions (median 95/session)
- TEST rows: 2359 across 22 sessions (median 113/session)
- per-setup pool file: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\B_AVWAP_RECLAIM_REVERSAL\pools\B_AVWAP_RECLAIM_REVERSAL\historical_all_available_pre_dedupe_live_candidates.csv`

## Session coverage (per session)

| session | window | raw rows | tickers |
|---|---|---|---|
| 2026-03-04 | TRAIN | 117 | 107 |
| 2026-03-05 | TRAIN | 119 | 111 |
| 2026-03-06 | TRAIN | 27 | 26 |
| 2026-03-09 | TRAIN | 115 | 101 |
| 2026-03-10 | TRAIN | 110 | 99 |
| 2026-03-12 | TRAIN | 9 | 9 |
| 2026-03-13 | TRAIN | 29 | 29 |
| 2026-03-16 | TRAIN | 84 | 72 |
| 2026-03-17 | TRAIN | 78 | 75 |
| 2026-03-18 | TRAIN | 151 | 131 |
| 2026-03-19 | TRAIN | 15 | 15 |
| 2026-03-20 | TRAIN | 77 | 75 |
| 2026-03-23 | TRAIN | 22 | 21 |
| 2026-03-24 | TRAIN | 96 | 85 |
| 2026-03-25 | TRAIN | 183 | 168 |
| 2026-03-30 | TRAIN | 100 | 92 |
| 2026-04-01 | TRAIN | 114 | 105 |
| 2026-04-02 | TRAIN | 34 | 33 |
| 2026-04-06 | TRAIN | 39 | 35 |
| 2026-04-07 | TRAIN | 111 | 101 |
| 2026-04-08 | TRAIN | 38 | 34 |
| 2026-04-09 | TRAIN | 41 | 39 |
| 2026-04-10 | TRAIN | 172 | 152 |
| 2026-04-13 | TRAIN | 36 | 35 |
| 2026-04-15 | TRAIN | 117 | 105 |
| 2026-04-16 | TRAIN | 49 | 46 |
| 2026-04-17 | TRAIN | 100 | 91 |
| 2026-04-20 | TRAIN | 60 | 53 |
| 2026-04-21 | TRAIN | 117 | 104 |
| 2026-04-22 | TRAIN | 103 | 97 |
| 2026-04-23 | TRAIN | 118 | 102 |
| 2026-04-27 | TRAIN | 123 | 114 |
| 2026-04-28 | TRAIN | 132 | 112 |
| 2026-04-29 | TRAIN | 61 | 55 |
| 2026-04-30 | TRAIN | 54 | 49 |
| 2026-05-04 | TRAIN | 95 | 89 |
| 2026-05-05 | TRAIN | 82 | 77 |
| 2026-05-06 | TRAIN | 49 | 49 |
| 2026-05-07 | TRAIN | 78 | 71 |
| 2026-05-08 | TRAIN | 131 | 115 |
| 2026-05-11 | TRAIN | 97 | 81 |
| 2026-05-13 | TRAIN | 192 | 147 |
| 2026-05-14 | TRAIN | 78 | 61 |
| 2026-05-15 | TRAIN | 101 | 82 |
| 2026-05-18 | TRAIN | 27 | 24 |
| 2026-05-19 | TRAIN | 174 | 115 |
| 2026-05-20 | TRAIN | 82 | 63 |
| 2026-05-22 | TRAIN | 87 | 72 |
| 2026-05-25 | TRAIN | 124 | 102 |
| 2026-05-26 | TRAIN | 96 | 90 |
| 2026-05-27 | TRAIN | 130 | 110 |
| 2026-05-29 | TRAIN | 32 | 27 |
| 2026-06-01 | TEST | 4 | 4 |
| 2026-06-02 | TEST | 121 | 100 |
| 2026-06-03 | TEST | 114 | 93 |
| 2026-06-04 | TEST | 178 | 133 |
| 2026-06-05 | TEST | 61 | 48 |
| 2026-06-08 | TEST | 137 | 83 |
| 2026-06-09 | TEST | 275 | 142 |
| 2026-06-10 | TEST | 111 | 58 |
| 2026-06-11 | TEST | 98 | 58 |
| 2026-06-12 | TEST | 131 | 82 |
| 2026-06-15 | TEST | 68 | 43 |
| 2026-06-16 | TEST | 147 | 129 |
| 2026-06-17 | TEST | 109 | 94 |
| 2026-06-18 | TEST | 113 | 103 |
| 2026-06-19 | TEST | 118 | 106 |
| 2026-06-22 | TEST | 125 | 108 |
| 2026-06-23 | TEST | 22 | 21 |
| 2026-06-24 | TEST | 151 | 128 |
| 2026-06-25 | TEST | 72 | 64 |
| 2026-06-29 | TEST | 15 | 15 |
| 2026-06-30 | TEST | 113 | 101 |
| 2026-07-01 | TEST | 76 | 74 |

## Data quality notes

- The pool is RAW candidates (pre-gate) from the production detector — the campaign tunes the same object the v11/live conf gate would consume.
- Weekdays with no session in any root (holiday/no-data) are listed above.
- Entry attachment drops rows with no next-1-min bar within 3 minutes of the 5-min signal (same rule as production).
- Rerun: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_AVWAP_RECLAIM_REVERSAL\scripts\recreate_pool.py` (fresh-scan segment: see pools/_fresh_scan.log for the exact scanner command).