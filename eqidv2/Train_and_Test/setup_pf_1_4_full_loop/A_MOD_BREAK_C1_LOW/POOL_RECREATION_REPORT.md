# A_MOD_BREAK_C1_LOW (SHORT) — POOL_RECREATION_REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Raw data sources used

- **5-minute signal generation:** production clean-pool scanner (`avwap_5min_ID_v11_backtesting.py --mode historical_all_available`, ab-gate enabled so A_*/B_* probation setups appear in the raw scan) on data root `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`.
- **1-minute exit realism:** `C:\TradingData\eqidv2\stocks_indicators_1min_eq` (+ live raw 1-min fallback merge inside `v11._load_1m_with_open`), exits resolved to 15:20 IST.
- **Cost model:** statutory NSE intraday costs (`nse_intraday_costs`) + 15 bps/leg adverse slippage on entry AND exit (repo default for this book).
- Harvested RAW-candidate segments (cross-source determinism verified on shared dates — identical A_MOD row sets):

  - `cleanpool_chunks`: 32501 rows
  - `unified_recent_raw`: 657 rows
  - `conf_fresh_20260629`: 13737 rows
  - `fresh_scan_20260625_20260701`: 729 rows

## Requested vs actual windows

- requested TRAIN: `2026-03-01 .. 2026-05-30`
- actual TRAIN: `2026-03-02 .. 2026-05-29` (**53 completed sessions**)
- requested TEST: `2026-06-01 .. 2026-07-02`
- actual TEST: `2026-06-01 .. 2026-06-30` (**20 completed sessions**)

- 2026-07-02 (today) is EXCLUDED: the 5-min feed is complete but the EOD 1-min sync has not run, so SL/target exits cannot be simulated realistically for it yet.
- 2026-05-30 / 2026-05-31 are Sat/Sun; last May session is 2026-05-29.
- weekdays inside the window with NO session data (exchange holiday or no-data): `2026-03-03, 2026-03-12, 2026-03-26, 2026-03-31, 2026-04-03, 2026-04-08, 2026-04-14, 2026-04-17, 2026-04-21, 2026-05-01, 2026-05-20, 2026-05-28, 2026-06-11, 2026-06-26, 2026-07-01, 2026-07-02`

## Pool contents

- rows (pre-dedupe basis, cross-source deduped): **25212**
- symbols: **1092**
- TRAIN rows: 17594 across 53 sessions (median 263/session)
- TEST rows: 7618 across 20 sessions (median 365/session)
- per-setup pool file: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\pools\A_MOD_BREAK_C1_LOW\historical_all_available_pre_dedupe_live_candidates.csv`

## Session coverage (per session)

| session | window | raw rows | tickers |
|---|---|---|---|
| 2026-03-02 | TRAIN | 804 | 472 |
| 2026-03-04 | TRAIN | 505 | 323 |
| 2026-03-05 | TRAIN | 381 | 261 |
| 2026-03-06 | TRAIN | 284 | 195 |
| 2026-03-09 | TRAIN | 242 | 181 |
| 2026-03-10 | TRAIN | 236 | 165 |
| 2026-03-11 | TRAIN | 557 | 348 |
| 2026-03-13 | TRAIN | 764 | 445 |
| 2026-03-16 | TRAIN | 171 | 144 |
| 2026-03-17 | TRAIN | 236 | 170 |
| 2026-03-18 | TRAIN | 166 | 123 |
| 2026-03-19 | TRAIN | 874 | 444 |
| 2026-03-20 | TRAIN | 416 | 287 |
| 2026-03-23 | TRAIN | 532 | 376 |
| 2026-03-24 | TRAIN | 47 | 40 |
| 2026-03-25 | TRAIN | 43 | 43 |
| 2026-03-27 | TRAIN | 553 | 366 |
| 2026-03-30 | TRAIN | 488 | 327 |
| 2026-04-01 | TRAIN | 150 | 124 |
| 2026-04-02 | TRAIN | 73 | 59 |
| 2026-04-06 | TRAIN | 53 | 45 |
| 2026-04-07 | TRAIN | 167 | 115 |
| 2026-04-09 | TRAIN | 418 | 289 |
| 2026-04-10 | TRAIN | 50 | 49 |
| 2026-04-13 | TRAIN | 16 | 14 |
| 2026-04-15 | TRAIN | 106 | 93 |
| 2026-04-16 | TRAIN | 416 | 283 |
| 2026-04-20 | TRAIN | 255 | 201 |
| 2026-04-22 | TRAIN | 246 | 173 |
| 2026-04-23 | TRAIN | 238 | 191 |
| 2026-04-24 | TRAIN | 536 | 345 |
| 2026-04-27 | TRAIN | 209 | 160 |
| 2026-04-28 | TRAIN | 518 | 338 |
| 2026-04-29 | TRAIN | 148 | 141 |
| 2026-04-30 | TRAIN | 220 | 174 |
| 2026-05-04 | TRAIN | 411 | 287 |
| 2026-05-05 | TRAIN | 246 | 197 |
| 2026-05-06 | TRAIN | 363 | 248 |
| 2026-05-07 | TRAIN | 263 | 200 |
| 2026-05-08 | TRAIN | 382 | 261 |
| 2026-05-11 | TRAIN | 296 | 208 |
| 2026-05-12 | TRAIN | 973 | 531 |
| 2026-05-13 | TRAIN | 136 | 106 |
| 2026-05-14 | TRAIN | 78 | 64 |
| 2026-05-15 | TRAIN | 379 | 236 |
| 2026-05-18 | TRAIN | 118 | 93 |
| 2026-05-19 | TRAIN | 195 | 135 |
| 2026-05-21 | TRAIN | 546 | 310 |
| 2026-05-22 | TRAIN | 60 | 54 |
| 2026-05-25 | TRAIN | 419 | 277 |
| 2026-05-26 | TRAIN | 547 | 372 |
| 2026-05-27 | TRAIN | 359 | 251 |
| 2026-05-29 | TRAIN | 705 | 401 |
| 2026-06-01 | TEST | 682 | 379 |
| 2026-06-02 | TEST | 218 | 148 |
| 2026-06-03 | TEST | 492 | 312 |
| 2026-06-04 | TEST | 177 | 126 |
| 2026-06-05 | TEST | 692 | 373 |
| 2026-06-08 | TEST | 318 | 200 |
| 2026-06-09 | TEST | 455 | 213 |
| 2026-06-10 | TEST | 253 | 158 |
| 2026-06-12 | TEST | 443 | 203 |
| 2026-06-15 | TEST | 730 | 319 |
| 2026-06-16 | TEST | 371 | 257 |
| 2026-06-17 | TEST | 77 | 67 |
| 2026-06-18 | TEST | 360 | 259 |
| 2026-06-19 | TEST | 400 | 274 |
| 2026-06-22 | TEST | 253 | 193 |
| 2026-06-23 | TEST | 728 | 454 |
| 2026-06-24 | TEST | 240 | 174 |
| 2026-06-25 | TEST | 102 | 92 |
| 2026-06-29 | TEST | 418 | 294 |
| 2026-06-30 | TEST | 209 | 159 |

## Data quality notes

- The pool is RAW candidates (pre-gate) — the same basis `build_unified_pool.py` uses for this mined-short readmit setup, so the campaign tunes the same object that runs in v11/live.
- 2026-06-26 has no data in any root (holiday/no-data) — reported above.
- Entry attachment drops rows with no next-1-min bar within 3 minutes of the 5-min signal (same rule as production).
- Rerun: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\recreate_pool.py` (fresh-scan segment: see pools/_fresh_scan.log for the exact scanner command).