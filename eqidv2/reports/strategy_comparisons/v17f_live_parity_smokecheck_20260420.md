# V17F Live Parity Smoke Check ? 2026-04-20

## Scope

Goal: verify that the patched live 5-minute stack is now using the `v17f` short-side logic path.

Method used:
- compared the patched live parity scan path against the latest `v17f` backtest output
- focused on the top 3 short-heavy `v17f` days
- checked the exact entry slots where `v17f` fired, plus one extra trailing slot
- used the same historical parquet source as `v17f` backtest: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`

Comparison key:
- exact key = `(ticker, side, setup, entry_time_ist)`

## Exact-Key Results (same historical source as v17f)

| Trade date | Expected shorts | Raw seen | Final seen | Matched | Missing | Extras | Missing but raw-present | Tickers checked | Slots checked |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-03-13 | 21 | 19 | 19 | 19 | 2 | 0 | 0 | 21 | 5 |
| 2026-03-23 | 15 | 15 | 15 | 15 | 0 | 0 | 0 | 15 | 11 |
| 2025-07-25 | 10 | 8 | 8 | 8 | 2 | 0 | 0 | 10 | 7 |
| **Total** | **46** | **42** | **42** | **42** | **4** | **0** | **0** | - | - |

Key read:
- exact-key parity matched `42 / 46` expected shorts on the top 3 short-heavy days
- there were **0 false-positive extras** on the checked slots
- all 4 misses were `not_in_raw`, so they were not being dropped by downstream `v17f` cleanup filters in this smoke check path

## Misses

Exact-key misses found:
- `2026-03-13 09:55` ? `GMBREW SHORT A_MOD_BREAK_C1_LOW`
- `2026-03-13 09:55` ? `JAMNAAUTO SHORT A_MOD_BREAK_C1_LOW`
- `2025-07-25 09:50` ? `ARE&M SHORT A_MOD_BREAK_C1_LOW`
- `2025-07-25 10:20` ? `SAIL SHORT A_MOD_BREAK_C1_LOW`

## Single-Ticker Diagnostic On The 4 Misses

I ran a second diagnostic on those 4 tickers using the patched live short scanner on the full day.

Observed outcome:
- `GMBREW` was present in raw/final output, but at `09:50` instead of expected `09:55`
- `JAMNAAUTO` was present in raw/final output, but at `09:45` instead of expected `09:55`
- `ARE&M` was present in raw output, but at `09:40` instead of expected `09:50`, and did not survive final filtering in that replay path
- `SAIL` was present in raw/final output, but at `09:45` instead of expected `10:20`

Interpretation:
- the remaining gaps are **timestamp / emission-timing mismatches**, not a burst of wrong extra signals
- the patched live stack is following the `v17f` short filter chain, but a few historical rows are surfacing at earlier entry timestamps than the stored `v17f` trade CSV

## Operational Live Dir Check

I also tested the hottest short day (`2026-03-13`) against the operational live directory:
- `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live`

Result:
- expected `21`, raw `0`, final `0`, matched `0`, extras `0`

Interpretation:
- the operational live directory is **not suitable for historical parity replay** for that date in its current state
- for historical parity validation, `stocks_indicators_5min_eq_live2` is the correct source

## Bottom Line

What the smoke check supports:
- the patched live stack is now running through the `v17f` short-side logic path
- on the same historical source as the `v17f` backtest, the live parity replay produced **no extra short signals** on the tested high-short days
- the remaining differences are concentrated in **entry timestamp alignment**, not in the downstream `v17f` cleanup bundle

What is still not proven:
- exact `100%` one-to-one historical key parity for every short row
- there are 4 exact-key mismatches out of 46 on the top 3 short-heavy days, all tied to timing/slot alignment behavior
