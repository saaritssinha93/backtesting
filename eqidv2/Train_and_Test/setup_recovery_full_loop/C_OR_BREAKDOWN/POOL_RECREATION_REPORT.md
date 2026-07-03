# C_OR_BREAKDOWN — POOL_RECREATION_REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Raw data sources (same raw pre-gate basis as the unified pool)

| source | role | span used |
|---|---|---|
| `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv` (master, basis=raw) | base extract | 2026-03-02..2026-06-24 |
| reused fresh v11 raw generations from the A_MOD_BREAK_C1_HIGH campaign (`setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/_gapfill_raw_gen`, `_tail_raw_gen`) | mid-June gap-fill + tail | 2026-06-17..2026-07-01 |
| fresh `avwap_5min_ID_v11_backtesting.py --mode historical_all_available` run 2026-07-03 (`_shared/d0702_raw_gen`) | final tail day | 2026-07-02 (147 rows) |
| 1-min store `stocks_indicators_1min_eq` | entry fill + SL/target/exit resolution | as needed |

All three candidate sources are the identical detector (`avwap_5min_ID_v2_backtesting._scan_day`,
reason `opening_range_breakdown`) on `stocks_indicators_5min_eq_live2`, same 94-column schema.
Entry = next 1-min open after the 5-min signal + slippage; exits resolved on 1-min OHLC to 15:20
IST; costs = statutory NSE intraday model; search slippage 15 bps/leg (5 bps sensitivity at
confirmation), exactly as `setup_train_test.py` implements.

## Recreated pool

- Path: `pools/pool_full/historical_all_available_pre_dedupe_live_candidates.csv`
- Rows: **12,219** (25,272 master + 1,720 fresh-gen → date-filtered 03-01..07-02, deduped on ticker|side|setup|signal_time_ist)
- Sessions: **73**, span **2026-03-02..2026-07-02**
- Manifest with exact session list: `pools/pool_full/_manifest.json`
- Derived research matrices: `pools/derived/` (attached entries, premom features @0.90 ref stop,
  35-combo exit grid, MAE/MFE) — built by `_shared/precompute.py`.

## Requested vs actual windows

| window | requested | actual sessions | count |
|---|---|---|---|
| TRAIN | 2026-03-01..2026-05-30 | 2026-03-02..2026-05-29 | 53 |
| TEST | 2026-06-01..2026-07-02 | 2026-06-01..2026-07-02 | 20 |

- FIT = first 60% of TRAIN sessions; VAL = remaining 40% (exact lists printed by every stage run).
- 2026-03-01 = Sunday; 2026-05-30/31 = weekend → nearest completed sessions used.

## Missing / absent dates — classified

- **2026-05-28, 2026-06-26** — unrecoverable raw-store holes (regeneration attempted in the
  A_MOD campaign; "no available historical dates" across all ~1,295 tickers).
- **2026-06-11, 2026-07-01** — genuine detector silence for THIS setup (other setups have rows
  those days; an OR-breakdown short simply did not trigger).
- **NSE holidays** (zero rows across all setups): 2026-03-03, 03-26, 03-31, 04-03, 04-14, 05-01.

## Notes

- The pool is RAW pre-gate basis (same basis the setup was originally mined and promoted on).
- 5-min data generates the signal; 1-min data does the fill + SL/target/EOD path — both present
  for the full span (07-02 1-min EOD backfill landed 07-03).
