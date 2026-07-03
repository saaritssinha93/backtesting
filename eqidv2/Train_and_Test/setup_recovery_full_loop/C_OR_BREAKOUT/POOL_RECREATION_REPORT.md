# C_OR_BREAKOUT — POOL_RECREATION_REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Raw data sources (same raw pre-gate basis as the unified pool)

| source | role | span used |
|---|---|---|
| `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv` (master, basis=raw) | base extract | 2026-03-04..2026-06-24 |
| reused fresh v11 raw generations from the A_MOD_BREAK_C1_HIGH campaign (`_gapfill_raw_gen`, `_tail_raw_gen`) | mid-June gap-fill + tail | 2026-06-17..2026-07-01 |
| fresh v11 generation 2026-07-03 (`setup_recovery_full_loop/_shared/d0702_raw_gen`) | final tail day | 2026-07-02 (145 rows) |
| 1-min store `stocks_indicators_1min_eq` | entry fill + SL/target/exit resolution | as needed |

Identical detector (`avwap_5min_ID_v2_backtesting._scan_day`, reason `opening_range_breakout`),
same 94-column schema, same probe pipeline as production. Entry = next 1-min open after the
5-min signal + slippage; exits on 1-min OHLC to 15:20 IST; statutory NSE intraday costs;
15 bps/leg search slippage.

## Recreated pool

- Path: `pools/pool_full/historical_all_available_pre_dedupe_live_candidates.csv`
- Rows: **8,414** (12,983 master + 1,004 fresh-gen → date-filtered, deduped on ticker|side|setup|signal_time_ist)
- Sessions: **75**, span **2026-03-04..2026-07-02**
- Manifest: `pools/pool_full/_manifest.json`; derived matrices under `pools/derived/`

## Requested vs actual windows

| window | requested | actual sessions | count |
|---|---|---|---|
| TRAIN | 2026-03-01..2026-05-30 | 2026-03-04..2026-05-29 | 52 |
| TEST | 2026-06-01..2026-07-02 | 2026-06-01..2026-07-02 | 23 |

FIT = first 60% of TRAIN sessions (31), VAL = remaining 40% (21).

## Missing / absent dates — classified

- **2026-05-28, 2026-06-26** — unrecoverable raw-store holes (documented in the A_MOD campaign).
- **2026-03-02/03-11 etc.** — genuine detector silence for this setup (LONG breakout quiet days).
- **NSE holidays**: 2026-03-03, 03-26, 03-31, 04-03, 04-14, 05-01.

## Data quality note

The raw pool's 5-min indicator columns (`adx`, `rsi`, `ema20_slope`, `macd_hist`,
`stock_ret`, wick pcts) are empty at source; wick/range features are recomputed by
`setup_train_test`'s feature layer, and indicator-style gating is available only through the
1-min pre-momentum features (verified present for this pool).
