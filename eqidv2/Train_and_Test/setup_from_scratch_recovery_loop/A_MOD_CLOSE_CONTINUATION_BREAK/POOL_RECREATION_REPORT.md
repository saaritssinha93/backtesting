# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — POOL_RECREATION_REPORT (recovery loop)

_Generated 2026-07-03. Research-only; no live trades; final_setup_conf.py untouched._

## Why a NEW pool (not the production one)

The production candidate pool for this setup is mis-sampled by construction: the shared scan
keeps ONE candidate per (ticker, bar), and `A_MOD_BREAK_C1_HIGH` (regime != BEAR,
alphabetically earlier) absorbs every non-BEAR bar. The card's pattern reached research as a
96.8% BEAR-day residue, and the v2 scan loop additionally never scans before ~10:55 IST.
This recovery loop therefore re-detects the card's own conditions directly from raw 5-min
data — uncollapsed, all regimes, from ~10:00 IST (earliest bar where the causal 20-bar
volume mean exists).

## Raw data sources

| source | role |
|---|---|
| `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2\*_stocks_indicators_5min.parquet` | 5-min OHLCV for signal generation (features recomputed causally via `v2._prepare_5m`) |
| NIFTYBEES 5-min (same store) | market return + regime context (`v2._market_context_from_df`, last bar <= signal) |
| `C:\TradingData\eqidv2\stocks_indicators_1min_eq` + live-raw supplement | entry fill (next 1-min open + 15 bps/leg) and SL/target/EOD exit simulation |

Faithful reuse of repo definitions: `_prepare_5m` (session-cumulative VWAP, SHIFTED 20-bar
volume mean, ATR fallback, vwap_dist_atr floor/clip), `_passes_common` liquidity floors
(price >= 80, bar value >= Rs1M, day value >= Rs20M after 10:00, range <= 3.5 ATR,
vol_ratio in [1.5, 8]), the LONG-momentum quality score, and the card's own conditions
(moderate impulse 0.60–2.20 ATR, close>open, close_loc >= 0.75, close > prev bar high,
above session VWAP, rs_pct > 0, vol_ratio >= 1.4, quality >= 6.8).

## Recreated pool

- Path: `pools/pool_redesigned/historical_all_available_pre_dedupe_live_candidates.csv`
  (+ `pools/pool_enriched/` with ~41 causal indicator/price-action/day-context features)
- Rows: **42,757** | tickers: **1,142** | sessions: **81** (2026-03-02 .. 2026-07-02)
- Median signals/session: 506 (pre-dedupe; the eval pipeline dedupes to one per ticker/day)
- **Regime mix: NEUTRAL 16,339 / BULL 13,928 / BEAR 9,976 / TREND 2,514** — vs the
  production pool's 96.8% BEAR. 71%+ of this universe was never visible to prior research.
- Structural flags emitted per signal: `x_bar_i`, `x_fresh_break` (prior bar had not already
  broken), `x_prev_pullback` (pullback-then-break two-stage), `x_break_rank_day`,
  `x_first_break_of_day`.

## Requested vs actual windows

| window | requested | actual |
|---|---|---|
| TRAIN | 2026-03-01 .. 2026-05-30 | 2026-03-02 .. 2026-05-29 (03-01/05-30 = weekend) |
| TEST | 2026-06-01 .. 2026-07-02 | 2026-06-01 .. 2026-07-01 (**07-02 excluded**: 1-min data truncated ~09:30; signals exist in pool but are not evaluated) |
| FIT / VAL | — | first 60% / last 40% of TRAIN sessions (exact lists in baseline JSON) |

## Missing dates / data quality

- 2026-06-26: no 5-min data in the store (absent for all tickers) — not a session here.
- 2026-07-02: 5-min signals generated; excluded from TEST (1-min exit data truncated).
- Holidays / no-data days as in the main campaign report (03-03, 04-03, 04-14, 05-01, ...).
- The `_eq_live2` store's historical duplicate-timestamp hazard is neutralised by
  `v2._read_ohlcv` (keep-last per bar).
- Stored parquet MACD/BB/CCI/MFI/OBV/VWAP columns are 0% populated in June — all such
  indicators are recomputed from OHLCV in the enrichment layer (uniform coverage).

## Rerun commands

```
cd <repo root>
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\scan_redesigned_pool.py
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\enrich_pool_features.py --no-premom --pool Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\pools\pool_redesigned --out Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\pools\pool_enriched
```
