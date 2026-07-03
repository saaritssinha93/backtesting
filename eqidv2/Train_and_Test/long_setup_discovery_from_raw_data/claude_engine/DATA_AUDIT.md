# DATA_AUDIT — raw 5-min / 1-min stores for FAST-MOMENTUM LONG (~0.75%) discovery

Stage-1 audit (read-only). Signals are built from RAW 5-min bars; exits resolved on RAW 1-min bars.

## Raw data paths
- **5-MINUTE (entry/signal discovery):** `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2` — 1295 symbols
- **1-MINUTE (intrabar exit simulation):** `C:\TradingData\eqidv2\stocks_indicators_1min_eq` — 1286 symbols
- **DAILY:** `C:\TradingData\eqidv2\stocks_indicators_daily_eq` — 1041 symbols — ⚠️ **STALE** (ends ~2026-05-15); NOT used. ATR taken from recomputed 5-min ATR instead.

## Available date range & sessions
- 5-min full trading sessions found: **266** (2025-06-02 .. 2026-06-30)
- 1-min-resolvable sessions (≥350 1-min bars/day on ref liquid names): **265**, last = **2026-06-29**
- 2026-06-30 EXCLUDED — 1-min incomplete (~2 bars; today's open). Last resolvable session = 2026-06-29.

### Train/Test split (task convention: TRAIN=6wk before TEST, TEST=last ~2wk; FIT/VAL = TRAIN halves)
- **WARMUP** (12): 2026-04-13 .. 2026-04-29 (loaded for indicator warmup, NOT signalled)
- **FIT** (15): ['2026-04-30', '2026-05-04', '2026-05-05', '2026-05-06', '2026-05-07', '2026-05-08', '2026-05-11', '2026-05-12', '2026-05-13', '2026-05-14', '2026-05-15', '2026-05-18', '2026-05-19', '2026-05-20', '2026-05-21']
- **VAL** (15): ['2026-05-22', '2026-05-25', '2026-05-26', '2026-05-27', '2026-05-29', '2026-06-01', '2026-06-02', '2026-06-03', '2026-06-04', '2026-06-05', '2026-06-08', '2026-06-09', '2026-06-10', '2026-06-11', '2026-06-12']
- **TRAIN** (30) = FIT+VAL: 2026-04-30 .. 2026-06-12
- **TEST** (10): ['2026-06-15', '2026-06-16', '2026-06-17', '2026-06-18', '2026-06-19', '2026-06-22', '2026-06-23', '2026-06-24', '2026-06-25', '2026-06-29']
- (Repo standard `compute_windows` = TEST last 4wk / TRAIN 3mo; we use the task's 6wk/2wk + FIT-VAL split for honest nested validation.)

## Symbols / universe
- Liquid universe = symbols with median daily turnover ≥ Rs 25 cr (over last 45 sessions) AND a 1-min file: **491** qualify, capped to top **250** by turnover.
- Turnover from 5-min (Σ close·volume per session); daily store stale so liquidity derived from intraday.
- Top 10 by turnover: HDFCBANK, RELIANCE, ICICIBANK, BSE, SBIN, BHARTIARTL, INFY, MTARTECH, VEDL, AXISBANK
- Universe saved -> `results/universe.json`; sessions -> `results/sessions.json`

## Columns
### 5-min parquet (32 cols)
- indicator-like (20): RSI, ATR, EMA_20, EMA_50, EMA_200, 20_SMA, VWAP, CCI, MFI, OBV, MACD, MACD_Signal, MACD_Hist, Upper_Band, Lower_Band, ADX, Recent_High, Recent_Low, Stoch_%K, Stoch_%D
- other (12): date, open, high, low, close, volume, date_only, Intra_Change, Prev_Day_Close, Daily_Change, gap_filled, opening_snapshot
### 1-min parquet (30 cols): date, open, high, low, close, volume, RSI, ATR, EMA_20, EMA_50, EMA_200, 20_SMA, VWAP, CCI, MFI, OBV, MACD, MACD_Signal, MACD_Hist, Upper_Band, Lower_Band, Recent_High, Recent_Low, Stoch_%K, Stoch_%D, ADX, date_only, Intra_Change, Prev_Day_Close, Daily_Change

### Required columns for this study
- Needed: date, open, high, low, close, volume (5-min AND 1-min) — **all present**.
- VWAP/EMA/ATR/RSI/ADX/MACD-hist — **recomputed causally in-engine** (parquet `VWAP` is the known-stale global-cumsum column; we do NOT use it). No required column missing.

## Data-quality checks (sample of 60 universe symbols)
- `opening_snapshot` duplicate rows (09:15 == 09:20 first real bar) — **dropped**: 2273 rows across sample.
- exact duplicate 5-min timestamps after snapshot-drop: 0
- 5-min rows with NaN OHLC: 0
- halted/short sessions (<30 bars on a resolvable day): 0
- VWAP caveat: parquet `VWAP` stale/anchored → engine recomputes session-anchored VWAP (cumΣ typical·vol).
- 2026-06-22 appears in some 1-min files but not all 5-min files → handled by using the 5-min∩1-min resolvable session intersection.

## Resolver validation (no-lookahead + tie-break parity)
- My 1-min resolver vs repo `v17D_exit_resolver.resolve` on 229 sampled LONG signals @0.75/0.75: **229/229 identical outcomes (100.0%)**, 0 mismatch.
- Both use SL-first pessimism when SL & target are touched in the SAME 1-min bar; my resolver additionally counts those tie-break bars (reported per candidate in the search).
- Entry = next 1-min OPEN at floor(signal)+1min (≤+3min), 15 bps/leg adverse slippage; identical to `setup_train_test._entry`. No bar's own future is used to trigger it → no lookahead.