# `avwap_5min_ID_v11_backtesting.py` — Full Technical Documentation

> AVWAP Intraday (ID) 5-minute **v11** live-strategy backtester.
> The v11 backtester is a **live-parity replayer**: it reproduces the exact current
> v7 live signal chain on historical data so that backtested PnL is a faithful
> estimate of what the live system would have done.

---

## 1. Purpose & Design Philosophy

v11 exists to answer one question with maximum fidelity:

> *"If the current v7 live scanner + 1-minute entry engine + paper executor had run
> on this historical day (or date range), what trades would it have taken and what
> would the PnL have been?"*

It keeps the **v8 backtesting output format** (so dashboards/tools that already
consume v8/v6 outputs keep working), but the **candidate source and every filter
mirror the live v7 scanner**. The chain is:

```
historical 5-min bars
   │  (candidate_scan.v2._scan_day  →  raw candidates)
   ▼
v7 live scanner pipeline   →  add_live_ranker_scores
   │                          apply_v8_live_gate
   │                          v11 live overlay
   │                          apply_research_live_filters
   │                          _filter_entry_window (09:30–14:30)
   │                          one-ticker-per-day dedupe
   ▼
v7 1-minute entry engine    →  next 1-min open after the 5-min signal
   │                          time/setup guards + pre-entry momentum gate
   │                          Rs 20k margin × 5x sizing
   ▼
selected-strategy profile   →  honest production filter (default tier123_balanced)
   │                          + optional Tier123 balanced non-overlap add-on
   ▼
PAPER_TRADE_TRUE-style fill →  next-1m open + slippage, rebased SL/target
   ▼
v17D exit resolver          →  walk 1-min OHLC to 15:20 IST → TARGET / SL / EOD
   ▼
trades.csv + summary + v11_ID_* canonical outputs
```

**Module identity (from the header docstring):**
- Default output root: `C:\TradingData\eqidv2\outputs_ID_v11_5min`
- Default historical 5-minute source: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`

---

## 2. Module Dependencies

The backtester is a thin orchestration layer over the real live modules — it imports
and calls the live code directly rather than re-implementing it, which is what makes
it "parity":

| Import (alias) | Module | Role in v11 |
|---|---|---|
| `live_discovery` | `eqidv2_signal_discovery_v7_5min_id_persistent` | The **live v7 scanner**. Provides `add_live_ranker_scores`, `apply_v8_live_gate`, `apply_research_live_filters`, `_filter_entry_window`. |
| `v11_overlay` | `eqidv2_v11_live_overlay` | The **v11 live candidate overlay** (`apply_live_candidate_overlay`) — admits profile setups that failed the early quality gate. |
| `candidate_scan` | `avwap_5min_ID_v7_candidate_scan` | Raw candidate generation from 5-min bars (`_scan_day`, `_load_live_5m`, `candidates_to_dataframe`, `EXCLUDED_SETUPS`, `ALLOWED_SETUPS`). |
| `v5` | `avwap_5min_ID_v5_backtesting` | Legacy v5 layer (imported for compatibility / DATA_ROOT). |
| `v6` | `avwap_5min_ID_v6_backtesting` | Provides `SETUP_EXIT_RULES`, sizing constants, `_metrics`, `_summary_text`, `_normalise_trades`, `_net_pnl_rs`, `DATA_1M_DIR`. |
| `er` | `v17D_exit_resolver` | The **exit engine** — walks 1-min OHLC from entry to 15:20 cutoff and returns TARGET/SL/EOD. |

**Env var set at import:**
```python
os.environ.setdefault("EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS", "0")
```
The live scanner defaults to SHORT-only (`SHORT_FOCUS=1`), but the backtest replays
**both LONG and SHORT** unless the caller overrides.

---

## 3. Key Constants & Configuration Values

### 3.1 Paths
| Constant | Value |
|---|---|
| `OUT_ROOT` | `C:\TradingData\eqidv2\outputs_ID_v11_5min` |
| `LIVE_CANDIDATE_JSON_DIR` | `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json` |
| `LIVE_PAPER_DIR` | `C:\TradingData\eqidv2\live_signals` |
| `HISTORICAL_5M_DIR` / `DEFAULT_CANDIDATE_5M_DIR` | `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2` |
| `v6.DATA_1M_DIR` | `C:\TradingData\eqidv2\stocks_indicators_1min_eq` |

### 3.2 v7 entry-engine / sizing constants
| Constant | Value | Meaning |
|---|---|---|
| `V7_ENTRY_SEARCH_MAX_DELAY_MIN` | `3` | Max minutes after the 5-min signal to find the next 1-min entry bar. |
| `V7_SIGNAL_MARGIN_RS` | `20_000.0` | Margin per signal (Rs). |
| `V7_INTRADAY_LEVERAGE` | `5.0` | Intraday leverage. |
| `V7_SIGNAL_NOTIONAL_RS` | `100_000.0` | = margin × leverage; used to size quantity. |
| `V7_PAPER_SLIPPAGE_PCT` | `0.0005` | 5 bps slippage applied to the paper fill. |

### 3.3 v6 sizing / cost constants (inherited)
| Constant | Value |
|---|---|
| `v6.CAPITAL_PER_TRADE` | `10_000.0` |
| `v6.LEVERAGE` | `5.0` |
| `v6.EFFECTIVE_NOTIONAL` | `50_000.0` |
| `v6.DEFAULT_COST_BPS` | `16.0` |
| `v6.MIN_SL_PCT` | `0.70` |

> **Note on the two sizing models.** The v7 entry-engine resolution path
> (`_resolve_v7_entry_engine_signal`) uses the **v7 sizing** (Rs 20k margin × 5x =
> Rs 100k notional, price-only PnL, **no backtest cost**). The legacy
> `_resolve_trade_1m_entry` path (used by `live_parity` mode) uses the **v6 sizing**
> (Rs 10k × 5x = Rs 50k notional) and applies `cost_bps`. See §9.

---

## 4. The Setups & Exit Rules (`v6.SETUP_EXIT_RULES`)

Every tradable setup maps to a `(sl_pct, target_pct)` tuple. SL must be ≥
`MIN_SL_PCT` (0.70%). The full table:

| Setup | SL% | Tgt% | | Setup | SL% | Tgt% |
|---|---|---|---|---|---|---|
| A_MOD_BREAK_C1_HIGH | 0.70 | 1.00 | | E_GAP_HOLD_CONTINUATION_LONG | 0.80 | 1.20 |
| A_MOD_BREAK_C1_LOW | 0.70 | 1.50 | | E_GAP_HOLD_CONTINUATION_SHORT | 0.80 | 1.20 |
| A_MOD_CLOSE_CONTINUATION_BREAK | 0.70 | 1.50 | | E_OPENING_DRIVE_CONTINUATION_LONG | 0.75 | 1.00 |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | 0.70 | 0.90 | | E_OPENING_DRIVE_CONTINUATION_SHORT | 0.75 | 1.00 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 0.85 | 1.00 | | E_ORB_BREAKOUT_LONG | 0.80 | 1.20 |
| B_AVWAP_RECLAIM_REVERSAL | 0.70 | 1.50 | | E_ORB_BREAKOUT_SHORT | 0.80 | 1.20 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 0.70 | 1.50 | | E_ORB_RETEST_HOLD_LONG | 0.70 | 1.00 |
| B_HUGE_PULLBACK_HOLD_BREAK | 0.70 | 1.10 | | E_ORB_RETEST_HOLD_SHORT | 0.70 | 1.00 |
| B_HUGE_RED_FAILED_BOUNCE | 0.70 | 1.50 | | E_RS_FIRST_HOUR_BREAK_LONG | 0.80 | 1.20 |
| C_OR_BREAKDOWN | 0.70 | 1.30 | | E_RS_FIRST_HOUR_BREAK_SHORT | 0.80 | 1.20 |
| C_OR_BREAKOUT | 1.20 | 1.50 | | E_VWAP_BAND_FADE | 0.70 | 0.60 |
| D_AVWAP_LOSE_REVERSAL | 1.00 | 1.50 | | E_VWAP_LOSE_EARLY_SHORT | 0.70 | 1.00 |
| D_EMA20_BOUNCE | 0.70 | 1.50 | | E_VWAP_RECLAIM_EARLY_LONG | 0.70 | 1.00 |
| D_EMA20_REJECTION | 0.75 | 1.30 | | G_HIGHER_HIGH_BREAK | 0.90 | 1.50 |
| E_FAILED_OR_BREAKDOWN_TRAP_LONG | 0.75 | 1.00 | | G_LOWER_LOW_BREAK | 0.85 | 0.90 |
| E_FAILED_OR_BREAKOUT_TRAP_SHORT | 0.75 | 1.00 | | L_BB_SQUEEZE_LONG | 0.75 | 0.75 |
| | | | | L_DOUBLE_BOTTOM_VWAP | 0.70 | 0.80 |
| | | | | L_PRESSURE_BURST_VWAP | 1.10 | 0.90 |
| | | | | L_TREND_PULLBACK | 0.70 | 0.90 |
| | | | | S_BB_SQUEEZE_SHORT | 1.00 | 1.50 |
| | | | | S_LIQUIDITY_SWEEP_REVERSAL | 0.70 | 1.50 |
| | | | | S_MACD_HIST_FLIP | 0.70 | 1.50 |

**Setup family prefixes:** `A_` modular breaks/pullbacks, `B_` huge-bar reclaim/reversal,
`C_` opening-range, `D_` EMA/AVWAP bounce-reject, `E_` ORB/VWAP/gap event setups,
`G_` higher-high/lower-low, `L_` long squeeze/trend, `S_` short squeeze/sweep,
`T_`/`MR_` Tier123 add-on setups (registered dynamically — see §12).

**Exit-override additions:** At import, `v6.SETUP_EXIT_RULES.update(TIER123_BALANCED_EXIT_RULES)`
adds the three Tier123 setups:
```python
T_TREND_DAY_EMA_STAIR_SHORT          : (0.70, 1.00)
MR_CONTROLLED_VWAP_EXTREME_FADE_LONG : (0.70, 0.80)
MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT: (0.70, 0.80)
```

**`EXCLUDED_SETUPS`** = `candidate_scan.EXCLUDED_SETUPS` — never traded (filtered out
everywhere). **`ENTRY_SHADOW_SETUPS`** = `frozenset({"C_OR_BREAKDOWN"})` — shadow
setups that are scanned but permanently blocked at the entry engine (never executable
live).

---

## 5. Run Modes (CLI `--mode`)

`main()` dispatches to one of five modes. Default is `historical_full_day`.

| Mode | Function | Candidate source | Use case |
|---|---|---|---|
| `live_parity` | `_run_live_parity` | Real saved live JSON snapshots (`raw_candidate_tickers_*.json`) | Re-resolve actual live candidates with 1-min exits — tightest parity. |
| `v7_live_paper_replay` | `_run_v7_live_paper_replay` | Real `paper_trades_*_id_5min_v7.csv` | Re-resolve **actual paper trades** taken by the live executor. |
| `historical_full_day` | `_run_historical_full_day` | Regenerated from historical 5-min bars for **one date** | Single-day what-if backtest. |
| `historical_all_available` | `_run_historical_all_available` | Regenerated for **all dates** in a range | Multi-day / full-history backtest. |
| `historical_cached_all_setups` | `_run_historical_cached_all_setups` | Cached `*_entry_engine_signals.csv` | Fast re-run of profile/Tier123 logic without rescanning bars. |

The **two "regeneration" modes** (`historical_full_day`, `historical_all_available`)
are the workhorses: they rebuild candidates from raw bars and run the entire live
pipeline. `historical_cached_all_setups` short-circuits the expensive scan by reusing
previously-saved entry-engine signals.

---

## 6. Core Pipeline — `_apply_v7_live_strategy()`

This is the heart of the live-parity replay. Given a raw candidate DataFrame plus a
`date_hint`, it walks **day → 5-minute slot** and reproduces the live scanner's
per-slot processing exactly. For each `(day, slot)`:

1. **`add_live_ranker_scores(raw_slot, day)`** → `ranked`
   Live ML/heuristic ranker assigns `live_rank_score` / `quality_score`.

2. **`apply_v8_live_gate(ranked)`** → `gated, gate_stats`
   The production quality gate (the standard live admission filter).

3. **`_apply_ab_probation_gate(ranked, …)`** → `ab_accepted, ab_stats`
   Optional explicit A/B probation admission (see §11). Off by default.

4. **`v11_overlay.apply_live_candidate_overlay(ranked, profile, ab_gate_profile)`**
   → `overlay_accepted`
   The v11 overlay admits `V11_PROFILE_SETUP_UNIVERSE` setups (e.g.
   `E_VWAP_LOSE_EARLY_SHORT`) that **failed the early quality gate** but pass the
   selected-strategy profile rules. Mirrors the live overlay that runs alongside the
   v8 gate.

5. **Merge before research filters** (live parity fix):
   ```python
   pre_research = concat([gated, overlay_accepted]) → dedupe on candidate_id
   ```
   In the live scanner `merge_v7_and_v11_candidates` runs **before**
   `apply_research_live_filters`, so all sources pass through anti-chase, SHORT_FOCUS
   and probation checks **exactly once**.

6. **`apply_research_live_filters(pre_research, day)`** → `accepted, rejected, research_stats`
   The research-layer live filters.

7. **`_filter_entry_window(accepted)`** (live parity fix) — drops candidates outside
   the **09:30–14:30** entry window. The live scanner calls this after research
   filters (scanner line 1539); the original backtester omitted it.

8. **Combine + dedupe** the accepted set with any AB-gate acceptances on `candidate_id`.

After all slots, **`_live_like_daily_dedupe`** enforces the live entry-engine guard:
**one candidate per `candidate_id` and one ticker per day** (first by live order). The
result, `live_like_candidates`, is what the live system would have surfaced.

**Returned dict keys:** `ranked_raw_candidates`, `v8_gated_candidates`,
`research_rejected_candidates`, `pre_dedupe_live_candidates`, `live_like_candidates`,
`slot_audit`, `stats`.

`slot_audit` records per-slot counts and merged gate/ab/research stats; `stats`
aggregates them across the run.

---

## 7. Raw Candidate Generation (historical modes)

### 7.1 Per-ticker, per-day scan — `_scan_one_ticker_day_candidates()`
Run inside worker processes. For one ticker/day:
1. Load the ticker's live 5-min parquet (`candidate_scan._load_live_5m`) and prepare it
   (`candidate_scan.v2._prepare_5m`).
2. Slice to the requested day and `[start_min, end_min]` slot window.
3. Append a **synthetic successor bar** (`_append_synthetic_successor`) so the last
   real slot can be scanned.
4. Run `candidate_scan.v2._scan_day(scan_df, ticker, market_ctx)`; if
   `EARLY_MODE_ENABLE`, also run `_scan_early_slot_candidates` per slot.
5. Keep only candidates whose signal timestamp lands on a real slot. Drop
   `EXCLUDED_SETUPS` (unless `include_ab_excluded` and the setup is an A/B probation
   setup). If `FILTER_TO_V8_EXIT_SETUPS`, keep only `ALLOWED_SETUPS`.
6. Convert to a DataFrame via `candidate_scan.candidates_to_dataframe`.

### 7.2 Day-wide orchestration — `_scan_historical_full_day_candidates()`
- Builds the slot range with `_slot_range_for_day` (5-min spacing from `start_time`
  to `end_time`).
- Loads the universe (`candidate_scan.v2._load_universe`).
- Fans the per-ticker payloads across a `ProcessPoolExecutor`
  (`--workers`, default 16; `initializer=_v11_worker_init`), or runs single-process if
  `workers <= 1`.
- De-dupes the combined raw frame (`_dedupe_candidate_frame`) and writes one
  `raw_candidate_tickers_<YYYYMMDD>_<HHMM>.json` snapshot per slot.

**Worker init (`_v11_worker_init`):** points `candidate_scan.LIVE_5M_DIR` /
`v2.DATA_ROOT_5M` at the data root, initializes the v2 worker with
`ENABLE_NOISY_ADVANCED_SHORTS=True`, `ENABLE_NATIVE_V2_MINED_FILTER=False`, and caches
the market context in `_V11_WORKER_MARKET_CTX`.

---

## 8. The v7 1-Minute Entry Engine Simulation

`_build_v7_entry_engine_signals(candidates)` → `(selected, raw_entries, rejects)`.
This faithfully mirrors `eqidv2_entry_engine_1min_v5_id.py` (the live 1-minute engine)
without hitting Kite — it uses historical 1-minute OHLC instead.

### 8.1 Raw row build — `_v7_entry_engine_raw_rows()`
Candidates are sorted by `(day, signal_ts, input_order)`. Then per candidate:

**Entry-engine guards (parity with the live engine, 2026-06-10):**
| Guard | Rule |
|---|---|
| Required fields | ticker present, side ∈ {LONG,SHORT}, valid signal time, setup ∈ `SETUP_EXIT_RULES`. |
| Shadow setup | `C_OR_BREAKDOWN` permanently blocked. |
| `E_VWAP_LOSE_EARLY_SHORT` | Blocked if signal time < **09:45** IST (`ENTRY_E_VWAP_EARLY_SHORT_MIN_SLOT`). |
| `A_MOD_BREAK_C1_HIGH` | Blocked if signal time ≥ **11:10** IST (`ENTRY_A_MOD_C1_HIGH_MAX_SLOT`); additionally only the **top-2 per (day, slot)** ranked by `vwap_dist_atr` desc survive (`ENTRY_A_MOD_C1_HIGH_TOP_N=2`). |
| `C_OR_BREAKOUT` | Requires `vwap_dist_atr ≥ 2.0` (`ENTRY_C_OR_BREAKOUT_MIN_VWAP_DIST_ATR`), `atr_pct ≤ 0.010` (`ENTRY_C_OR_BREAKOUT_MAX_ATR_PCT`), and signal time in **09:55–10:40** IST window. |

**1-minute entry lookup** — `_first_1m_entry(bars, signal_ts, max_delay_minutes=3)`:
takes the **first 1-minute bar strictly after the 5-min signal**, within 3 minutes,
and uses its **open** as the raw signal entry price (rounded to 2dp).

**Stop/target price construction** (from `SETUP_EXIT_RULES`):
- LONG: `stop = entry × (1 − sl%/100)`, `target = entry × (1 + tgt%/100)`
- SHORT: `stop = entry × (1 + sl%/100)`, `target = entry × (1 − tgt%/100)`

**Pre-entry momentum gate** (see §10) — applied per setup after entry price is known;
missing features ⇒ block.

**Sizing:** `quantity = max(1, int(V7_SIGNAL_NOTIONAL_RS / entry_price))` (Rs 100k /
price). A `signal_id` is computed as an md5 of `ticker|side|bar_time|setup`.

The function returns accepted rows plus a full `rejects` DataFrame with explicit
`reject_reason` for every dropped candidate (great for parity auditing).

### 8.2 Selection / dedupe — `_select_v7_entry_engine_signals()`
- Per `(day, slot)`: sort by score desc, drop duplicate `(bar_time, ticker)`.
- Per **day**: greedily keep the best score per ticker, enforcing **one ticker per day**
  across all slots (mirrors the live intraday ticker guard).

---

## 9. Entry Fill & Exit Resolution

### 9.1 v7 entry-engine resolution — `_resolve_v7_entry_engine_signal()`
Used by all historical modes. Two fill models (`--entry_fill_model`):

- **`ltp_on_signal_1m_open`** (default — mirrors the live `ltp_on_signal` bat mode):
  - LONG fill = `signal_entry × (1 + 0.0005)`; SHORT fill = `signal_entry × (1 − 0.0005)`.
  - Stop/target are **rebased** to the slipped entry by the original multipliers.
- **`signal_bar`**: fill exactly at the 1-min open, no slippage.

**Selected-strategy exit override** (`SELECTED_STRATEGY_EXIT_OVERRIDES`): for the
relevant profile, `E_ORB_BREAKOUT_SHORT` is forced to `(0.80, 1.50)` SL/target and
stop/target are recomputed from the entry. Otherwise SL/target % are derived from the
constructed stop/target prices (`_side_exit_pcts`).

**Exit walk** — `er.resolve(...)` (see §9.3). PnL is **price-only**:
`pnl_rs = (exit − entry) × qty` for LONG, `(entry − exit) × qty` for SHORT.
**No backtest cost** is applied in this path (`v6_cost_rs = 0.0`); sizing fields record
`capital_per_trade_rs = 20_000`, `leverage = 5`, `notional = entry × qty`.

`_resolve_v7_entry_engine_signals()` loops all signals, prints progress every 250,
sorts by `(trade_date, entry_time, ticker)`.

### 9.2 Legacy v6 resolution — `_resolve_trade_1m_entry()`
Used by `live_parity` mode (and `_resolve_live_parity_candidates`). Looks up the
next-1m open entry, resolves via `er.resolve`, and computes **net PnL with costs** via
`v6._net_pnl_rs(pnl_pct, outcome, cost_bps)` on the **Rs 50k** effective notional. SL
exits incur extra bps (`STOP_EXTRA_BPS`). This path honors `--cost_bps` (default 16).

### 9.3 The exit engine — `v17D_exit_resolver.resolve()`
Walks 1-minute OHLC bars from `entry_time` to the **EOD cutoff 15:20 IST**:
- LONG: `sl_price = entry×(1−sl%)`, `tgt_price = entry×(1+tgt%)`; SL hit when `low ≤ sl`,
  target hit when `high ≥ tgt`. SHORT is mirrored.
- Bar-by-bar, first hit wins. **If both SL and target hit in the same bar → pessimistic
  SL** (assume stop first).
- If neither hits by cutoff → **EOD** exit at the last bar's close.
- Returns `ResolutionResult(outcome, exit_price, exit_time_ist, pnl_pct_price, bars_held)`.
  `pnl_pct_price` is **price-only, before leverage and before costs**.

---

## 10. Pre-Entry Momentum Gate

`PRE_ENTRY_MOMENTUM_SETUP_GATES` (version `v7_pre_entry_momentum_2026_06_04_t_probation`)
mirrors the live entry engine's per-setup momentum gate. Each gate is a tuple of
`(feature, op, threshold)` triples; **all must pass**. Missing/NaN features ⇒ **block**
(`PRE_ENTRY_MOMENTUM_MISSING_ACTION = "block"`).

| Setup | Gate terms (all required) |
|---|---|
| B_AVWAP_RECLAIM_REVERSAL | `pre_entry_momentum_score ≤ 64.7678` |
| C_OR_BREAKOUT | `sig5_adx_calc ≥ 25`, `sig5_rsi_dir ≥ 60`, `sig5_vol_ratio20 ≥ 1.5`, `pre2_mom_r ≥ −0.05` |
| D_EMA20_BOUNCE | `pre3_range_r ≥ 0.2923`, `pre_entry_momentum_score ≤ 78.3448` |
| D_EMA20_REJECTION | `pre10_mom_r ≤ 0.1566`, `pre5_mom_r ≥ 0.1249`, `sig5_adx_calc ≥ 20` |
| E_ORB_BREAKOUT_LONG | `pre15_vol_ratio20 ≤ 1.0830`, `pre1_adx ≥ 42.3138` |
| E_ORB_BREAKOUT_SHORT | `pre10_dir_count ≥ 5`, `pre5_vol_ratio20 ≥ 1.6556` |
| E_VWAP_LOSE_EARLY_SHORT | `sig5_vol_ratio20 ≥ 1.5643`, `pre3_body_sum_r ≤ 0.7975` |
| G_HIGHER_HIGH_BREAK | `pre3_close_pos ≤ 0.9854`, `sig5_rsi_dir ≤ 67.878` |
| L_TREND_PULLBACK | `pre_entry_momentum_score ≥ 73.021`, `pre2_mom_r ≥ 0.2339` |
| T_TREND_DAY_EMA_STAIR_SHORT | `pre3_close_pos ≥ 0.6625`, `pre5_dir_count ≤ 3`, `pre1_adx ≤ 31`, `pre5_range_r ≥ 0.35` |

### 10.1 Feature engineering — `_pre_entry_momentum_features_v11()`
Reconstructs the live engine's `_pre_entry_momentum_features` / `_add_window_features`
on historical parquet:
- Loads the ticker's 1-min bars; slices to **same-day bars strictly before the entry
  minute**. Requires ≥ 16 pre-entry bars, else returns a "missing" reason (→ block).
- `risk = |entry − stop|`; direction `d = +1` (LONG) / `−1` (SHORT).
- **`preN_mom_r`** for N∈{1,2,3,5,10,15}: directional close-to-close momentum / risk.
- **Window features** for N∈{3,5,10,15}: `preN_close_pos` (position in window
  high–low), `preN_dir_count` (# directional bars), `preN_body_sum_r`,
  `preN_range_r` (window range / risk), `preN_vol_ratio20` (window mean vol ÷ base
  vol, where base = mean of 20 bars before the last bar).
- **`pre1_*`** single-bar features; `pre1_adx`, `pre1_rsi_dir` from 1-min ADX/RSI.
- **`pre_entry_momentum_score`** =
  `50 + 25·tanh(2·mom) + 15·(pos−0.5) + 10·(vol_component−0.33)` where `mom` is mean of
  `{pre1_body_r, pre3_mom_r, pre5_mom_r}`, `pos` mean of `{pre3_close_pos,
  pre5_close_pos}`, `vol_component = min(pre3_vol_ratio20, 3)/3`.
- **`sig5_*`** features from the signal's 5-min indicator bar (`_load_5m_ind_bars`):
  `sig5_body_r`, `sig5_range_r`, `sig5_close_pos`, `sig5_adx_calc`, `sig5_rsi_dir`,
  `sig5_vol_ratio20`.

`_eval_pre_momentum_terms()` evaluates the gate; any NaN/failing term fails the gate.

---

## 11. A/B Probation Gate (`--ab_gate_profile`)

Optional explicit admission of A_/B_ probation setups, separate from the production
gate. Choices: `off` (default) or `quality_top_slot`.

`_apply_ab_probation_gate(ranked_slot, profile, min_quality, max_per_side, max_per_slot,
allowed_setups)`:
- Filters to `allowed_setups` (depends on the selected profile — see
  `_ab_gate_setups_for_selected_profile`).
- Keeps rows with `quality_score ≥ min_quality` (default **250.0**).
- Per side keeps top `max_per_side` (default 1) by `(quality, ranker)`, then overall
  top `max_per_slot` (default 2) per slot.
- Tags accepted rows: `v8_live_gate_status="AB_PROBATION_PASSED"`,
  `research_live_filter_status="AB_PROBATION_BYPASS"`, version `v11_ab_probe_v1`.

**Allowed-setup sets** (driven by the selected profile):
- `AB_PROBATION_SETUPS` — all `A_`/`B_` setups in `SETUP_EXIT_RULES`.
- `AB_GOOD_RESULT_SETUPS` — `A_MOD_BREAK_C1_LOW`, `A_PULLBACK_C2_THEN_BREAK_C2_LOW`,
  `B_HUGE_C1_CLOSE_RECLAIM_BREAK`.
- `AB_FILTERED_RELAXED_SETUPS` — `A_PULLBACK_C2_THEN_BREAK_C2_LOW`,
  `B_HUGE_C1_CLOSE_RECLAIM_BREAK`.
- `AB_MAX_PNL_LOW_VALID_SETUPS` — the six max-PnL A/B setups.

**Auto-enable:** in `main()`, if the selected profile is in
`AB_SELECTED_STRATEGY_PROFILES` and `--ab_gate_profile` is still `off`, it is forced to
`quality_top_slot` (so the A/B setups the profile expects can actually be admitted).

---

## 12. Selected-Strategy Profiles (`--selected_strategy_profile`)

After the v7 entry engine, a **post-entry profile filter** keeps only the honest /
production-validated setups + rules. `_selected_strategy_mask()` builds a boolean mask
per row; `_apply_selected_strategy_profile()` splits into accepted/rejected and tags
`v11_selected_strategy_rule`. Default profile is **`tier123_balanced`**
(`production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced`).

### 12.1 Profile ladder
| Profile | What it adds on top of the core |
|---|---|
| `none` | Keeps **all** raw v11/v7 entry-engine signals (no profile filtering). |
| `production_core` | The honest holdout-improved core (see core rules below). |
| `production_core_tiny` | + tiny BEAR-only `E_VWAP_LOSE_EARLY_SHORT`. |
| `production_core_ab_probe` | + all A/B probation setups. |
| `ab_only_probe` | **Only** A/B probation setups. |
| `production_core_ab_good_probe` | + the positive A/B watchlist setups. |
| `ab_good_only_probe` | **Only** the A/B watchlist setups. |
| `production_core_ab_filtered_relaxed` | + filtered B_HUGE / A_PULLBACK rules. |
| `ab_filtered_relaxed_only` | **Only** those filtered A/B rules. |
| `production_core_ab_max_pnl_low_valid` | + the 2026-06-02 max-PnL A/B add-ons. |
| `…_max_pnl_low_valid_residual_overlay` | + residual late-D_EMA20 / morning-S_BB overlays. |
| `…_residual_overlay_tier123_balanced` **(default)** | residual-overlay profile **protected** + Tier123 balanced non-overlap add-on. |

### 12.2 Production-core rules (always applied for any `production_core*` profile)
- **C_OR_BREAKOUT** — kept broadly (passed honest holdout).
- **D_EMA20_BOUNCE** — `(vol_ratio ≤ 1.60 OR vwap_dist_atr ≥ −0.39) AND signal_minute ≤ 705`.
- **E_ORB_BREAKOUT_LONG** — `v7_signal_notional_rs ≥ 100_000`.
- **E_ORB_BREAKOUT_SHORT** — `market_ret_pct ≥ −0.63 AND quality_score ≥ 97.9 AND
  upper_wick_pct ≤ 0.015`; exit overridden to 0.80/1.50.
- **L_BB_SQUEEZE_LONG** — `(market_abs_ret_pct ≤ 0.74 OR vol_ratio ≤ 3.0) AND
  ranker_score ≥ 0.73`.

### 12.3 Max-PnL low-valid add-ons (key thresholds)
| Setup | Rule |
|---|---|
| S_BB_SQUEEZE_SHORT | `market_ret_pct ≥ 0.54 OR v7_signal_notional_rs ≥ 100_000` |
| B_AVWAP_RECLAIM_REVERSAL | `vwap_dist_atr ≥ 0.60` |
| A_MOD_CLOSE_CONTINUATION_BREAK | `signal_range_pct ≥ 2.2 OR notional ≤ 100_000` |
| A_MOD_BREAK_C1_LOW | `|rs_pct| ≥ 9.2 AND vol_ratio ≥ 1.80` |
| A_MOD_BREAK_C1_HIGH | `rs_pct ≥ 2.0 AND atr_pct ≤ 0.006 AND signal_minute ≤ 670` (11:10 IST)* |
| D_EMA20_REJECTION | `body_pct ≥ 0.89 AND ranker_score ≥ 0.39` |
| E_VWAP_BAND_FADE | `atr_pct ≥ 0.0059 AND signal_minute ≤ 690` |
| E_VWAP_LOSE_EARLY_SHORT | `vwap_dist_atr ≥ −1.25` |

\* The `A_MOD_BREAK_C1_HIGH` gate was **replaced 2026-06-09** (old `market_abs ≤ 0.26 AND
vol_ratio ≤ 2.0` rejected genuine winners). New gate validated across 10 sessions: 32
trades, PF 3.25. Mirrors `eqidv2_v11_live_overlay.py`.

### 12.4 Residual-overlay add-ons
- **Late-session D_EMA20_REJECTION** — `780 < signal_minute ≤ 825 AND (body_pct ≥ 0.93
  OR wick_skew_pct ≤ −0.065)`.
- **Residual morning S_BB_SQUEEZE_SHORT** — `signal_minute ≤ 705`.

### 12.5 Feature derivation — `_selected_strategy_features()`
Adds derived columns used by the masks: `signal_minute` (from the best available signal
timestamp), `upper_wick_pct` / `lower_wick_pct` / `wick_skew_pct`, `signal_range_pct`,
and `market_abs_ret_pct = |market_ret_pct|` — all computed from the signal-bar OHLC.

---

## 13. Tier123 Balanced Non-Overlap Add-On

Only active when the profile is the default `tier123_balanced`. Runs **after** the base
book is resolved and appends **non-overlapping** trades (same ticker+date already in the
base book is rejected). Three add-on setups (`TIER123_BALANCED_SETUPS`):

| Setup | Side | Core trigger (in `_tier123_scan_ticker_dates`) |
|---|---|---|
| `T_TREND_DAY_EMA_STAIR_SHORT` | SHORT | Late bearish/trend day: `regime∈{BEAR,TREND}`, EMA20 slope < 0, `close < ema20 ≤ ema50`, retest of EMA20, short structure, below VWAP, `rs_pct ≤ −0.05`, `minute ≤ 840`. |
| `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG` | LONG | `minute ≥ 660`, controlled regime/ADX≤22, `vwap_dist_atr ≤ −2.0`, `lower_wick ≥ 0.35`, close > prior high, RSI≤38/Stoch≤35. |
| `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT` | SHORT | `minute ≥ 660`, controlled, `vwap_dist_atr ≥ 2.0`, `upper_wick ≥ 0.35`, close < prior low, RSI≥62/Stoch≥65. |

**Profile gate** (applied in `_selected_strategy_mask`):
- T-stair short: `market_ret_pct ≤ −0.39 AND 780 ≤ signal_minute ≤ 840`.
- Fade long: `vol_ratio ≥ 2.47`.
- Fade short: `vol_ratio ≤ 1.70 AND quality_score ≥ 60.7`.

**Scan internals:** `_tier123_read_5m` / `_tier123_prepare_5m` recompute VWAP/ATR-based
features (`vol_ratio`, `atr_pct`, `vwap_dist_atr`, wicks, `close_loc`, `body_pct`) and
prev-day levels. `_tier123_market_context` derives per-bar `market_ret_pct` + `regime`
(BULL/BEAR/TREND/NEUTRAL) from NIFTY. `_tier123_quality` scores each candidate.
`_tier123_probe_gate` keeps the best score per `(setup, ticker, day)` and caps at
`TIER123_MAX_RAW_PER_SETUP = 2500` per setup. Universe is NSE-futures-only
(`filtered_stocks_NSE_futures_only`), excluding NIFTY/BEES tickers.

`_apply_tier123_balanced_addon()` runs the full sub-pipeline (scan → profile filter →
entry engine → resolve), then `non_overlap = addon ∉ base_keys` and
`combined = sort(base + non_overlap)`.

---

## 14. Sizing, PnL & Cost Models (summary)

| Path | Notional | Slippage | Cost | PnL |
|---|---|---|---|---|
| v7 entry-engine resolution (historical modes) | Rs 100k (20k × 5x), qty = floor(100k/price) | 5 bps on fill (default model) | **none** | price-only `(exit−entry)×qty` |
| Legacy v6 resolution (`live_parity`) | Rs 50k (10k × 5x) | none | `cost_bps` (16) + SL extra bps | `v6._net_pnl_rs` net |
| `v7_live_paper_replay` | actual live `quantity` | n/a (actual fills) | none | price-only on actual entry |

All exits resolve on **1-minute OHLC to 15:20 IST** (EOD cutoff) via the v17D resolver.

---

## 15. Output Files

Written under `--out` (default `OUT_ROOT`). Common files via `_write_outputs()`:

| File | Contents |
|---|---|
| `trades.csv` | Full resolved trade rows (internal schema). |
| `daily.csv` | Per-day net PnL, cumulative, drawdown. |
| `by_setup.csv` | Per side/setup: trades, win/target/SL/EOD rates, PnL, SL/target %. |
| `setup_exit_rules.csv` | The active `SETUP_EXIT_RULES` table. |
| `entry_timing_audit.csv` | Entry-timing fields (signal vs entry vs old-v7 entry, delay, outcome, PnL). |
| `summary.txt` | Human-readable summary (re-labelled "AVWAP ID 5-min v11 backtest"). |
| `v11_ID_trades.csv` | **Canonical** schema (date, symbol, side, setup_name, entry/exit, pnl, filters…). |
| `v11_ID_daily_summary.csv`, `v11_ID_setup_summary.csv`, `v11_ID_summary.csv` | Canonical daily/setup/summary outputs. |
| `inputs.txt` | Full run configuration for reproducibility. |
| `<mode>_*` CSVs | Per-stage dumps: raw candidates, ranked, v8-gated, research-rejected, pre-dedupe, live-like, entry-engine raw/rejects/signals, selected-strategy signals/rejects, slot audit, pipeline stats. |
| `generated_candidate_snapshots/*.json` | Per-slot raw + gated candidate snapshots (parity with live JSON). |

`_write_empty_outputs()` writes the same file set with empty frames + a no-trade reason
when nothing survives.

**`--parity-debug`** writes `v11_ID_parity_debug.csv` — a per-candidate audit trail
across every pipeline stage (`ranked → v8_gate → research_filter → entry_engine →
accepted/rejected` with reasons), for cross-checking against live session logs.

`_build_v11_id_trades()` maps the internal frame to the canonical output schema
(date, symbol, side, setup_name, entry_time, entry_price, entry_source_timeframe =
`5min_signal_1min_open`, exit_time, exit_price, exit_reason, pnl, pnl_pct, quantity,
reason, filters_passed/failed, source files).

---

## 16. CLI Arguments

| Arg | Default | Purpose |
|---|---|---|
| `--mode` | `historical_full_day` | One of the 5 modes (§5). |
| `--out` | `OUT_ROOT` | Output directory. |
| `--cached_all_setups_dir` | `…outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250` | Cached signals source for the cached mode. |
| `--live_candidate_json_dir` | `LIVE_CANDIDATE_JSON_DIR` | Live JSON snapshots (live_parity). |
| `--live_paper_dir` | `LIVE_PAPER_DIR` | Live paper-trade CSVs. |
| `--candidate_5m_dir` | `DEFAULT_CANDIDATE_5M_DIR` | Primary 5-min data root. |
| `--fallback_candidate_5m_dir` | `HISTORICAL_5M_DIR` | Fallback root when a date isn't in the primary. |
| `--live_date` / `--historical_date` | `""` | Single-day selectors. |
| `--start_date` / `--end_date` | `""` | Date range (all_available / paper replay). |
| `--start_time` / `--end_time` | `09:15` / `15:00` | Slot scan window. |
| `--workers` | `16` | Scan process-pool size. |
| `--cost_bps` | `16.0` | Cost (legacy/live_parity path only). |
| `--entry_fill_model` | `ltp_on_signal_1m_open` | `ltp_on_signal_1m_open` (slippage + rebased SL/target) or `signal_bar`. |
| `--selected_strategy_profile` | `tier123_balanced` | Post-entry profile (§12). |
| `--ab_gate_profile` | `off` | `off` / `quality_top_slot` A/B admission (§11). |
| `--ab_gate_min_quality` | `250.0` | Min quality for the A/B gate. |
| `--ab_gate_max_per_side` | `1` | A/B cap per side per slot. |
| `--ab_gate_max_per_slot` | `2` | A/B cap per slot (both sides). |
| `--parity-debug` | off | Write `v11_ID_parity_debug.csv`. |

`main()` normalizes the profile / ab_gate_profile, auto-enables `quality_top_slot` when
needed, and dispatches to the mode runner. Exit code is `main()`'s return.

---

## 17. End-to-End Flow (historical_full_day, default profile)

```
1. _run_historical_full_day(args)
2.   resolve data_root (primary, else fallback) for the date
3.   _scan_historical_full_day_candidates → raw candidates + per-slot JSON snapshots
4.   _apply_v7_live_strategy(raw, date, …)
        per (day, slot):
          add_live_ranker_scores → apply_v8_live_gate
          + AB probation gate (if enabled)
          + v11 overlay
          merge(gated, overlay) → apply_research_live_filters → _filter_entry_window
        → daily one-ticker dedupe → live_like_candidates
5.   _build_v7_entry_engine_signals(pre_dedupe_live_candidates)
        guards (shadow / E_VWAP early / A_MOD_C1_HIGH top-2 & 11:10 / C_OR window)
        next-1m open entry, stop/target, pre-entry momentum gate, Rs100k sizing
        → entry_engine_signals (best setup/ticker/slot, one ticker/day)
6.   _apply_selected_strategy_profile(entry_signals, profile) → selected signals
7.   _resolve_v7_entry_engine_signals(selected, fill_model, profile)
        paper fill (slippage) → er.resolve (1-min walk to 15:20) → price-only PnL
8.   _apply_tier123_balanced_addon → append non-overlapping Tier123 trades
9.   _write_outputs → trades.csv / daily / by_setup / summary / v11_ID_* / inputs.txt
```

---

## 18. Quick Reference — Important Time/Threshold Constants

| Constant | Value | Where |
|---|---|---|
| Entry search max delay | 3 min | `V7_ENTRY_SEARCH_MAX_DELAY_MIN` |
| E_VWAP_LOSE_EARLY_SHORT earliest | 09:45 | `ENTRY_E_VWAP_EARLY_SHORT_MIN_SLOT` |
| A_MOD_BREAK_C1_HIGH latest | 11:10 | `ENTRY_A_MOD_C1_HIGH_MAX_SLOT` |
| A_MOD_BREAK_C1_HIGH per-slot cap | top-2 by `vwap_dist_atr` | `ENTRY_A_MOD_C1_HIGH_TOP_N` |
| C_OR_BREAKOUT min VWAP dist | 2.0 ATR | `ENTRY_C_OR_BREAKOUT_MIN_VWAP_DIST_ATR` |
| C_OR_BREAKOUT max ATR% | 0.010 | `ENTRY_C_OR_BREAKOUT_MAX_ATR_PCT` |
| C_OR_BREAKOUT window | 09:55–10:40 | `ENTRY_C_OR_BREAKOUT_MIN/MAX_SIGNAL_TIME` |
| Entry window (research) | 09:30–14:30 | `live_discovery._filter_entry_window` |
| EOD exit cutoff | 15:20 IST | `er.EOD_CUTOFF_HOUR/MIN` |
| Paper slippage | 5 bps | `V7_PAPER_SLIPPAGE_PCT` |
| Default cost (legacy path) | 16 bps | `v6.DEFAULT_COST_BPS` |
| Min SL | 0.70% | `v6.MIN_SL_PCT` |

---

*Generated from `avwap_5min_ID_v11_backtesting.py` (4037 lines) and its direct
dependencies `avwap_5min_ID_v6_backtesting.py`, `v17D_exit_resolver.py`,
`avwap_5min_ID_v7_candidate_scan.py`, and
`eqidv2_signal_discovery_v7_5min_id_persistent.py`.*
