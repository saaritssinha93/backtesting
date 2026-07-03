# SETUP_FAMILY_IDEAS — 10 FAST-MOMENTUM LONG families (tight ~0.75% symmetric)

> Namespaced engine: `claude_engine/`. (The parent folder holds a parallel
> `tight_raw_long_discovery.py` run; this file is independent of it.)

All triggers fire on **raw 5-min bars** (close-stamped: bar @T covers [T−5m, T]), use only
information known at the bar close, and enter at the **next 1-min open** (no lookahead).
Exit = tight bracket resolved on 1-min. Every family carries the same *candidate filter/guard
menu* below; the FIT/VAL search turns them on one logical group at a time (2 coordinate rounds).

Raw trigger code: `scripts/lib_long_disc.py :: family_triggers`. Causal features: `compute_features`.

## Shared candidate filter / guard menu (driven by RAW_DATA_LONG_EDGE_STUDY.md)
- **Bracket** (headline): 0.75/0.75 anchor; grid {0.50/0.50, 0.60/0.60, 0.75/1.00, 0.50/0.75};
  variants {time-exit 12/18 bars, break-even after +0.40%}. All 1-min resolved, SL-first tie.
- **Time guard** (strongest edge, 35pp): `max_minute` (morning-only). Early session follows through.
- **Volatility floor** (30pp): `atr_pct >=` — need room to reach +0.75% fast; reject dead-low ATR.
- **Momentum** (~15pp): `mom2_pct>=` / `mom3_pct>=` / `adx>=` / `macd_hist>=0` — continuation.
- **Trend**: `above_vwap`, `above_ema20`, `ema20_slope>=0` (most families bake these into the trigger).
- **Overextension guard**: `vwap_dist_atr <=` (≥3 ATR above VWAP is worse), `upper_wick<=`, `rsi<=`.
- **Volume**: edge study shows high `vol_ratio` is *inverted* (spikes = exhaustion); volume filters
  are tested but HURT — kept off.
- **Guards**: `min/max_minute`, `top_n` per (day,slot) by rank feature, `max_per_sym_day`,
  `max_book_concurrent` (≤20, live cap). Selection enforces **no overlapping position per symbol**.

Per-family raw target-first% (P(+0.75% before −0.75%), TRAIN) from the edge study in ( ).

---
### F1 — LONG VWAP Reclaim Momentum  (35.5%)
- **Trigger:** green bar, `close>VWAP`, prior close ≤ VWAP (reclaim), `close_loc≥0.5`.

### F2 — LONG Pressure Burst Breakout  (33.5%)
- **Trigger:** green, `close>prev_high`, `body_frac≥0.55`, `close_loc≥0.6`.

### F3 — LONG Consolidation Expansion Breakout  (33.1%)
- **Trigger:** green, `close>highest-high-of-prior-5`, prior-5 range compressed (`compress5_atr≤2.5`).

### F4 — LONG Failed Breakdown Reversal  (26.6% — weakest)
- **Trigger:** prior bar broke 10-bar low, THIS bar reclaims (`close>prev_close`), green, `lower_wick≥0.2`.

### F5 — LONG Pullback Continuation  (32.4%)
- **Trigger:** `above_ema20 & above_vwap` (uptrend), ≥1 of prior 2 bars red (pullback), green `close>prev_high`.

### F6 — LONG Volume Expansion Breakout  (31.4%)
- **Trigger:** green, `vol_ratio≥1.5`, `close>hh5`, `close_loc≥0.5`. (Volume edge is *negative* — see study.)

### F7 — LONG EMA/VWAP Trend Continuation  (36.4% — best fast% 12.4%)
- **Trigger:** `ema9>ema20>ema50` stacked, `above_vwap`, green, new session high.

### F8 — LONG Opening Strength Continuation  (45.1% — best raw edge, low n)
- **Trigger:** slot≤6 (first ~30 min), `above_vwap`, `close>opening-range-high`.

### F9 — LONG Midday Reclaim Continuation  (38.9%)
- **Trigger:** slot 12–42, `above_vwap`, `close>prev_high`, prior close near/under VWAP (reclaim).

### F10 — LONG Range Expansion After Compression  (33.9%)
- **Trigger:** green, `range_pct > 1.4× prior bar range`, `close>hh5`, `close_loc≥0.55`.

### ALL — pooled union (no family restriction) + generic edge stack.

## Outcome (see ITERATION_LOG.md / BEST_LONG_SETUP_RECOMMENDATION.md)
After 858 logged FIT/VAL trials (2 coordinate rounds × 11 families), **no family produced a
net-profitable tight ~0.75% LONG setup**. The best filtered subsets converge on the
**0.75/1.00** bracket + morning-only + atr_pct floor + momentum, reaching a **zero-cost
price-path PF of ~1.2–1.26** (a small REAL continuation edge), but **statutory cost + slippage
drag net PF to 0.6–0.8 on both TRAIN and TEST**. F4 (reversal) and F6 (volume) reject hardest,
exactly as the edge study predicted. Verdict: **REJECT** — cost-limited, not promotable.
