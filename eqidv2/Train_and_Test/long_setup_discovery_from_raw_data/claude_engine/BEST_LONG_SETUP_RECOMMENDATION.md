# BEST_LONG_SETUP_RECOMMENDATION — fast-momentum LONG (~0.75% symmetric)

> Engine namespace: `Train_and_Test/long_setup_discovery_from_raw_data/claude_engine/`
> (kept separate from the parallel `tight_raw_long_discovery.py` run in the parent folder).

## ⛔ BOTTOM LINE: **REJECT — DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL**
No tight ~0.75% symmetric LONG setup reached the goal (TEST PF > 1.4, win > 55%). After **858
logged FIT/VAL trials** across 10 families + a pooled union, **two coordinate rounds**, the full
bracket grid and exit variants, validated on TRAIN then TEST — **every candidate is net-negative
after realistic costs.** This is a genuine, well-supported negative result, not a tuning failure.

The same conclusion was reached independently by the parallel run (`candidates/NO_CANDIDATES.md`).

---
## What the search actually found (the honest ceiling)
The single best configuration the search could build (out of all families/filters/brackets):

**Setup name:** `NEW_LONG_FAST_PULLBACK_075_100`  ·  **Family:** F5 — LONG Pullback Continuation

**Exact entry logic (raw 5-min, causal, close-stamped bar @T):**
1. `close > EMA20` AND `close > session-VWAP` (intact intraday uptrend), and
2. at least one of the prior 2 bars was red (a pullback), and
3. this bar is green (`close > open`) and `close > prior-bar high` (resumption breakout).
Enter at the **next 1-min open** (floor(T)+1min, ≤+3min search), 15→5 bps/leg adverse slippage.

**Exact indicator values (mask, AND-combined):** `mom3_pct >= 0.10` (3-bar momentum positive) ·
`atr_pct >= 0.35` (volatility floor — room to reach the target) · `rsi <= 80` (not overbought).

**Exact non-indicator rules / pre-momentum:** uptrend (above EMA20 & VWAP) + a 1–2 bar pullback +
a green breakout of the prior high = "buy the dip resumption". Morning-only (see guards). Prior
bars not both bearish is implied by the pullback-then-green structure.

**Exact filters & guards:** `max_minute = 690` (entries only ≤ 11:30 IST — afternoon follow-through
collapses) · `top_n = 1` strongest per (day, slot) by `atr_pct` · `max_per_sym_day = 2` ·
`max_book_concurrent = 20` · one position per symbol at a time (no overlap).

**Exact SL / target / exit + intrabar resolution:** **SL −0.75% / target +1.00%** off the entry
(the most cost-robust point on the tight grid; pure 0.75/0.75 is worse). Resolved on **1-minute
bars** from entry to EOD cutoff 15:20 IST. If SL and target are touched in the **same 1-min bar**,
**SL is assumed first (pessimistic)** — but at this bracket on liquid names that tie-break fires on
**0.0%** of trades (a 1-min bar essentially never spans the full ±0.75–1.0%), so the resolution is
unambiguous. Variants tested: time-exit 12/18 bars (neutral) and break-even-after-+0.4% (**much
worse** — PF ~0.4, it scratches would-be winners).

### Exact results (net of statutory NSE cost + per-leg slippage)
| window | cost | trades | PF | win% | exp/trade | net Rs | avg win | avg loss | day-dom | sym-dom | top-trade |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **TRAIN** (30d) | 5 bps/leg | 644 | **0.82** | 48.3 | −Rs84 | −53,787 | +806 | −914 | 0.04 | 0.03 | 0.003 |
| **TEST** (10d) | 5 bps/leg | 205 | **0.73** | 45.4 | −Rs134 | −27,507 | +794 | −905 | 0.06 | 0.05 | 0.011 |
| TRAIN | 15 bps/leg | 644 | 0.51 | 48.3 | −Rs282 | −181,479 | +607 | −1,112 | 0.03 | 0.02 | 0.003 |
| TEST | 15 bps/leg | 205 | 0.45 | 44.9 | −Rs332 | −68,135 | +603 | −1,094 | 0.01 | 0.05 | 0.011 |
| **TRAIN** | **0 cost (price-path)** | 644 | **1.26** | 48.3 | +Rs98 | +63,087 | +988 | −733 | 0.04 | 0.03 | 0.003 |
| **TEST** | **0 cost (price-path)** | 205 | **1.12** | 45.9 | +Rs47 | +9,647 | +967 | −732 | 0.09 | 0.06 | 0.011 |

Avg holding ~43 min (TRAIN) / 56 min (TEST). tgt/sl/eod ≈ 309/327/8 (TRAIN), 91/108/6 (TEST).

## Why it (almost) works, and why it ultimately fails
- **The raw edge is REAL but tiny.** At zero cost the structure earns **+Rs98/trade (~+0.10% of a
  Rs1L notional)**, PF 1.26 TRAIN / 1.12 TEST. A morning pullback-continuation in a volatile,
  uptrending liquid name *does* reach +1.0% slightly more value-weighted than it gives back 0.75%.
- **Costs eat the edge.** Statutory NSE cost (~Rs90/trade ≈ 0.09% round-trip) + 5 bps/leg slippage
  (~Rs92) ≈ **Rs182/trade** — larger than the +Rs98 gross edge → net **−Rs84/trade**. It is
  **cost-limited, not lucky and not concentrated**: day-dominance 0.04, symbol-dominance 0.03,
  top-trade 0.3% — the loss is spread across hundreds of trades, dozens of names, every week.
- **The break-even math is unforgiving for sub-1% targets.** At 5 bps/leg the break-even win-rate
  is ~57% for 0.75/0.75 and ~49% for 0.75/1.00; the best achievable win-rate after every sensible
  filter is only ~45–52%. The edge study's base P(+0.75% before −0.75%) is just **32%** (lifting to
  ~50% with morning+ATR+momentum) — structurally below the bar costs impose.
- **TEST does not collapse from overfitting** — it tracks TRAIN closely (TRAIN PF 0.82 → TEST 0.73,
  gross 1.26 → 1.12). The OOS behaviour is consistent; there is simply no profit to capture.

## Stability checks (all consistent with REJECT)
- Trade count high (good — tight target = many fills, as intended): ~20–21/day.
- No single day/symbol/trade dominates (dominance ≤0.09). ✓ not a fluke.
- Avg loss does **not** blow past the bracket (−Rs914 ≈ −0.75% + slippage, as designed). ✓
- Intrabar tie-break fires 0.0% → 1-min resolution trustworthy. ✓
- Time-exits/EOD do not dominate; outcome is decided by SL/target. ✓
- It fails the ONLY check that matters here: **net PF < 1 on TRAIN and TEST.**

## Verdict
**REJECT for any capital (live or paper-for-profit).** It is safe only as a *watch/log* item to
confirm the cost wall in live data. Recommended next directions (all OUTSIDE the tight ±0.75% theme,
so they need your explicit go-ahead to research): (a) larger targets (≥1.5–2.0% R-multiples) where
the same +0.10% edge clears costs; (b) reducing cost (maker/limit entries instead of 1-min-open
market fills); (c) SHORT-side symmetric test (LONG reverts at 0.75% — shorts may differ).

---
## Candidate config block (best near-miss — for the record only)
`candidates/BEST_NEAR_MISS_candidate_001.json`
```json
{
  "setup": "NEW_LONG_FAST_PULLBACK_075_100",
  "family": "F5_PULLBACK_CONT",
  "config": {
    "family": "F5_PULLBACK_CONT", "bracket": "b_075_100",
    "max_minute": 690, "top_n": 1, "rank_feat": "atr_pct",
    "max_per_sym_day": 2, "max_book_concurrent": 20,
    "mask": [["mom3_pct", ">=", 0.1], ["atr_pct", ">=", 0.35], ["rsi", "<=", 80]]
  },
  "verdict": "REJECT — net PF 0.82 TRAIN / 0.73 TEST @5bps (cost-limited)"
}
```

## Exact final-config block that WOULD need approval (illustrative — NOT recommended)
If — and only if — you later approved it, the repo `final_setup_conf.FINAL_SETUP_CONF` entry would
look like the block below. **It is shown so the spec is complete; it is NOT a recommendation and the
backtest says it loses money.**
```python
# >>> ILLUSTRATIVE ONLY — backtest verdict is REJECT (net-negative). DO NOT ADD. <<<
"NEW_LONG_FAST_PULLBACK_075_100": {
    "side": "LONG",
    "sl_pct": 0.75, "target_pct": 1.00,           # tight, mildly asymmetric (most cost-robust)
    "selected_strategy_profile": "final_setup_conf",
    "conf_mask": [                                  # AND-combined, on causal 5-min features
        ["mom3_pct", ">=", 0.10],
        ["atr_pct",  ">=", 0.35],
        ["rsi",      "<=", 80.0],
    ],
    "entry_guards": {"max_slot": "11:30", "top_n": 1},
    "max_per_symbol_day": 2,
    "structure": "above EMA20 & VWAP; 1-2 bar pullback; green close > prior-bar high",
},
```

## 🚫 DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL
**This research found NO promotable fast-momentum LONG setup. The block above is illustrative and
the validated backtest verdict is REJECT (net-negative after costs on both TRAIN and TEST).
`final_setup_conf.py` and `Train_and_Test/final_setup_conf.py` were NOT modified.**
