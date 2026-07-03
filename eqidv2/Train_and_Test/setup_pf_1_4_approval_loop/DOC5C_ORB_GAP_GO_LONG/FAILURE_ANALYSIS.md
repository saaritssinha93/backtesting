# DOC5C_ORB_GAP_GO_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample.

## Classified failure reasons

- TRAIN too few trades (train_n<20)
- TRAIN PF too low (<1.30)
- TRAIN target-fill rate below 10.0%
- TRAIN concentrated (one trade/day/symbol dominates)
- neighborhood robustness failed
- term-dropout robustness failed
- TEST too few trades (test_n<6)

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 0.0% (min 10.0%)
- TEST PF/day-block p: 0.245 / None (gate 1.4 / 0.1)

---

## Root-cause analysis (from `scripts/gap_knob_sweep.py`)

**Primary failure class: NO EDGE (structural), compounded by too-few-trades OOS.**

Classified against the task's failure taxonomy:

| failure reason | evidence |
|---|---|
| **too many SL exits / poor win rate** | raw TRAIN 69 SL vs 9 TARGET vs 23 EOD; win 19.8%, target-fill 8.9% |
| **fake breakout (the core issue)** | gap-and-go on a 5-min next-open fill = enter ~5 min into the ORB break, deep in the move; it reverts. Doc explicitly flags Setup C/B as *"hit hardest by 5-min-only."* |
| **TRAIN PF too low** | best full-TRAIN PF over the entire search = 0.683 (« 1.30 floor) |
| **TEST PF below 1.40** | best confirmable TEST = engine's 0.245 on n=2; raw TEST 0.140 |
| **too few trades (OOS)** | any gate that lifts FIT PF at all shrinks TEST to ≤6 trades over 4 sessions |
| **one-trade/day/symbol dominance** | at low n the surviving books trip the 0.50 dominance cap (top-trade 0.36–1.0) |
| **FIT↔VAL anti-correlation** | filters that raise VAL PF (e.g. vol_ratio≥3.5 → VAL 0.64, pre3_range_r≥0.5 → VAL 1.03) simultaneously crush FIT (0.04–0.06) on ≤7 trades = noise, not signal |

### Worst behaviour by window
- **Worst days (raw):** the whole 09:45–11:00 gap book bleeds daily — TRAIN net −Rs 62,660 across
  ~9–15 active days; no single day rescues it (max top-day share is a rounding artifact at low n).
- **Exit behaviour:** SL-dominated (raw ~68% SL), target-fill single digits — the target is rarely
  reached because entries are already extended (`orh_dist_atr` median ≈ 2.2 ATR above ORH).
- **Extension check:** tightening `orh_dist_atr≤1.0` (buy nearer the ORH, doc `ext_max`) does NOT
  rescue it (FIT 0.035 / VAL 0.497 on n=9/4) — the reversion is not just a chase-distance artifact.

### Why the gap-specific levers don't help
`gap_pct` (controlled-gap band), `orh_dist_atr` (extension cap), and `vwap_slope_atr`
(rising-VWAP) — the three doc levers the canonical engine can't sweep — were each swept
individually and in ≤2-term combinations. Best worse-half PF across 2,185 combinations = **0.51**.
The gap-and-go structure simply has no positive expectancy at a 5-min next-open fill on this
universe/window.

### What would be required to revisit (not pursued — out of task scope)
A true `stop_confirm` execution model (buy-stop at ORH, fill only if `T+1` trades through) or a
1-min entry — the doc's own note that gap-and-go needs intra-bar confirmation the 5-min-only
model removed. Both are execution-layer changes, not knob tuning, and neither is a
`final_setup_conf` knob.