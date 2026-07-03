# DOC5A_AVWAP_PULLBACK_LONG (LONG) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-01. Research-only. Evidence base: `trials.csv` (700 Optuna-TPE FIT/VAL trials) +
`scripts/eval_baseline.py`. All PF net of NSE costs @15 bps/leg, next-open fill._

**Headline:** across 700 trials spanning the full repo-supported knob space, **0 configs** reached
min(FIT,VAL) PF ≥ 1.00 at meaningful trade counts. **Ceiling = min(FIT,VAL) PF 0.449.** No indicator,
non-indicator, pre-momentum, filter, guard, or exit value moves this setup toward the 1.30–1.70 band.
Adding *any* mask term makes it worse than the unfiltered book — there is no profitable subset to isolate.

## Exit SL / target sweep (grid SL {0.50,0.70,0.85,1.00,1.10,1.20,1.50} × Tgt {0.60,0.80,1.00,1.25,1.50,2.00,2.50})

| SL | Tgt | best min(FIT,VAL) PF | median FIT / VAL PF | trials | note |
|---|---|---|---|---|---|
| 0.70 | 1.25 | **0.449** | 0.449 / 0.447 | 169 | best bracket — still a heavy loser |
| 0.85 | 1.25 | 0.440 | 0.356 / 0.351 | 265 | |
| 0.70 | 1.00 | 0.370 | 0.370 / 0.398 | 7 | tighter target, no help |
| 0.85 | 2.50 | 0.356 | 0.286 / 0.331 | 29 | wide target starves fills |
| 1.00–1.50 | 1.25 | 0.31–0.33 | — | — | wider SL worse |
| 0.50 | 1.25 | 0.314 | 0.277 / 0.267 | 13 | tighter SL worse |

**Best stable range:** SL 0.70–0.85, Tgt 1.00–1.25. **Rejected:** wide SL (≥1.00), wide Tgt (≥1.50) —
targets rarely fill on this move. Even the best bracket is PF < 0.45.

## Indicator + non-indicator mask sweep (feats: rs_pct, vol_ratio, atr_pct, body_pct, close_loc, vwap_dist_atr, quality_score, ranker_score, signal_range_pct, upper/lower_wick_pct, wick_skew_pct)

| mask feature | best min(FIT,VAL) PF | median | note |
|---|---|---|---|
| **(none)** | **0.449** | 0.321 | no-mask is the best — filtering only hurts |
| vol_ratio | 0.331 | 0.248 | |
| upper_wick_pct | 0.284 | 0.284 | |
| quality_score | 0.282 | 0.242 | |
| ranker_score | 0.255 | 0.253 | |
| body_pct / atr_pct / rs_pct / close_loc / signal_range_pct | 0.18–0.24 | — | all worse |

**Best stable range:** none — every mask term lowers PF vs the unfiltered book. **Rejected:** all mask
features (no threshold on any indicator/price-action value isolates a winning subset). Overfit-risk note:
a couple of vol_ratio/quality_score points score marginally higher on FIT but collapse on VAL (n≈2–3).

## Pre-momentum sweep (feats: pre1_adx, pre3_range_r, pre5_mom_r, pre3_close_pos, pre_entry_momentum_score, sig5_adx_calc, sig5_rsi_dir, sig5_vol_ratio20)

| premom feature | best min(FIT,VAL) PF | median | note |
|---|---|---|---|
| pre3_range_r | 0.449 | 0.447 | best, but identical to no-mask baseline (no lift) |
| pre1_adx | 0.356 | 0.326 | |
| sig5_adx_calc | 0.343 | 0.259 | |
| pre5_mom_r | 0.334 | 0.251 | |
| pre_entry_momentum_score / sig5_rsi_dir / pre3_close_pos / sig5_vol_ratio20 | 0.27–0.33 | — | no lift |

**Best stable range:** none provides positive expectancy. `pre3_range_r>=0.31` ties the unfiltered
ceiling (0.449) — it trims count without changing edge, so it is not a real filter.

## Guards / portfolio sweep (min_slot, max_slot, top_n, max_positions, daily_loss_rs)
- `max_slot 12:30` + `top_n 3` appears in the "best" config — but only because it caps the bleed
  (fewer losers), not because it finds winners. `min_slot`, `top_n`, `max_positions {10,20}`,
  `daily_loss_rs {0,4000}` all leave PF < 0.45.
- **Best stable range:** trade-count reducers only reduce losses; none creates edge. Rejected as edge levers.

## Conclusion
Every knob was swept over a realistic range. The best obtainable in-sample (FIT/VAL) PF is **0.449** and
the best full-TRAIN config the search confirmed is **PF 0.428 (n=64)** → **TEST PF 0.499 (n=12)**.
There is no combination of existing indicator / non-indicator / pre-momentum / filter / guard / exit
values that produces a tradeable positive book. **Setup is clearly dead** (search stopped at 700 trials,
far beyond the 25-iteration floor; the spec's "unless clearly dead earlier" applies). See FAILURE_ANALYSIS.md.
