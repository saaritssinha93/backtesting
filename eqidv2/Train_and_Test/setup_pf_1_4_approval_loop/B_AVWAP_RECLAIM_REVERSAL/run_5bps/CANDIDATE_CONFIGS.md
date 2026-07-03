# CANDIDATE_CONFIGS — B_AVWAP_RECLAIM_REVERSAL

Only configs with **full-TRAIN PF in [1.3,1.7] AND TEST PF > 1.4** (plus trade-count and trade/day/symbol-dominance stability) are listed. Net of cost **@5 bps/leg (paper)**. Window: TRAIN 2026-05-18..06-16 · TEST 2026-06-22..06-24 (2 sessions).

**No candidate cleared the band+TEST gate (even at 5 bps).**

## Closest near-miss @5bps (NOT a candidate)
Best of 400 trials (FIT/VAL band score 1.60):
```json
{
  "exit": {"sl_pct": 0.8, "tgt_pct": 1.5},
  "mask_terms": [["vwap_dist_atr", "<=", 1.0], ["vol_ratio", ">=", 4.410413], ["atr_pct", ">=", 0.002151]],
  "pre_momentum_terms": [["pre1_adx", "<=", 17.56403]],
  "entry_guards": {}, "max_positions": 20, "daily_loss_rs": 0.0
}
```
- FIT n=7 PF 1.80 · VAL n=7 PF 1.63 (thin folds).
- TRAIN n=14 PF **1.715** net +Rs2,663 → *just above the 1.70 band (overfit)*, day-dom **0.848**, sym-dom 0.513 (> 0.40).
- TEST n=**0** (filters take zero trades in the 2-day TEST) → OOS unconfirmable.
- Note: the `pre1_adx≤17.56` gate is a LOW-ADX (counter-intuitive) filter — a non-structural curve-fit signature.

See PARAMETER_SWEEP_SUMMARY.md (every single knob fails even at 5 bps) and EXIT_SWEEP_RESULTS.md (no SL/target reaches TEST > 0.79). Recommendation: **do not promote**.