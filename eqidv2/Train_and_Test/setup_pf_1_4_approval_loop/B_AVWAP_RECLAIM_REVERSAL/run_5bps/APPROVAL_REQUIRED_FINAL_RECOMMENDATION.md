# APPROVAL_REQUIRED — FINAL RECOMMENDATION — B_AVWAP_RECLAIM_REVERSAL (LONG)

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.** This file is a recommendation only. Nothing here has been written to `final_setup_conf.py` or `Train_and_Test/final_setup_conf.py`.

## Cost basis: 5 bps/leg (paper) — optimistic
This run uses **5 bps/leg slippage** (not the realistic 15 bps; the 15 bps run is one level up). Window: TRAIN 2026-05-18..06-16 (20 sessions) · TEST 2026-06-22..06-24 (**2 sessions only**). Lower cost lifts every PF vs 15 bps, but the conclusion is unchanged.

## Verdict: **NO — do not promote** (even at 5 bps)

- **Baseline (card `vwap_dist_atr≤1.0`, 0.70/1.50) @5bps:** TRAIN n=573 PF **0.54** −Rs131,275 · TEST n=60 PF **0.48** −Rs15,844. Still a firehose loser (~30 trades/day).
- **Stage-2 individual-knob sweep @5bps: every single knob still fails** — 0 knobs get both FIT and VAL above PF 1.30 even at this optimistic cost.
- **Exit sweep @5bps:** no SL/target helps — raw best TEST PF 0.79, card best 0.66.
- **Best combination (band search, 400 trials):** best FIT/VAL score 1.60 (vs 1.28 @15bps), but **0 candidates** clear the gate.

### Closest near-miss @5bps (best of 400 trials) — REJECT
- cfg: `SL 0.8 / Tgt 1.5`, mask `vwap_dist_atr≤1.0 & vol_ratio≥4.410413 & atr_pct≥0.002151`, premom `pre1_adx≤17.56403` (a LOW-ADX gate — counter-intuitive/non-monotonic), guard `{}`.
- FIT n=7 PF 1.80 · VAL n=7 PF 1.63 (both folds only 7 trades).
- **TRAIN n=14 PF 1.715 net +Rs2,663** → *just ABOVE the 1.70 band (overfit flag)*, day-dom **0.848** (one day ≈85% of net), sym-dom 0.513 (both > 0.40).
- **TEST n=0** → its filters take zero trades in the 2-day TEST window; OOS unconfirmable.
- **Failure classification:** TRAIN PF too high / overfit; one-day-dominated TRAIN; TEST empty (too-few / 2-day TEST); thin folds (7+7).

Even halving the cost to 5 bps does not create a real edge — it only inflates a tiny (14-trade, single-day) overfit pocket while OOS is empty. Native screening firehose; treat as screening-only.

## If you still want to iterate
- The binding constraint and the closest near-miss are recorded in ITERATION_LOG.md.
- No promotion target file should be edited.

## Commands
```
# baseline replay:
py -3.12 Train_and_Test/setup_loop_runner.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/B_AVWAP_RECLAIM_REVERSAL --configs <baseline.json> --train_start 2026-05-18 --train_end <day-before-test> --test_start 2026-06-20 --test_end <latest> --slippage_bps 15

# full loop rerun:
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_RECLAIM_REVERSAL/scripts/pf_band_search.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/B_AVWAP_RECLAIM_REVERSAL --train_start 2026-05-18 --test_start 2026-06-20 --trials 400 --time_budget_min 25.0 --seed 7
```