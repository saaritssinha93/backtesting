# L_PRESSURE_BURST_VWAP — Final Summary

**Verdict: REJECT / keep parked — the clearest, most robust reject in the audit.** A structural loser at every
config and slippage on a large sample.

## What was done (≈26 evaluations)
- Verified the conf gate is applied verbatim (readmit; live research layer also blocks L*).
- Baseline at 5 + 15 bps; 12 hand iterations × both slippages (exit grid, quality mask on/off/flip, ADX
  threshold sweep, vol/rs masks); loss analysis.

## Key numbers (conf gate)

| Slippage | TRAIN n/PF | TEST n/PF |
|---|---:|---:|
| 5 bps/leg | 138 / 0.84 | 67 / 0.79 |
| 15 bps/leg | 138 / 0.51 | 67 / 0.39 |

Best config found (exit 0.9/1.5 @5 bps): TRAIN 0.95 / TEST 0.79 — still both losers.

## Why REJECT
- **Loser at every gate/exit/slippage**, on n=138/67 (robust, not noise).
- The gate barely improves on the ungated firehose (TRAIN 0.51 vs 0.32) — almost no edge contribution.
- `quality_score≤25` (low-quality selection) and non-monotonic `pre1_adx≥44` were always speculative; the fresh
  window confirms there is no real edge. Matches the doc's USER_APPROVED_OVERRIDE_WEAK / failed-anti-overfit flag.

## Action
- **No config change.** Keep parked. Unlike the other setups, there is no WATCH lead — recommend it not be
  re-promoted under any condition without a fundamentally different thesis.
