# L_DOUBLE_BOTTOM_VWAP — Final Summary

**Verdict: REJECT (keep parked).** Clean, decisive: TEST collapses universally on the fresh window (PF 0.21–0.92,
60–79% SL) — a regime failure for long reversals, not a cost or tuning problem.

## What was done (≈26 evaluations)
- Verified the conf gate is applied verbatim, but noted the live research layer **blocks the L* family** (so a good
  result would still need that block lifted) — recorded live/backtest gating gap.
- Baseline at 5 + 15 bps; 12 hand iterations × both slippages (exit grid, alt G-style gate, momentum loose/tight,
  ADX tighten, vol/rs masks, premom drop-out); loss analysis.

## Key numbers (conf gate)

| Slippage | TRAIN n/PF | TEST n/PF | TEST SL% |
|---|---:|---:|---:|
| 5 bps/leg | 38 / 1.30 | 29 / 0.48 | 65 |
| 15 bps/leg | 38 / 0.88 | 29 / 0.29 | 72 |

Conf published (RAW-pool, gating caveat): train 2.55 / test 3.57 — **not reproduced** on the fresh window.

## Why REJECT
- **TEST PF ≤ 0.92 across all 12 configs × 2 slippages**, with a 60–79% SL rate — the long reclaims were walled by
  stops throughout the late-May/June test period.
- TRAIN-positive variants (alt G-gate 1.60, rs-strong 1.55 @5 bps) **all collapse on TEST** — textbook
  train-test divergence (overfit and/or adverse regime).
- Not a slippage story: lower cost lifts TRAIN but TEST stays broken (the losses are SL hits, not fees).

## Action
- **No config change.** Keep parked. Re-validation needs a long-favourable regime + lifting the L*-family
  research block + a realistic-cost TEST edge — none present now.
