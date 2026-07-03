# A_MOD_BREAK_C1_LOW — Final Summary

**Verdict: REJECT for sizing / keep unsized (parked).** The strongest of the four active mined shorts at paper
cost (TRAIN 1.23 well-distributed), but a clear loser at realistic execution cost and TEST never clears 1.3.

## What was done (≈26 evaluations)
- Verified backtest == live for the conf path (bootstrap reads conf mask + premom verbatim). Noted the doc §5.2
  overlay-gate divergence (overlay uses `abs(rs_pct)≥9.2 & vol≥1.80`, suppressed when conf flag on). ✔
- Baseline at 5 + 15 bps; 12 hand iterations (exit grid, premom loose/tight, mask-only, vol band, rs-weakness,
  time guard) at **both** slippages; maxpf tuner @15 bps; loss analysis.

## Key numbers

| Config | 5 bps TRAIN/TEST | 15 bps TRAIN/TEST |
|---|---|---|
| conf gate (1.10/1.00) | 1.23 (71) / 1.06 (22) | 0.62 (72) / 0.57 (23) |
| best TRAIN variant (vol band ≤4) | 1.48 (48) / 0.96 (18) | 0.76 / 0.53 |

Conf published claim: TRAIN 2.58 / TEST 2.83 — not reproduced on the readmit pool basis.

## Why REJECT (against the acceptance bar)
- **Realistic cost:** loser everywhere (15 bps/leg) → not deployable.
- **TEST ≥ 1.3:** never met at any slippage (best 1.10); baseline TEST 1.06 is one-day-dominated.
- **Overfit signature:** TRAIN-boosting filters all hurt TEST (the hallmark of curve-fitting a thin sample).
- **Reproducibility:** conf 2.58/2.83 not reproduced (basis-size discrepancy).

## Honest notes / leads
- Best paper-cost TRAIN of the four shorts (1.23, well-distributed, dbp 0.30) and most balanced exits (33% target
  hit). If the book is ever re-examined under genuinely low realistic fills, A_MOD is the first to re-check.
- The asymmetric 1.10/1.00 exit is correct for this high-win scalp — tighter SLs (0.7–0.9) consistently hurt.

## Action
- **No config change.** Recommend parking/demoting (fails re-promotion trigger at realistic cost). Flagged for review.
