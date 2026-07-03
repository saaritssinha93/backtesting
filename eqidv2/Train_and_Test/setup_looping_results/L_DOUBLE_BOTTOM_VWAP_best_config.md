# L_DOUBLE_BOTTOM_VWAP — Best Config

**No viable config. No change recommended. Keep parked.**

The conf gate (and every variant tried) suffers a **universal TEST collapse** driven by a 60–79% SL rate in the
late-May/June test window — a regime failure for double-bottom-reclaim LONGS that no gate, exit, or slippage
assumption rescued.

```
exit: 0.90/1.50 ; mask none ; premom pre_entry_momentum_score≥79 & sig5_adx_calc≥28   (conf of record)
```

| Config | 5 bps TRAIN/TEST | 15 bps TRAIN/TEST | TEST SL% |
|---|---|---|---|
| conf gate | 1.30 / 0.48 | 0.88 / 0.29 | 65–72 |
| best TEST (exit 1.1/1.5) | 1.28 / 0.92 | 0.90 / 0.74 | 53 |
| best TRAIN (alt G-gate) | 1.60 / 0.56 | 1.02 / 0.31 | 60–68 |

## Why no change
- TEST PF ≤ 0.92 in all 12 iterations × 2 slippages; the failure is the SL rate (longs stopped out), not cost.
- TRAIN-positive variants (alt-gate, rs-strong) all collapse on TEST → either overfit or a genuine adverse regime.
- The doc's published 2.55/3.57 was RAW-pool with a live-gating caveat (research layer blocks the L* family) and is
  not reproduced on the fresh window.

## Recommendation
Keep parked. No `final_setup_conf.py` change. Re-validation would require a long-favourable regime AND lifting the
research-layer L*-block AND a realistic-cost TEST edge — none present now.
