# A_MOD_BREAK_C1_LOW — Best Config

**No robust improvement found. No config change recommended.** Best of the three active shorts at paper cost, but
still a loser at realistic cost and TEST never clears the bar.

## Existing conf gate of record (unchanged — already near-optimal for TRAIN @5 bps)
```
exit:               SL 1.10 / Tgt 1.00   (asymmetric high-win scalp)
mask_terms:         vol_ratio >= 1.955814
pre_momentum_terms: pre5_mom_r   >= 0.425861
                    pre3_range_r <= 0.202087   (ALL required, missing -> block)
entry_guards:       {}
```

## Performance (fresh window TRAIN 04-13..05-25 / TEST 05-26..06-24)

| Config | 5 bps/leg TRAIN / TEST | 15 bps/leg TRAIN / TEST |
|---|---|---|
| conf gate (1.10/1.00) | **1.23 (71) / 1.06 (22)** | 0.62 (72) / 0.57 (23) |
| + vol band ≤4.0 | 1.48 (48) / 0.96 (18) | 0.76 / 0.53 |
| premom tight ≥0.55 | 1.44 (11) / 0.93 (8) | 0.75 / 0.43 |

## Why no change / no promotion
- **Realistic 15 bps/leg: loser across all 12 iterations** (best 0.76/0.53) → not deployable.
- At paper 5 bps the baseline TRAIN clears 1.2 (1.23, well-distributed) but **TEST 1.06 < 1.3** and is
  one-day-dominated (243%). Every TRAIN-boosting variant (volband 1.48, mom-tight 1.44) **degrades TEST** → overfit.
- The existing conf gate is already the best TEST config at paper cost; nothing improves it robustly.

## Recommendation
Keep A_MOD_BREAK_C1_LOW **unsized**; recommend parking/demoting with the others (fails its re-promotion trigger
at realistic cost). It is the strongest survivor candidate of the four mined shorts (cleanest paper TRAIN), so if
the book is ever revisited at genuinely low fills, this is the first to re-check. **No `final_setup_conf.py` edit
made** (requires `--approve` + sign-off; flagged).
