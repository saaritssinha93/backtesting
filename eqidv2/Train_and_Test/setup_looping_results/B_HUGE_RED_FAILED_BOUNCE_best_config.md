# B_HUGE_RED_FAILED_BOUNCE - Best Config

## Rolling 2-Week/1-Week Audit Update - 2026-06-29

Decision: **REJECTED - no accepted new config**.

Keep the current config unchanged in this pass. The rolling split does not justify promoting, loosening, tightening, or sizing B.

| Candidate | TRAIN n/PF/net | TEST n/PF/net | Decision |
|---|---:|---:|---|
| Current conf @15 bps | 10 / 0.647 / -Rs1,246 | 5 / 0.873 / -Rs295 | Reject |
| Current conf @5 bps | 10 / 1.747 / +Rs1,749 | 5 / 1.294 / +Rs525 | Watch only; TEST below 1.3 and n=5 |
| Best hand-loop TEST PF: `sig5_rsi_dir<=60` | 6 / 1.055 / +Rs108 | 3 / 18.301 / +Rs1,919 | Reject; TRAIN PF failed and TEST n=3 |
| Best hand-loop TRAIN PF with n>=6: `vol_ratio>=2.0` | 7 / 1.896 / +Rs1,078 | 2 / 0.000 / -Rs1,193 | Reject TEST collapse |
| Official maxpf tuner | 15 / 3.494 / +Rs5,352 | 3 / 0.447 / -Rs1,190 | Reject overfit |
| Official band tuner | 27 / 1.440 / +Rs2,932 | 9 / 0.140 / -Rs6,919 | Reject TEST collapse |

No production config files were changed.

---

## Prior Active-Book Best Config

**No robust improvement found. No config change recommended.** The existing conf gate stays the
best-documented config, but its viability is **slippage-fragile** and its published edge is **not reproducible**
on the current pool basis.

## Existing conf gate of record (unchanged)
```
exit:               SL 0.90 / Tgt 1.25
mask_terms:         []
pre_momentum_terms: pre3_close_pos <= 0.581797
                    sig5_rsi_dir   <= 64.104659
                    pre5_mom_r     <= 0.284145   (ALL required, missing -> block)
entry_guards:       {}
```

## Performance summary (fresh window TRAIN 04-13..05-25 / TEST 05-26..06-24)

| Slippage/leg | TRAIN PF (n) | TEST PF (n) | Read |
|---|---:|---:|---|
| 15 bps (realistic-fill stress) | 0.79 (28) | 0.52 (16) | clear loser |
| 5 bps (≈ live paper) | 1.63 (28) | 1.10 (16) | TRAIN ok, TEST below 1.3 bar + one day = 362% of net |

## Why no change / no promotion
- At realistic execution (15 bps/leg) it loses → fails the user's "robust net of realistic cost" bar.
- At paper execution (5 bps/leg) TRAIN passes 1.2 but **TEST PF 1.10 < 1.3** and is dominated by a single day
  (top-1-day = 362% of TEST net) → fails "not dominated by one lucky day" and the re-promotion trigger.
- The published conf evidence (train 2.90 / test 3.49) is **not reproducible** on the unified-pool readmit
  basis (train n=75/PF 1.28 even at 5 bps) → the basis the conf was tuned on differs from the live-faithful pool.
- A `quality_score≤37.6` mask looks strong (5 bps: TRAIN 2.95 / TEST 3.30) but TEST n=4 — too thin to adopt.

## Recommendation
Keep B_HUGE_RED **unsized**. It is currently still ACTIVE in `FINAL_SETUP_CONF` (survived the 2026-06-29
demotion); this audit argues it should be **parked/demoted** like the others — it fails the very re-promotion
trigger (fresh TEST PF ≥ 1.30, ≥20 non-concentrated trades, at realistic cost) it would need to remain.
**No `final_setup_conf.py` edit made** — config changes require `--approve` + user sign-off; flagged for review.
