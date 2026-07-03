# C_OR_BREAKDOWN — Best Config

**No robust improvement found. No config change recommended.** Loser at realistic execution cost.

## Existing conf gate of record (unchanged)
```
exit:               SL 0.90 / Tgt 2.00
mask_terms:         []
pre_momentum_terms: sig5_adx_calc >= 39.670518
                    pre1_adx      <= 21.368044   (ALL required, missing -> block)
entry_guards:       {}
```

## Performance (fresh window TRAIN 04-13..05-25 / TEST 05-26..06-24)

| Config | 15 bps/leg TRAIN / TEST | 5 bps/leg TRAIN / TEST |
|---|---|---|
| conf gate (0.9/2.0) | 0.64 (83) / 0.62 (53) | 0.94 (83) / 1.12 (53) |
| `rs_pct≤-4.92`, 1.2/1.5 *(best lead)* | 0.64 (121) / 0.93 (76) | **1.00 (121) / 1.33 (76)** |
| `vol_ratio≥2.0`, 0.9/2.0 | 0.66 (51) / 0.71 (30) | 0.98 (51) / 1.42 (30) |

## Why no change / no promotion
- **Realistic cost (15 bps/leg): every config is a loser.** This is the deployable bar (live paper PF was 0.25,
  i.e. real fills are worse than 15 bps) → not deployable.
- At paper cost (5 bps/leg) the best lead (`rs_pct≤-4.92`) gives a **well-distributed TEST 1.33** but **TRAIN only
  1.00** (breakeven; train net is one day at 54× share) → fails the TRAIN ≥ 1.2 bar.
- maxpf @15 bps and band @5 bps both FDR-dropped (test p 0.69 and 0.12). Conf claim (2.78/5.26) not reproduced.

## Research lead worth keeping (do NOT trade yet)
`rs_pct ≤ -4.92` (deep relative weakness, replacing the ADX pre-momentum gate) is the only direction with a
genuinely well-distributed positive TEST (1.33, n=76, top-1-day 0.72, dbp 0.20) at paper cost. It needs: (a)
profitability at realistic cost, (b) a TRAIN that clears 1.2 after dedupe, (c) more data. Currently it fails all three.

## Recommendation
Keep C_OR_BREAKDOWN **unsized**; recommend parking/demoting it with the others (fails its re-promotion trigger
at realistic cost). **No `final_setup_conf.py` edit made** (requires `--approve` + user sign-off; flagged).
