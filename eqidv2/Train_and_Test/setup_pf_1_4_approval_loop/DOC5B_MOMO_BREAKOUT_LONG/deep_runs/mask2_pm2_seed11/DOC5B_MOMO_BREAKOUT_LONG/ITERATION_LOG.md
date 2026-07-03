# DOC5B_MOMO_BREAKOUT_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 600 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001925', 'atr_pct>=0.002187', 'body_pct<=0.517299', 'body_pct<=0.61087', 'close_loc<=0.95925', 'close_loc<=1.0', 'lower_wick_pct<=0.0', 'lower_wick_pct>=0.083517', 'quality_score<=79.630276', 'quality_score<=83.630824', 'quality_score>=87.582324', 'ranker_score<=114.680021'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;pre1_adx<=44.037386 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 20/0.768 | 14/0.755 | 0.745 |
| 2 | 0.7 | 1.0 | - | sig5_rsi_dir<=62.892662;sig5_vol_ratio20<=2.12622 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 11/0.818 | 7/1.012 | 0.6625 |
| 3 | 0.7 | 2.5 | - | pre3_range_r<=0.145999;sig5_vol_ratio20<=3.416982 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 13/0.599 | 14/0.613 | 0.5874 |
| 4 | 0.7 | 2.5 | - | pre3_range_r<=0.145999;sig5_vol_ratio20<=3.416982 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 13/0.599 | 14/0.613 | 0.5874 |
| 5 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.584261 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 6 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 7 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 8 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 9 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 10 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 11 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 12 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 13 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 14 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 15 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 16 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 17 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 18 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.370634 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 19 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.12622 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |
| 20 | 0.7 | 2.5 | - | sig5_vol_ratio20<=1.77545;sig5_vol_ratio20<=2.12622 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 21/0.696 | 16/0.635 | 0.5867 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 2.5 | mask [(none)] | premom [sig5_vol_ratio20<=1.77545; pre1_adx<=44.037386] | guard {'min_slot': '11:00', 'max_slot': '14:00', 'top_n': 1} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=34 PF=0.763 net=Rs-3,557 win%=41.2 avgW=Rs816 avgL=Rs-749 maxDD=Rs-5,487 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.27 tradeDom=0.198 dayDom=9.99 symDom=9.99 dbp=0.7144
- **TEST  @15bps:** n=8 PF=0.662 net=Rs-1,517 win%=37.5 avgW=Rs989 avgL=Rs-897 maxDD=Rs-2,621 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.695 dayDom=9.99 symDom=9.99 dbp=0.798
- **TRAIN @5bps:**  n=34 PF=0.763 net=Rs-3,557 win%=41.2 avgW=Rs816 avgL=Rs-749 maxDD=Rs-5,487 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.27 tradeDom=0.198 dayDom=9.99 symDom=9.99 dbp=0.7144
- **TEST  @5bps:**  n=8 PF=0.662 net=Rs-1,517 win%=37.5 avgW=Rs989 avgL=Rs-897 maxDD=Rs-2,621 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.695 dayDom=9.99 symDom=9.99 dbp=0.798

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool --trials 600 --time_budget_min 12.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```