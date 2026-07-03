# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 500 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.001589', 'atr_pct>=0.002872', 'atr_pct>=0.003223', 'atr_pct>=0.00384', 'atr_pct>=0.00483', 'body_pct>=0.5432', 'body_pct>=0.742217', 'body_pct>=0.846154', 'body_pct>=0.916667', 'body_pct>=0.943067', 'close_loc<=0.894691', 'close_loc>=0.781722'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 2.5 | quality_score>=78.052591;lower_wick_pct<=0.038486 | - | - | 7/2.002 | 7/1.382 | 0.9068 |
| 2 | 1.5 | 2.5 | quality_score>=78.052591;lower_wick_pct<=0.038486 | - | - | 7/2.002 | 7/1.382 | 0.9068 |
| 3 | 1.5 | 2.5 | quality_score>=78.052591;lower_wick_pct<=0.038486 | - | - | 7/2.002 | 7/1.382 | 0.9068 |
| 4 | 1.5 | 2.5 | quality_score>=78.052591;lower_wick_pct<=0.038486 | - | - | 7/2.002 | 7/1.382 | 0.9068 |
| 5 | 1.5 | 2.5 | quality_score>=78.052591;lower_wick_pct<=0.038486 | - | - | 7/2.002 | 7/1.382 | 0.9068 |
| 6 | 1.5 | 2.5 | quality_score>=78.052591;lower_wick_pct<=0.038486 | - | - | 7/2.002 | 7/1.382 | 0.9068 |
| 7 | 1.5 | 2.5 | quality_score>=78.052591;lower_wick_pct<=0.038486 | - | - | 7/2.002 | 7/1.382 | 0.9068 |
| 8 | 1.2 | 2.5 | quality_score>=78.052591 | - | - | 13/0.735 | 13/0.714 | 0.6965 |
| 9 | 1.2 | 2.5 | quality_score>=78.052591 | - | - | 13/0.735 | 13/0.714 | 0.6965 |
| 10 | 1.2 | 2.5 | quality_score>=78.052591 | - | - | 13/0.735 | 13/0.714 | 0.6965 |
| 11 | 1.2 | 2.5 | quality_score>=78.052591 | - | - | 13/0.735 | 13/0.714 | 0.6965 |
| 12 | 1.2 | 2.5 | quality_score>=78.052591 | - | - | 13/0.735 | 13/0.714 | 0.6965 |
| 13 | 1.2 | 2.5 | quality_score>=78.052591 | - | - | 13/0.735 | 13/0.714 | 0.6965 |
| 14 | 1.2 | 2.5 | quality_score>=78.052591 | - | - | 13/0.735 | 13/0.714 | 0.6965 |
| 15 | 1.5 | 2.5 | quality_score>=78.052591 | - | - | 13/0.638 | 13/0.634 | 0.6302 |
| 16 | 1.5 | 2.5 | quality_score>=78.052591 | - | - | 13/0.638 | 13/0.634 | 0.6302 |
| 17 | 1.5 | 2.5 | quality_score>=78.052591 | - | - | 13/0.638 | 13/0.634 | 0.6302 |
| 18 | 1.5 | 2.5 | quality_score>=78.052591 | - | - | 13/0.638 | 13/0.634 | 0.6302 |
| 19 | 1.5 | 2.5 | quality_score>=78.052591 | - | - | 13/0.638 | 13/0.634 | 0.6302 |
| 20 | 1.5 | 2.5 | quality_score>=78.052591 | - | - | 13/0.638 | 13/0.634 | 0.6302 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 2.5 | mask [quality_score>=78.052591; lower_wick_pct<=0.038486] | premom [(none)] | guard - | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=14 PF=1.629 net=Rs3,188 win%=64.3 avgW=Rs918 avgL=Rs-1,014 maxDD=Rs-3,377 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.27 tradeDom=0.274 dayDom=0.767 symDom=0.711 dbp=0.246
- **TEST  @15bps:** n=4 PF=0.0 net=Rs-4,339 win%=0.0 avgW=Rs0 avgL=Rs-1,085 maxDD=Rs-2,619 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=14 PF=1.629 net=Rs3,188 win%=64.3 avgW=Rs918 avgL=Rs-1,014 maxDD=Rs-3,377 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.27 tradeDom=0.274 dayDom=0.767 symDom=0.711 dbp=0.246
- **TEST  @5bps:**  n=4 PF=0.0 net=Rs-4,339 win%=0.0 avgW=Rs0 avgL=Rs-1,085 maxDD=Rs-2,619 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/doc5_long_setups/pool --trials 500 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```