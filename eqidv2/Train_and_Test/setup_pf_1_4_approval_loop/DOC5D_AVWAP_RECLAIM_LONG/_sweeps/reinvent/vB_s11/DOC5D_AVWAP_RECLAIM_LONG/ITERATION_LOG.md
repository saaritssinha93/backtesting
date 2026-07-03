# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 400 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.002916', 'atr_pct>=0.003794', 'atr_pct>=0.004206', 'atr_pct>=0.007983', 'body_pct<=0.821143', 'body_pct<=0.90813', 'body_pct>=0.716767', 'body_pct>=0.821143', 'body_pct>=0.979383', 'close_loc<=0.891624', 'close_loc<=0.93375', 'close_loc>=0.75'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 2.0 | wick_skew_pct>=0.04464 | pre1_adx<=18.768368 | - | 7/1.616 | 13/1.731 | 1.5443 |
| 2 | 0.7 | 1.0 | quality_score>=69.476513 | sig5_rsi_dir<=54.543866 | {"top_n": 3} | 6/1.499 | 9/1.521 | 1.4995 |
| 3 | 0.7 | 1.0 | quality_score>=69.476513 | sig5_rsi_dir<=54.543866 | {"top_n": 3} | 6/1.499 | 9/1.521 | 1.4995 |
| 4 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 5 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 6 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 7 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 8 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 9 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 10 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 11 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 12 | 0.7 | 1.0 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.499 | 9/1.521 | 1.4995 |
| 13 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |
| 14 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |
| 15 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |
| 16 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |
| 17 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |
| 18 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |
| 19 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |
| 20 | 0.7 | 1.25 | ranker_score>=69.476513 | sig5_rsi_dir<=54.543866 | - | 6/1.866 | 9/1.579 | 1.3683 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 2.0 | mask [wick_skew_pct>=0.04464] | premom [pre1_adx<=18.768368] | guard - | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=20 PF=1.687 net=Rs6,050 win%=45.0 avgW=Rs1,650 avgL=Rs-800 maxDD=Rs-4,765 SL/TGT/EOD=10/7/3 tgt%=35.0 tpd=1.67 tradeDom=0.126 dayDom=0.917 symDom=0.308 dbp=0.225
- **TEST  @15bps:** n=2 PF=0.0 net=Rs-982 win%=0.0 avgW=Rs0 avgL=Rs-491 maxDD=Rs-829 SL/TGT/EOD=1/0/1 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=20 PF=1.687 net=Rs6,050 win%=45.0 avgW=Rs1,650 avgL=Rs-800 maxDD=Rs-4,765 SL/TGT/EOD=10/7/3 tgt%=35.0 tpd=1.67 tradeDom=0.126 dayDom=0.917 symDom=0.308 dbp=0.225
- **TEST  @5bps:**  n=2 PF=0.0 net=Rs-982 win%=0.0 avgW=Rs0 avgL=Rs-491 maxDD=Rs-829 SL/TGT/EOD=1/0/1 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vB --trials 400 --time_budget_min 5.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```