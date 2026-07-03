# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 400 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002703', 'atr_pct>=0.002703', 'atr_pct>=0.007624', 'body_pct<=0.848462', 'close_loc<=0.828958', 'close_loc<=0.956646', 'lower_wick_pct>=0.014574', 'lower_wick_pct>=0.069557', 'quality_score<=81.612949', 'ranker_score<=60.548787', 'ranker_score>=77.558394', 'rs_pct<=0.997453'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 2 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 3 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 4 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 5 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 6 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 7 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 8 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 9 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 10 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 11 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 12 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 13 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 14 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 15 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 16 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 17 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 18 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 19 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |
| 20 | 0.7 | 1.25 | - | - | {"max_slot": "12:00", "top_n": 3} | 104/0.755 | 133/0.759 | 0.7517 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 1.25 | mask [(none)] | premom [(none)] | guard {'max_slot': '12:00', 'top_n': 3} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=237 PF=0.757 net=Rs-27,286 win%=37.1 avgW=Rs965 avgL=Rs-753 maxDD=Rs-37,463 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.85 tradeDom=0.013 dayDom=9.99 symDom=9.99 dbp=0.9402
- **TEST  @15bps:** n=66 PF=0.512 net=Rs-16,519 win%=27.3 avgW=Rs963 avgL=Rs-705 maxDD=Rs-20,160 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.0 tradeDom=0.064 dayDom=9.99 symDom=9.99 dbp=0.9702
- **TRAIN @5bps:**  n=237 PF=0.757 net=Rs-27,286 win%=37.1 avgW=Rs965 avgL=Rs-753 maxDD=Rs-37,463 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.85 tradeDom=0.013 dayDom=9.99 symDom=9.99 dbp=0.9402
- **TEST  @5bps:**  n=66 PF=0.512 net=Rs-16,519 win%=27.3 avgW=Rs963 avgL=Rs-705 maxDD=Rs-20,160 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.0 tradeDom=0.064 dayDom=9.99 symDom=9.99 dbp=0.9702

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vA --trials 400 --time_budget_min 5.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```