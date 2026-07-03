# MR_VWAP_EXTREME_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.001852', 'body_pct<=0.933341', 'body_pct>=0.478834', 'close_loc<=0.966388', 'close_loc<=1.0', 'lower_wick_pct<=0.002632', 'quality_score<=58.718627', 'ranker_score>=52.794817', 'rs_pct>=0.454766', 'rs_pct>=0.822409', 'signal_range_pct<=0.160156', 'upper_wick_pct<=0.026055'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 2 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 3 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 4 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 5 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 6 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 7 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 8 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 9 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 10 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 11 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 12 | 1.2 | 2.0 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.702 | 30/0.704 | 0.7015 |
| 13 | 1.0 | 1.25 | - | sig5_rsi_dir<=46.939049;pre1_adx<=30.175745 | {"top_n": 3} | 16/1.189 | 17/0.811 | 0.6599 |
| 14 | 1.2 | 1.5 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.715 | 30/0.667 | 0.6471 |
| 15 | 1.2 | 1.5 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.715 | 30/0.667 | 0.6471 |
| 16 | 1.2 | 1.5 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.715 | 30/0.667 | 0.6471 |
| 17 | 1.2 | 1.5 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.715 | 30/0.667 | 0.6471 |
| 18 | 1.2 | 1.5 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.715 | 30/0.667 | 0.6471 |
| 19 | 1.2 | 1.5 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.715 | 30/0.667 | 0.6471 |
| 20 | 1.2 | 1.5 | - | sig5_rsi_dir<=46.939049 | {"min_slot": "09:30", "top_n": 2} | 26/0.715 | 30/0.667 | 0.6471 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.2/Tgt 2.0 | mask [(none)] | premom [sig5_rsi_dir<=46.939049] | guard {'min_slot': '09:30', 'top_n': 2} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=56 PF=0.703 net=Rs-6,170 win%=41.1 avgW=Rs636 avgL=Rs-630 maxDD=Rs-9,885 SL/TGT/EOD=4/1/51 tpd=1.87 tradeDom=0.12 dayDom=9.99 symDom=9.99 dbp=0.8552
- **TEST  @15bps:** n=7 PF=0.232 net=Rs-2,989 win%=28.6 avgW=Rs452 avgL=Rs-779 maxDD=Rs-2,505 SL/TGT/EOD=2/0/5 tpd=2.33 tradeDom=0.825 dayDom=9.99 symDom=9.99 dbp=0.9624
- **TRAIN @5bps:**  n=56 PF=0.966 net=Rs-608 win%=46.4 avgW=Rs657 avgL=Rs-590 maxDD=Rs-7,904 SL/TGT/EOD=4/1/51 tpd=1.87 tradeDom=0.108 dayDom=9.99 symDom=9.99 dbp=0.544
- **TEST  @5bps:**  n=7 PF=0.326 net=Rs-2,297 win%=42.9 avgW=Rs370 avgL=Rs-852 maxDD=Rs-2,116 SL/TGT/EOD=2/0/5 tpd=2.33 tradeDom=0.761 dayDom=9.99 symDom=9.99 dbp=0.9624

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup MR_VWAP_EXTREME_RECLAIM_LONG --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\MR_VWAP_EXTREME_RECLAIM_LONG --trials 200 --time_budget_min 8.0 --seed 7
```