# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.00238', 'atr_pct<=0.002854', 'atr_pct<=0.003166', 'atr_pct<=0.003592', 'body_pct<=0.539293', 'body_pct<=0.691084', 'body_pct<=0.789963', 'body_pct<=0.935439', 'body_pct>=0.935439', 'close_loc<=0.659408', 'close_loc<=0.772089', 'close_loc<=0.818182'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 2 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 3 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 4 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 5 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 6 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 7 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 8 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 9 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 10 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 11 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 12 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 13 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 14 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 15 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "max_slot": "14:30", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 16 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 17 | 1.0 | 2.5 | quality_score<=49.69124 | - | {"min_slot": "10:00", "top_n": 1} | 59/0.585 | 45/0.611 | 0.5749 |
| 18 | 1.0 | 2.5 | quality_score<=49.69124;vol_ratio<=6.597919 | - | {"min_slot": "10:00", "max_slot": "14:30", "top_n": 1} | 59/0.585 | 44/0.617 | 0.5724 |
| 19 | 1.0 | 2.5 | quality_score<=49.69124;vol_ratio<=6.597919 | - | {"min_slot": "10:00", "max_slot": "14:30", "top_n": 1} | 59/0.585 | 44/0.617 | 0.5724 |
| 20 | 1.0 | 2.5 | quality_score<=49.69124;vol_ratio<=6.597919 | - | {"min_slot": "10:00", "max_slot": "14:30", "top_n": 1} | 59/0.585 | 44/0.617 | 0.5724 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 2.5 | mask [quality_score<=49.69124] | premom [(none)] | guard {'min_slot': '10:00', 'top_n': 1} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=104 PF=0.595 net=Rs-24,980 win%=29.8 avgW=Rs1,186 avgL=Rs-846 maxDD=Rs-29,841 SL/TGT/EOD=38/10/56 tpd=7.43 tradeDom=0.062 dayDom=9.99 symDom=9.99 dbp=0.9322
- **TEST  @15bps:** n=18 PF=0.741 net=Rs-2,294 win%=38.9 avgW=Rs937 avgL=Rs-805 maxDD=Rs-2,933 SL/TGT/EOD=4/1/13 tpd=4.5 tradeDom=0.345 dayDom=9.99 symDom=9.99 dbp=0.8785
- **TRAIN @5bps:**  n=104 PF=0.732 net=Rs-14,646 win%=32.7 avgW=Rs1,179 avgL=Rs-782 maxDD=Rs-22,289 SL/TGT/EOD=38/10/56 tpd=7.43 tradeDom=0.059 dayDom=9.99 symDom=9.99 dbp=0.7987
- **TEST  @5bps:**  n=18 PF=0.937 net=Rs-501 win%=50.0 avgW=Rs821 avgL=Rs-877 maxDD=Rs-2,560 SL/TGT/EOD=4/1/13 tpd=4.5 tradeDom=0.32 dayDom=9.99 symDom=9.99 dbp=0.6322

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_C1_CLOSE_RECLAIM_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_C1_CLOSE_RECLAIM_BREAK --trials 220 --time_budget_min 9.0 --seed 7
```