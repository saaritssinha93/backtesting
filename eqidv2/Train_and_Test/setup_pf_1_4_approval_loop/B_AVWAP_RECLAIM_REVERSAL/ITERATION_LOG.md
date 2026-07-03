# B_AVWAP_RECLAIM_REVERSAL (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002413', 'atr_pct<=0.002673', 'atr_pct>=0.002166', 'atr_pct>=0.005333', 'body_pct<=0.565217', 'body_pct<=0.632822', 'body_pct<=0.992271', 'body_pct>=0.565217', 'body_pct>=0.69454', 'body_pct>=0.75', 'body_pct>=0.789474', 'body_pct>=0.833981'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 2 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 3 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 4 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 5 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 6 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 7 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 8 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 1} | 13/0.819 | 8/0.941 | 0.7709 |
| 9 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 3} | 19/0.568 | 9/0.731 | 0.502 |
| 10 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 3} | 19/0.568 | 9/0.731 | 0.502 |
| 11 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 3} | 19/0.568 | 9/0.731 | 0.502 |
| 12 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 3} | 19/0.568 | 9/0.731 | 0.502 |
| 13 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 3} | 19/0.568 | 9/0.731 | 0.502 |
| 14 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 3} | 19/0.568 | 9/0.731 | 0.502 |
| 15 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=57.996594 | {"max_slot": "14:00", "top_n": 3} | 19/0.568 | 9/0.731 | 0.502 |
| 16 | 0.5 | 2.0 | quality_score<=55.020613 | pre1_adx>=37.246008 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 1} | 17/0.597 | 8/0.968 | 0.4488 |
| 17 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=50.369725 | {"max_slot": "14:00", "top_n": 3} | 42/0.477 | 23/0.582 | 0.4347 |
| 18 | 0.7 | 1.5 | body_pct>=0.992271 | sig5_rsi_dir>=50.369725 | {"max_slot": "14:00", "top_n": 3} | 42/0.477 | 23/0.582 | 0.4347 |
| 19 | 0.5 | 2.0 | quality_score<=86.941182 | pre1_adx>=37.246008 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 1} | 34/0.419 | 31/0.452 | 0.4067 |
| 20 | 1.0 | 1.5 | quality_score>=117.141996 | sig5_vol_ratio20>=1.315492 | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 3} | 30/0.426 | 24/0.501 | 0.3958 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 1.5 | mask [body_pct>=0.992271] | premom [sig5_rsi_dir>=57.996594] | guard {'max_slot': '14:00', 'top_n': 1} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=21 PF=0.865 net=Rs-1,179 win%=33.3 avgW=Rs1,077 avgL=Rs-623 maxDD=Rs-4,381 SL/TGT/EOD=8/5/8 tpd=2.1 tradeDom=0.168 dayDom=9.99 symDom=9.99 dbp=0.6168
- **TEST  @15bps:** n=1 PF=0.0 net=Rs-921 win%=0.0 avgW=Rs0 avgL=Rs-921 maxDD=Rs0 SL/TGT/EOD=1/0/0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=21 PF=1.124 net=Rs919 win%=38.1 avgW=Rs1,041 avgL=Rs-570 maxDD=Rs-3,292 SL/TGT/EOD=8/5/8 tpd=2.1 tradeDom=0.164 dayDom=3.062 symDom=1.489 dbp=0.4264
- **TEST  @5bps:**  n=1 PF=0.0 net=Rs-823 win%=0.0 avgW=Rs0 avgL=Rs-823 maxDD=Rs0 SL/TGT/EOD=1/0/0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None

- **Keep/reject:** REJECT  — too few trades (test_n<6); TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --trials 220 --time_budget_min 9.0 --seed 7
```