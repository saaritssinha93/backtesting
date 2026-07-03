# MR_CONTROLLED_VWAP_EXTREME_FADE_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.001942', 'atr_pct>=0.002657', 'atr_pct>=0.002911', 'body_pct<=0.374399', 'body_pct>=0.137453', 'body_pct>=0.259772', 'body_pct>=0.308143', 'body_pct>=0.374399', 'body_pct>=0.407843', 'body_pct>=0.451056', 'close_loc<=0.779219', 'close_loc<=0.968881'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 2 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 3 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 4 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 5 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 6 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 7 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 8 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 9 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 10 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 11 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 12 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 13 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 14 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 15 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 16 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 17 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 18 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 19 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |
| 20 | 0.7 | 1.0 | vwap_dist_atr>=-2.830013 | - | {"max_slot": "14:30", "top_n": 1} | 6/1.643 | 8/2.005 | 1.5159 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 1.0 | mask [vwap_dist_atr>=-2.830013] | premom [(none)] | guard {'max_slot': '14:30', 'top_n': 1} | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=14 PF=1.826 net=Rs3,109 win%=64.3 avgW=Rs764 avgL=Rs-753 maxDD=Rs-971 SL/TGT/EOD=3/9/2 tpd=1.4 tradeDom=0.111 dayDom=0.492 symDom=0.247 dbp=0.0969
- **TEST  @15bps:** n=5 PF=0.0 net=Rs-3,535 win%=0.0 avgW=Rs0 avgL=Rs-707 maxDD=Rs-2,604 SL/TGT/EOD=3/0/2 tpd=1.25 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=14 PF=2.38 net=Rs4,512 win%=64.3 avgW=Rs865 avgL=Rs-654 maxDD=Rs-832 SL/TGT/EOD=3/9/2 tpd=1.4 tradeDom=0.111 dayDom=0.384 symDom=0.192 dbp=0.0354
- **TEST  @5bps:**  n=5 PF=0.0 net=Rs-3,041 win%=0.0 avgW=Rs0 avgL=Rs-608 maxDD=Rs-2,209 SL/TGT/EOD=3/0/2 tpd=1.25 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0

- **Keep/reject:** REJECT  — TRAIN PF too low / too few trades (train_n<20); too few trades (test_n<6); TRAIN PF too high / overfit risk (>1.70); TEST PF below 1.40; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup MR_CONTROLLED_VWAP_EXTREME_FADE_LONG --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\MR_CONTROLLED_VWAP_EXTREME_FADE_LONG --trials 200 --time_budget_min 8.0 --seed 7
```