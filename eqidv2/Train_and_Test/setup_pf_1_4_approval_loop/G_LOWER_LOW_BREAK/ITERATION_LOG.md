# G_LOWER_LOW_BREAK (SHORT) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001805', 'atr_pct<=0.002145', 'atr_pct<=0.002365', 'body_pct<=0.68', 'body_pct<=0.850556', 'body_pct>=0.819248', 'body_pct>=1.0', 'close_loc<=0.0', 'close_loc<=0.054386', 'lower_wick_pct<=0.0', 'lower_wick_pct<=0.092528', 'lower_wick_pct>=0.018179'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;lower_wick_pct>=0.0 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 34/1.794 | 21/1.651 | 1.6565 |
| 2 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 3 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 4 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 5 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 6 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 7 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 8 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 9 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 10 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 11 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 12 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 13 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "10:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 14 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 15 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 16 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 17 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 18 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 19 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |
| 20 | 1.2 | 1.0 | vwap_dist_atr<=-3.89879;quality_score>=37.224911 | pre5_mom_r<=0.261893 | {"min_slot": "11:00"} | 25/1.679 | 15/1.864 | 1.6503 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.2/Tgt 1.0 | mask [vwap_dist_atr<=-3.89879; lower_wick_pct>=0.0] | premom [pre5_mom_r<=0.261893] | guard {'min_slot': '11:00'} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=55 PF=1.733 net=Rs8,989 win%=60.0 avgW=Rs644 avgL=Rs-557 maxDD=Rs-3,977 SL/TGT/EOD=4/23/28 tpd=4.58 tradeDom=0.036 dayDom=0.257 symDom=0.17 dbp=0.0001
- **TEST  @15bps:** n=5 PF=0.387 net=Rs-1,401 win%=40.0 avgW=Rs443 avgL=Rs-763 maxDD=Rs-2,164 SL/TGT/EOD=1/1/3 tpd=2.5 tradeDom=0.86 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=55 PF=2.416 net=Rs14,430 win%=63.6 avgW=Rs703 avgL=Rs-509 maxDD=Rs-3,049 SL/TGT/EOD=4/23/28 tpd=4.58 tradeDom=0.035 dayDom=0.201 symDom=0.119 dbp=0.0
- **TEST  @5bps:**  n=5 PF=0.545 net=Rs-904 win%=40.0 avgW=Rs542 avgL=Rs-663 maxDD=Rs-1,963 SL/TGT/EOD=1/1/3 tpd=2.5 tradeDom=0.794 dayDom=9.99 symDom=9.99 dbp=None

- **Keep/reject:** REJECT  — too few trades (test_n<6); TRAIN PF too high / overfit risk (>1.70); TEST PF below 1.40; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_LOWER_LOW_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_LOWER_LOW_BREAK --trials 220 --time_budget_min 9.0 --seed 7
```