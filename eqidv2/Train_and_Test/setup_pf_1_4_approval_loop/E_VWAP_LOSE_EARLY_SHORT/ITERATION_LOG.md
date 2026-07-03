# E_VWAP_LOSE_EARLY_SHORT (SHORT) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003239', 'atr_pct<=0.003585', 'atr_pct<=0.004047', 'atr_pct<=0.004977', 'atr_pct<=0.006501', 'atr_pct>=0.006963', 'body_pct<=0.75', 'body_pct>=0.769762', 'body_pct>=0.859011', 'close_loc<=0.189474', 'close_loc<=0.277607', 'close_loc>=0.1'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 2.5 | atr_pct<=0.004977;upper_wick_pct<=0.043922 | - | {"top_n": 1} | 14/1.176 | 7/1.124 | 1.1035 |
| 2 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 3 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 4 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 5 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 6 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 7 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 8 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 9 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 10 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 11 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 12 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 13 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 14 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 15 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 16 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 17 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 18 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 19 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |
| 20 | 0.7 | 1.0 | lower_wick_pct<=0.120865;close_loc<=0.118426 | - | {"top_n": 1} | 13/0.794 | 6/0.814 | 0.7863 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 2.5 | mask [atr_pct<=0.004977; upper_wick_pct<=0.043922] | premom [(none)] | guard {'top_n': 1} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=21 PF=1.155 net=Rs1,429 win%=52.4 avgW=Rs966 avgL=Rs-919 maxDD=Rs-2,775 SL/TGT/EOD=10/0/11 tpd=1.75 tradeDom=0.152 dayDom=1.128 symDom=1.128 dbp=0.3657
- **TEST  @15bps:** n=11 PF=0.457 net=Rs-2,761 win%=45.5 avgW=Rs465 avgL=Rs-847 maxDD=Rs-3,222 SL/TGT/EOD=5/0/6 tpd=2.75 tradeDom=0.629 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=21 PF=1.423 net=Rs3,469 win%=52.4 avgW=Rs1,061 avgL=Rs-820 maxDD=Rs-2,475 SL/TGT/EOD=10/0/11 tpd=1.75 tradeDom=0.147 dayDom=0.493 symDom=0.493 dbp=0.2138
- **TEST  @5bps:**  n=11 PF=0.623 net=Rs-1,688 win%=45.5 avgW=Rs559 avgL=Rs-747 maxDD=Rs-2,821 SL/TGT/EOD=5/0/6 tpd=2.75 tradeDom=0.558 dayDom=9.99 symDom=9.99 dbp=0.9483

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup E_VWAP_LOSE_EARLY_SHORT --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --trials 220 --time_budget_min 9.0 --seed 7
```