# G_HIGHER_HIGH_BREAK (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.00275', 'atr_pct<=0.003216', 'atr_pct<=0.00357', 'atr_pct<=0.003972', 'body_pct<=0.845619', 'body_pct<=0.947891', 'body_pct>=0.975029', 'close_loc<=0.643734', 'close_loc<=0.712955', 'close_loc<=0.773314', 'close_loc<=0.834242', 'close_loc<=0.884615'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 2 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 3 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 4 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 5 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 6 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 7 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 8 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 9 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 10 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 11 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 12 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 13 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 14 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 15 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 16 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 17 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 18 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 19 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |
| 20 | 1.5 | 1.5 | close_loc<=0.994667 | - | {"max_slot": "12:30"} | 12/1.214 | 6/1.893 | 0.9422 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 1.5 | mask [close_loc<=0.994667] | premom [(none)] | guard {'max_slot': '12:30'} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=18 PF=1.424 net=Rs3,658 win%=61.1 avgW=Rs1,117 avgL=Rs-1,233 maxDD=Rs-3,812 SL/TGT/EOD=2/9/7 tpd=2.57 tradeDom=0.103 dayDom=1.188 symDom=0.346 dbp=0.2608
- **TEST  @15bps:** n=11 PF=0.613 net=Rs-2,968 win%=45.5 avgW=Rs940 avgL=Rs-1,278 maxDD=Rs-3,751 SL/TGT/EOD=3/3/5 tpd=3.67 tradeDom=0.27 dayDom=9.99 symDom=9.99 dbp=0.9624
- **TRAIN @5bps:**  n=18 PF=1.681 net=Rs5,406 win%=61.1 avgW=Rs1,214 avgL=Rs-1,135 maxDD=Rs-3,315 SL/TGT/EOD=2/9/7 tpd=2.57 tradeDom=0.102 dayDom=0.874 symDom=0.253 dbp=0.1669
- **TEST  @5bps:**  n=11 PF=0.739 net=Rs-1,871 win%=54.5 avgW=Rs883 avgL=Rs-1,434 maxDD=Rs-3,054 SL/TGT/EOD=3/3/5 tpd=3.67 tradeDom=0.258 dayDom=9.99 symDom=9.99 dbp=0.7424

- **Keep/reject:** REJECT  — TRAIN PF too low / too few trades (train_n<20); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_HIGHER_HIGH_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_HIGHER_HIGH_BREAK --trials 220 --time_budget_min 9.0 --seed 7
```