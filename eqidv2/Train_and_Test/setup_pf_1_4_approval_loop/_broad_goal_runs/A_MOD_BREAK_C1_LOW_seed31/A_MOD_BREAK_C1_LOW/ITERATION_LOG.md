# A_MOD_BREAK_C1_LOW (SHORT) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003328', 'atr_pct>=0.001782', 'atr_pct>=0.002176', 'atr_pct>=0.002381', 'atr_pct>=0.002921', 'atr_pct>=0.003328', 'atr_pct>=0.004014', 'body_pct<=0.540541', 'body_pct<=0.610967', 'body_pct<=0.744186', 'body_pct<=0.789474', 'body_pct<=0.834293'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 2.5 | body_pct>=0.976744;atr_pct>=0.001522 | - | {"top_n": 1} | 26/1.059 | 17/1.077 | 1.0448 |
| 2 | 1.0 | 2.5 | body_pct>=0.976744;atr_pct>=0.001522 | - | {"top_n": 1} | 26/1.059 | 17/1.077 | 1.0448 |
| 3 | 1.0 | 2.5 | body_pct>=0.976744;atr_pct>=0.001522 | - | {"top_n": 1} | 26/1.059 | 17/1.077 | 1.0448 |
| 4 | 1.1 | 2.5 | body_pct>=0.976744;lower_wick_pct>=0.0 | pre5_mom_r>=0.11202;pre3_close_pos>=0.841589 | {"top_n": 1} | 10/1.065 | 14/1.116 | 1.0253 |
| 5 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 6 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 7 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 8 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 9 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 10 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 11 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 12 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 13 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 14 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 15 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 16 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 17 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 18 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 19 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |
| 20 | 1.1 | 2.5 | body_pct>=0.976744;signal_range_pct>=0.184534 | - | {"top_n": 1} | 25/1.05 | 19/1.106 | 1.0052 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 2.5 | mask [body_pct>=0.976744; atr_pct>=0.001522] | premom [(none)] | guard {'top_n': 1} | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=43 PF=1.067 net=Rs1,251 win%=46.5 avgW=Rs992 avgL=Rs-808 maxDD=Rs-5,322 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=4.3 tradeDom=0.114 dayDom=3.37 symDom=1.808 dbp=0.4471
- **TEST  @15bps:** n=10 PF=0.002 net=Rs-8,996 win%=10.0 avgW=Rs22 avgL=Rs-1,002 maxDD=Rs-7,764 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=43 PF=1.067 net=Rs1,251 win%=46.5 avgW=Rs992 avgL=Rs-808 maxDD=Rs-5,322 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=4.3 tradeDom=0.114 dayDom=3.37 symDom=1.808 dbp=0.4471
- **TEST  @5bps:**  n=10 PF=0.002 net=Rs-8,996 win%=10.0 avgW=Rs22 avgL=Rs-1,002 maxDD=Rs-7,764 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_LOW --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\A_MOD_BREAK_C1_LOW --trials 700 --time_budget_min 10.0 --seed 31 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```