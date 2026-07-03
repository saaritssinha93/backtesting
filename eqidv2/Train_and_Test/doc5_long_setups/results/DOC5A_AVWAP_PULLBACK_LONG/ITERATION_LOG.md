# DOC5A_AVWAP_PULLBACK_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.004048', 'atr_pct<=0.005325', 'atr_pct>=0.002118', 'atr_pct>=0.002526', 'atr_pct>=0.003586', 'atr_pct>=0.004048', 'atr_pct>=0.004685', 'atr_pct>=0.005325', 'atr_pct>=0.006595', 'body_pct>=0.50755', 'body_pct>=0.651688', 'body_pct>=0.695774'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"max_slot": "12:30", "top_n": 1} | 26/0.884 | 26/0.938 | 0.8404 |
| 2 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"max_slot": "12:30", "top_n": 1} | 26/0.884 | 26/0.938 | 0.8404 |
| 3 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"max_slot": "12:30", "top_n": 1} | 26/0.884 | 26/0.938 | 0.8404 |
| 4 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"max_slot": "12:30", "top_n": 1} | 26/0.884 | 26/0.938 | 0.8404 |
| 5 | 1.5 | 1.25 | rs_pct>=1.045161 | pre3_range_r>=0.142838 | {"top_n": 2} | 45/0.962 | 41/1.133 | 0.8248 |
| 6 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"max_slot": "13:00", "top_n": 1} | 29/0.801 | 28/0.827 | 0.781 |
| 7 | 1.5 | 1.25 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | - | 18/0.941 | 20/0.815 | 0.714 |
| 8 | 1.5 | 1.25 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | - | 18/0.941 | 20/0.815 | 0.714 |
| 9 | 1.5 | 1.25 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | - | 18/0.941 | 20/0.815 | 0.714 |
| 10 | 0.85 | 1.25 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.964 | 28/0.815 | 0.6965 |
| 11 | 0.85 | 1.25 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.964 | 28/0.815 | 0.6965 |
| 12 | 1.5 | 1.25 | quality_score>=79.745625 | pre3_range_r>=0.174936 | - | 47/0.701 | 33/0.722 | 0.6837 |
| 13 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |
| 14 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |
| 15 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |
| 16 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |
| 17 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |
| 18 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |
| 19 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |
| 20 | 0.85 | 1.0 | rs_pct>=1.045161 | pre3_range_r>=0.287846 | {"top_n": 1} | 32/0.741 | 28/0.827 | 0.6719 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 1.0 | mask [rs_pct>=1.045161] | premom [pre3_range_r>=0.287846] | guard {'max_slot': '12:30', 'top_n': 1} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=52 PF=0.911 net=Rs-2,063 win%=57.7 avgW=Rs701 avgL=Rs-1,049 maxDD=Rs-3,646 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.93 tradeDom=0.036 dayDom=9.99 symDom=9.99 dbp=0.6287
- **TEST  @15bps:** n=18 PF=0.473 net=Rs-5,368 win%=38.9 avgW=Rs687 avgL=Rs-925 maxDD=Rs-7,026 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.38 tradeDom=0.159 dayDom=9.99 symDom=9.99 dbp=0.9462
- **TRAIN @5bps:**  n=52 PF=0.911 net=Rs-2,063 win%=57.7 avgW=Rs701 avgL=Rs-1,049 maxDD=Rs-3,646 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.93 tradeDom=0.036 dayDom=9.99 symDom=9.99 dbp=0.6287
- **TEST  @5bps:**  n=18 PF=0.473 net=Rs-5,368 win%=38.9 avgW=Rs687 avgL=Rs-925 maxDD=Rs-7,026 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.38 tradeDom=0.159 dayDom=9.99 symDom=9.99 dbp=0.9462

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5A_AVWAP_PULLBACK_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```