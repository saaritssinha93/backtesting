# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002568', 'atr_pct<=0.002854', 'atr_pct>=0.00214', 'atr_pct>=0.002568', 'atr_pct>=0.002854', 'atr_pct>=0.003166', 'atr_pct>=0.005406', 'body_pct<=0.73913', 'body_pct>=0.539293', 'body_pct>=0.6', 'body_pct>=0.691084', 'body_pct>=0.73913'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 15/1.908 | 6/1.564 | 1.3073 |
| 2 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 3 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 4 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 5 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 6 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 7 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 8 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 9 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 10 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 11 | 1.2 | 2.5 | close_loc>=0.929858 | sig5_adx_calc<=31.269015 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 16/2.343 | 6/1.564 | 0.9591 |
| 12 | 1.2 | 2.5 | close_loc>=0.818182 | sig5_adx_calc<=43.221248 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 37/0.894 | 19/0.975 | 0.8286 |
| 13 | 1.2 | 2.5 | close_loc>=0.818182 | sig5_adx_calc<=43.221248 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 37/0.894 | 19/0.975 | 0.8286 |
| 14 | 1.2 | 2.5 | close_loc>=0.818182 | sig5_adx_calc<=43.221248 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 37/0.894 | 19/0.975 | 0.8286 |
| 15 | 1.2 | 2.5 | close_loc>=0.818182 | sig5_adx_calc<=43.221248 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 37/0.894 | 19/0.975 | 0.8286 |
| 16 | 1.2 | 2.5 | close_loc>=0.818182 | sig5_adx_calc<=43.221248 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 37/0.894 | 19/0.975 | 0.8286 |
| 17 | 1.2 | 2.5 | close_loc>=0.818182 | sig5_adx_calc<=43.221248 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 37/0.894 | 19/0.975 | 0.8286 |
| 18 | 1.2 | 2.5 | close_loc>=0.818182 | sig5_adx_calc<=35.227914 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 35/1.024 | 16/1.284 | 0.8168 |
| 19 | 1.2 | 2.5 | close_loc>=0.929858 | pre1_adx<=49.865161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 18/1.016 | 8/1.287 | 0.7984 |
| 20 | 1.2 | 2.5 | close_loc>=0.929858 | pre1_adx<=49.865161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 3} | 18/1.016 | 8/1.287 | 0.7984 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.2/Tgt 2.5 | mask [close_loc>=0.929858] | premom [sig5_adx_calc<=31.269015] | guard {'min_slot': '09:30', 'max_slot': '12:00', 'top_n': 2} | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=21 PF=1.779 net=Rs6,433 win%=47.6 avgW=Rs1,469 avgL=Rs-750 maxDD=Rs-4,874 SL/TGT/EOD=2/6/13 tgt%=28.6 tpd=2.1 tradeDom=0.154 dayDom=0.58 symDom=0.352 dbp=0.153
- **TEST  @15bps:** n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=None dayDom=None symDom=None dbp=None
- **TRAIN @5bps:**  n=21 PF=2.188 net=Rs8,510 win%=47.6 avgW=Rs1,567 avgL=Rs-651 maxDD=Rs-3,779 SL/TGT/EOD=2/6/13 tgt%=28.6 tpd=2.1 tradeDom=0.151 dayDom=0.474 symDom=0.278 dbp=0.0896
- **TEST  @5bps:**  n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=None dayDom=None symDom=None dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `TRAIN PF above preferred band (>1.70)`

- **Keep/reject:** REJECT  — TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6); TRAIN PF above preferred band (>1.70)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_C1_CLOSE_RECLAIM_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_C1_CLOSE_RECLAIM_BREAK --trials 700 --time_budget_min 10.0 --seed 47 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```