# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-02. Optimizer: Optuna TPE. 12 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003086', 'body_pct>=0.959261', 'lower_wick_pct>=0.018405', 'rs_pct>=5.080001', 'signal_range_pct<=0.445881', 'upper_wick_pct>=0.0', 'upper_wick_pct>=0.070754', 'vwap_dist_atr<=2.306247'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 0.8 | atr_pct<=0.003086 | pre5_mom_r<=0.171743;sig5_vol_ratio20<=1.735451 | {"top_n": 1} | 31/0.271 | 58/0.303 | 0.2447 |
| 2 | 1.5 | 0.8 | upper_wick_pct>=0.0 | sig5_vol_ratio20<=2.350783 | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 3} | 613/0.338 | 672/0.279 | 0.2321 |
| 3 | 0.85 | 1.5 | upper_wick_pct>=0.070754 | sig5_vol_ratio20>=3.018104 | {"top_n": 3} | 244/0.417 | 358/0.311 | 0.2257 |
| 4 | 0.7 | 1.5 | lower_wick_pct>=0.018405 | - | {"max_slot": "12:30", "top_n": 3} | 366/0.424 | 374/0.312 | 0.2233 |
| 5 | 1.1 | 2.0 | - | - | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 3} | 341/0.357 | 360/0.275 | 0.2104 |
| 6 | 0.7 | 2.0 | - | sig5_rsi_dir<=71.532269;pre_entry_momentum_score>=65.002483 | {"top_n": 3} | 477/0.435 | 540/0.267 | 0.1326 |
| 7 | 1.5 | 0.8 | signal_range_pct<=0.445881 | pre_entry_momentum_score<=65.002483 | {"min_slot": "10:00"} | 708/0.296 | 713/0.184 | 0.0943 |
| 8 | 1.0 | 0.8 | body_pct>=0.959261 | sig5_adx_calc>=25.397455;pre3_range_r>=0.170208 | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 1} | 39/0.323 | 47/0.15 | 0.0117 |
| 9 | 0.85 | 1.0 | rs_pct>=5.080001;atr_pct<=0.003086 | - | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 2} | 23/0.213 | 12/0.078 | -0.0312 |
| 10 | 0.7 | 1.5 | vwap_dist_atr<=2.306247 | sig5_vol_ratio20<=2.131444;sig5_vol_ratio20>=2.131444 | {"min_slot": "09:30", "top_n": 3} | 0/0.0 | 0/0.0 | -5.0 |
| 11 | 0.85 | 1.0 | vwap_dist_atr<=2.306247;lower_wick_pct<=0.0 | pre3_range_r>=0.76365;pre3_close_pos>=0.923663 | {"max_slot": "12:00", "top_n": 2} | 0/0.0 | 0/0.0 | -5.0 |
| 12 | 1.0 | 1.25 | atr_pct<=0.003086;signal_range_pct>=0.708065 | pre5_mom_r<=0.171743 | {"top_n": 1} | 0/0.0 | 0/0.0 | -5.0 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 0.8 | mask [atr_pct<=0.003086] | premom [pre5_mom_r<=0.171743; sig5_vol_ratio20<=1.735451] | guard {'top_n': 1} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=89 PF=0.293 net=Rs-31,727 win%=33.7 avgW=Rs438 avgL=Rs-760 maxDD=Rs-32,105 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.043 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @15bps:** n=59 PF=0.255 net=Rs-23,225 win%=30.5 avgW=Rs441 avgL=Rs-760 maxDD=Rs-25,357 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.47 tradeDom=0.071 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=89 PF=0.293 net=Rs-31,727 win%=33.7 avgW=Rs438 avgL=Rs-760 maxDD=Rs-32,105 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.043 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @5bps:**  n=59 PF=0.255 net=Rs-23,225 win%=30.5 avgW=Rs441 avgL=Rs-760 maxDD=Rs-25,357 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.47 tradeDom=0.071 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_full --trials 700 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```