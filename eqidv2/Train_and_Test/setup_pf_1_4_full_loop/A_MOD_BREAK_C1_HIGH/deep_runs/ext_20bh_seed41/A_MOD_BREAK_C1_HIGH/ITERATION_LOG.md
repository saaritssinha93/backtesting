# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 18 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['cci_x<=149.866354', 'ema20_slope5_atr<=0.392899', 'ema50_dist_atr<=3.056342', 'lower_wick_pct<=0.0', 'lower_wick_pct>=0.0', 'pre1_ret_atr<=1.582213', 'quality_score>=58.547077', 'rs_pct>=3.289908', 'vwap_hold_bars>=0.0'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 1.5 | - | - | {"min_slot": "10:30", "max_slot": "14:30"} | 775/0.535 | 834/0.536 | 0.5341 |
| 2 | 1.5 | 1.25 | - | sig5_adx_calc>=16.133893;pre_entry_momentum_score>=53.633318 | {"min_slot": "09:30", "max_slot": "11:05"} | 170/0.533 | 160/0.553 | 0.5164 |
| 3 | 1.5 | 1.5 | - | - | {"min_slot": "09:30"} | 871/0.542 | 924/0.516 | 0.4951 |
| 4 | 1.5 | 1.25 | - | - | {"min_slot": "09:30"} | 921/0.502 | 1000/0.497 | 0.4928 |
| 5 | 1.5 | 1.25 | - | - | {"min_slot": "09:30"} | 921/0.502 | 1000/0.497 | 0.4928 |
| 6 | 1.1 | 2.0 | rs_pct>=3.289908 | - | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 2} | 136/0.523 | 120/0.497 | 0.4766 |
| 7 | 1.1 | 1.0 | vwap_hold_bars>=0.0 | pre_entry_momentum_score>=41.291826 | {"min_slot": "10:30", "max_slot": "14:30"} | 863/0.429 | 919/0.457 | 0.4072 |
| 8 | 1.2 | 1.25 | lower_wick_pct>=0.0;notional_5m_rs<=9710680.9 | sig5_rsi_dir<=72.960401 | {"min_slot": "10:00", "max_slot": "11:05"} | 49/0.545 | 55/0.723 | 0.4023 |
| 9 | 0.85 | 2.5 | - | - | {"max_slot": "12:30", "top_n": 2} | 423/0.561 | 492/0.462 | 0.3827 |
| 10 | 1.5 | 1.5 | - | pre3_close_pos>=0.709959 | {"min_slot": "11:00"} | 806/0.492 | 781/0.413 | 0.3507 |
| 11 | 0.5 | 1.5 | - | sig5_adx_calc>=22.259292 | {"min_slot": "09:30"} | 1612/0.388 | 1810/0.341 | 0.3033 |
| 12 | 1.0 | 1.0 | cci_x<=149.866354 | pre1_adx>=54.85292 | {"min_slot": "10:00", "max_slot": "13:00", "top_n": 3} | 24/0.332 | 13/0.314 | 0.3 |
| 13 | 0.7 | 0.6 | quality_score>=58.547077 | pre1_adx<=19.647271;pre3_close_pos>=0.457968 | {"max_slot": "12:30", "top_n": 3} | 40/0.312 | 35/0.449 | 0.2036 |
| 14 | 1.0 | 0.6 | - | sig5_vol_ratio20>=3.288703;pre1_adx>=35.22109 | {"max_slot": "11:05"} | 28/0.478 | 47/0.272 | 0.1079 |
| 15 | 0.85 | 2.5 | lower_wick_pct<=0.0 | sig5_adx_calc>=47.700816;pre_entry_momentum_score>=75.768898 | {"top_n": 1} | 7/0.008 | 8/0.191 | -0.139 |
| 16 | 0.85 | 0.8 | pre1_ret_atr<=1.582213;range_compress3>=1.14468 | sig5_rsi_dir<=69.378704;sig5_vol_ratio20>=2.02094 | {"top_n": 1} | 19/0.427 | 37/0.108 | -0.147 |
| 17 | 1.5 | 0.6 | ema20_slope5_atr<=0.392899;adx_x>=19.335004 | pre3_range_r<=0.410561 | {"top_n": 1} | 8/0.05 | 20/0.39 | -0.2225 |
| 18 | 0.7 | 2.5 | ema50_dist_atr<=3.056342;ema20_slope5_atr>=1.093151 | pre5_mom_r<=0.383945;sig5_rsi_dir>=69.378704 | {"max_slot": "14:00", "top_n": 1} | 3/0.085 | 3/1.022 | -4.5 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 1.5 | mask [(none)] | premom [(none)] | guard {'min_slot': '10:30', 'max_slot': '14:30'} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=1609 PF=0.535 net=Rs-559,527 win%=39.2 avgW=Rs1,022 avgL=Rs-1,231 maxDD=Rs-557,798 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=30.94 tradeDom=0.002 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @15bps:** n=525 PF=0.412 net=Rs-251,587 win%=34.5 avgW=Rs973 avgL=Rs-1,243 maxDD=Rs-251,128 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=23.86 tradeDom=0.007 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=1609 PF=0.535 net=Rs-559,527 win%=39.2 avgW=Rs1,022 avgL=Rs-1,231 maxDD=Rs-557,798 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=30.94 tradeDom=0.002 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @5bps:**  n=525 PF=0.412 net=Rs-251,587 win%=34.5 avgW=Rs973 avgL=Rs-1,243 maxDD=Rs-251,128 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=23.86 tradeDom=0.007 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_20bh --trials 600 --time_budget_min 14.0 --seed 41 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```