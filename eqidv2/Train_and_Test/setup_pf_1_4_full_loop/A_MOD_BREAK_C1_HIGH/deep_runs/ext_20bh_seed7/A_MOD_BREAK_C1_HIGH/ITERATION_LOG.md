# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 13 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['day_ret_pct<=0.644972', 'day_ret_pct>=0.286611', 'ema20_slope5_atr>=0.540251', 'gap_pct>=1.105209', 'quality_score>=67.940675', 'rs_pct<=2.024677', 'vwap_hold_bars<=0.0'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 1.5 | quality_score>=67.940675;bb_width_pct>=1.402912 | pre3_close_pos<=0.874992 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 1} | 155/0.579 | 97/0.466 | 0.3763 |
| 2 | 1.5 | 0.8 | - | sig5_vol_ratio20<=2.71987 | {"top_n": 3} | 812/0.438 | 873/0.379 | 0.3328 |
| 3 | 0.7 | 2.0 | - | sig5_rsi_dir<=72.984066;pre_entry_momentum_score>=66.609389 | {"top_n": 3} | 586/0.464 | 589/0.386 | 0.3237 |
| 4 | 1.5 | 1.5 | quality_score>=67.940675;above_or_high>=1.0 | pre1_adx<=36.98334 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 1} | 138/0.439 | 124/0.35 | 0.2794 |
| 5 | 1.1 | 2.5 | day_ret_pct>=0.286611 | - | {"top_n": 1} | 557/0.517 | 733/0.38 | 0.2708 |
| 6 | 0.7 | 1.0 | rs_pct<=2.024677;pre3_ret_atr<=2.593293 | pre5_mom_r>=0.26621;pre5_mom_r<=0.396471 | {"min_slot": "11:00", "top_n": 3} | 132/0.38 | 192/0.299 | 0.2334 |
| 7 | 1.5 | 0.6 | vwap_hold_bars<=0.0 | - | {"max_slot": "13:00"} | 485/0.389 | 471/0.299 | 0.2269 |
| 8 | 0.5 | 0.6 | - | - | {"max_slot": "14:30", "top_n": 2} | 1293/0.238 | 1493/0.201 | 0.1712 |
| 9 | 0.7 | 1.0 | ema20_slope5_atr>=0.540251 | sig5_adx_calc>=28.150253;sig5_rsi_dir<=75.20671 | {"top_n": 1} | 203/0.402 | 237/0.273 | 0.1702 |
| 10 | 1.0 | 2.5 | - | pre3_close_pos>=0.944462;pre3_close_pos>=0.795174 | {"max_slot": "14:00", "top_n": 2} | 262/0.688 | 282/0.395 | 0.161 |
| 11 | 0.5 | 0.6 | - | pre_entry_momentum_score>=59.068604;sig5_adx_calc>=48.719426 | {"min_slot": "09:45", "top_n": 3} | 136/0.217 | 131/0.171 | 0.1334 |
| 12 | 1.2 | 2.0 | gap_pct>=1.105209;bb_width_pct>=1.175461 | sig5_rsi_dir<=75.20671 | {"min_slot": "10:00", "max_slot": "12:00"} | 180/0.65 | 50/0.341 | 0.0937 |
| 13 | 0.85 | 1.25 | day_ret_pct<=0.644972 | pre5_mom_r>=0.769707 | {"min_slot": "10:30", "top_n": 3} | 22/1.235 | 12/0.337 | -0.3808 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 1.5 | mask [quality_score>=67.940675; bb_width_pct>=1.402912] | premom [pre3_close_pos<=0.874992] | guard {'min_slot': '10:30', 'max_slot': '13:00', 'top_n': 1} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=252 PF=0.528 net=Rs-95,201 win%=40.1 avgW=Rs1,055 avgL=Rs-1,336 maxDD=Rs-100,732 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.0 tradeDom=0.012 dayDom=9.99 symDom=9.99 dbp=0.9995
- **TEST  @15bps:** n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=252 PF=0.528 net=Rs-95,201 win%=40.1 avgW=Rs1,055 avgL=Rs-1,336 maxDD=Rs-100,732 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.0 tradeDom=0.012 dayDom=9.99 symDom=9.99 dbp=0.9995
- **TEST  @5bps:**  n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_20bh --trials 600 --time_budget_min 14.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```