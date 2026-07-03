# DOC5B_MOMO_BREAKOUT_LONG (LONG) - PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-01 from `trials.csv` and requested-split rerun._

## Search Scope

- Trials: 200 Optuna TPE FIT/VAL trials.
- Optimization objective: FIT/VAL only, `reward(min(FIT_PF, VAL_PF)) - 0.80 * abs(FIT_PF - VAL_PF)`.
- TRAIN confirmation and TEST scoring were run only after selecting the best FIT/VAL config.
- Exit grid: SL `[0.50, 0.70, 0.85, 1.00, 1.10, 1.20, 1.50]`; target `[0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]`.
- Filters searched: up to 1 mask term and up to 1 pre-momentum term, plus slot guards, top_n, max_positions, and daily_loss_rs.

## Best FIT/VAL Pocket

| rank | SL | target | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---:|---:|---:|---|---|---|---|---|---:|
| 1 | 1.50 | 2.00 | `signal_range_pct<=0.388188` | none | `max_slot=12:30`, `top_n=2` | 64 / 0.477 | 61 / 0.501 | 0.4573 |
| 2 | 1.50 | 2.00 | `signal_range_pct<=0.388188` | none | `top_n=2` | 86 / 0.457 | 79 / 0.470 | 0.4476 |
| 3 | 1.50 | 2.00 | `signal_range_pct<=0.388188` | none | `max_slot=14:30`, `top_n=2` | 86 / 0.457 | 79 / 0.470 | 0.4476 |

## Confirmation Metrics For Best Pocket

| window | trades | PF | net PnL | win% | trades/day | target-fill | day-block p |
|---|---:|---:|---:|---:|---:|---:|---:|
| TRAIN | 125 | 0.488 | Rs-37,044 | 36.0 | 6.25 | 0.0% | 0.9818 |
| TEST | 48 | 0.211 | Rs-38,729 | 22.9 | 8.00 | 0.0% | 0.9864 |

## Sweep Read

- No trial got close to the TRAIN PF 1.30 floor. The best FIT/VAL score was still a losing book on both halves.
- Larger exits (`SL 1.50 / target 2.00`) and a tight signal-range mask reduced the damage but did not create an edge.
- TEST collapsed further than TRAIN, so there is no defensible approval candidate and no parameter set to promote.

## Expanded 2x2 Search

I also ran a broader but still simple search:

```powershell
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool --trials 600 --time_budget_min 12.0 --seed 11 --train_start 2026-05-18 --test_start 2026-06-20 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500 --out Train_and_Test\setup_pf_1_4_approval_loop\DOC5B_MOMO_BREAKOUT_LONG\deep_runs\mask2_pm2_seed11
```

Best expanded-search config:

- Exit: SL 0.70% / target 2.50%.
- Mask terms: none.
- Pre-momentum: `sig5_vol_ratio20 <= 1.77545`, `pre1_adx <= 44.037386`.
- Guards: `min_slot=11:00`, `max_slot=14:00`, `top_n=1`.
- Max positions: 10; daily loss stop: Rs4,000.

| window | trades | PF | net PnL | win% | trades/day | day-block p |
|---|---:|---:|---:|---:|---:|---:|
| TRAIN | 34 | 0.763 | Rs-3,557 | 41.2 | 2.27 | 0.7144 |
| TEST | 8 | 0.662 | Rs-1,517 | 37.5 | 2.00 | 0.7980 |

## Train-Band Rescore Audit

After the two searches, I rescored all tried configs on full TRAIN first:

- Trial rows read: 800.
- Unique configs rescored on TRAIN: 453.
- Configs with TRAIN PF 1.30-1.70 and at least 20 TRAIN trades: 0.
- Configs reaching TEST after meaningful TRAIN-band confirmation: 0.
- Best meaningful TRAIN rescore (`n >= 20`): PF 0.952 over 32 trades, net Rs-646.
- Only thin TRAIN-band pockets found: 7, 8, or 11 TRAIN trades, which is not meaningful enough to approve.

Audit outputs:

- `deep_train_band_rescore/SUMMARY.md`
- `deep_train_band_rescore/all_tried_configs_train_rescore.csv`
- `deep_train_band_rescore/train_band_candidates_tested.csv`

## RS/Breadth V2 Rescan

The original scanner did not implement true cross-sectional `rs_rank` or breadth. I added a DOC5B-only v2 pool builder:

- Script: `Train_and_Test/doc5_long_setups/scan_doc5b_rs_breadth_v2.py`
- Pool: `Train_and_Test/doc5_long_setups/pool_rs_breadth_v2/`
- Rows: 353 after the detector/probe gate.
- Added searchable features: `rs_rank`, `breadth_above_vwap`, `breadth_pos_ret`, `breakout_strength_atr`, plus related breakout-distance fields.

V2 optimizer result:

| window | trades | PF | net PnL | win% | trades/day | day-block p |
|---|---:|---:|---:|---:|---:|---:|
| TRAIN | 29 | 0.653 | Rs-5,767 | 41.4 | 2.23 | 0.8792 |
| TEST | 10 | 0.329 | Rs-7,157 | 20.0 | 3.33 | 1.0000 |

V2 rescore audit:

- 800 trial rows / 736 unique configs rescored.
- `n >= 20` TRAIN-band configs: 0.
- Best `n >= 20`: TRAIN PF 1.283 over 22 trades, just under the 1.30 floor.
- Exploratory `n >= 15` found one train-band near-miss: TRAIN PF 1.636 over 18 trades, but TEST PF 0.000 over 5 trades.

Detailed report: `RS_BREADTH_V2_RESCAN.md`.

## Retest V3 Rescan

Because direct breakout entries failed, I tested a retest/hold version: mark a strong RS/breadth breakout first, then enter only after a controlled pullback to the breakout reference and a reclaim/hold.

- Script: `Train_and_Test/doc5_long_setups/scan_doc5b_retest_v3.py`
- Pool: `Train_and_Test/doc5_long_setups/pool_retest_v3/`
- Rows: 74 after the detector/probe gate.
- Main structural features: `retest_depth_atr`, `pullback_from_breakout_high_atr`, `retest_close_reclaim_atr`, `breakout_age_bars`.

V3 optimizer result:

| window | trades | PF | net PnL | win% | trades/day |
|---|---:|---:|---:|---:|---:|
| TRAIN | 12 | 1.816 | Rs2,384 | 83.3 | 1.50 |
| TEST | 4 | 0.000 | Rs-5,857 | 0.0 | 2.00 |

V3 rescore audit:

- 700 trial rows / 444 unique configs rescored.
- `n >= 20` TRAIN-band configs: 0.
- Best `n >= 20`: TRAIN PF 1.141 over 20 trades, below band.
- Exploratory `n >= 12`: one train-band near-miss, TRAIN PF 1.303 over 12 trades, TEST PF 0.000 over 4 trades.

Detailed report: `RETEST_V3_RESCAN.md`.
