# E_VWAP_LOSE_EARLY_SHORT Final Summary

## 2026-06-29 Six-Week Rerun

Final status: **REJECTED / keep parked**.

Pinned split used:

| Period | Dates | Raw setup rows |
|---|---|---:|
| TRAIN | 2026-04-27..2026-06-05 | 265 |
| TEST | 2026-06-08..2026-06-12 | 60 |

The available pool runs through 2026-06-24, but the weeks ending 2026-06-19 and 2026-06-26 are partial. Therefore 2026-06-08..2026-06-12 is the latest completed available TEST week, and TRAIN is the 6 immediately preceding completed weeks.

### Result

| Config | TRAIN n/PF/net | TEST n/PF/net | Verdict |
|---|---:|---:|---|
| Baseline documented conf | 54 / 0.362 / -Rs20,407 | 10 / 0.643 / -Rs1,698 | Reject |
| Best hand-loop TEST-looking candidate: `sig5_adx_calc >= 20` | 43 / 0.424 / -Rs13,609 | 8 / 1.054 / +Rs157 | Reject: TRAIN failed, TEST below 1.3 and thin |
| Best count expansion: no volume band, min 09:45 | 192 / 0.522 / -Rs47,682 | 38 / 0.293 / -Rs15,892 | Reject |
| v11 overlay-style VWAP rule: `vwap_dist_atr >= -1.25` | 176 / 0.532 / -Rs42,871 | 37 / 0.306 / -Rs14,964 | Reject |
| Official max-PF TRAIN candidate | 25 / 1.876 / +Rs4,856 | 11 / 0.129 / -Rs7,564 | Reject overfit |
| Official band objective | no edge | no edge | Reject |

### Live / Backtest Mismatch

- `final_setup_conf.py` and `Train_and_Test/final_setup_conf.py` contain the E research config, but `_LIVE_SURVIVAL_DEMOTION_2026_06_29` pops it out of `FINAL_SETUP_CONF`.
- `eqidv2_v11_live_overlay.py` still contains an overlay path for E based on `vwap_dist_atr >= -1.25`, not the documented `vol_ratio 1.8..3.2` band.
- `eqidv2_entry_engine_1min_v5_id.py` still has an old default pre-momentum gate for E unless final-conf bootstrap overrides it.
- The source card had a stale active/strongest title; it was corrected in this audit.

### Files / Commands

Files added or updated for this rerun:

- `Train_and_Test/setup_looping_results/run_E_VWAP_LOSE_EARLY_SHORT_6wk_loop.py`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_6wk_loop_metrics.csv`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_6wk_loop_details.json`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_baseline.md`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_experiment_log.md`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_best_config.md`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_final_summary.md`
- `Train_and_Test/setup_looping_results/MASTER_SETUP_OPTIMIZATION_SUMMARY.md`
- `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md`

Commands run:

```powershell
python Train_and_Test\split_pool_by_setup.py --pool C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --out C:\TradingData\eqidv2\setup_pools_2026_06_29 --setups E_VWAP_LOSE_EARLY_SHORT
python -m py_compile Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_6wk_loop.py Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py Train_and_Test\setup_train_test.py
python Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_6wk_loop.py
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --train_start 2026-04-27 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 27 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --train_start 2026-04-27 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective band --min_train_trades 27 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
```

No live trades were placed. No production config was changed.

### Next

Next source-doc setup to process remains `D_EMA20_REJECTION`.

---

## Prior Thin 2wk/1wk Summary

Final status: **REJECTED / keep parked**.

Pinned split used:

| Period | Dates |
|---|---|
| TRAIN | 2026-05-25..2026-06-05 |
| TEST | 2026-06-08..2026-06-12 |

## Result

| Config | TRAIN n/PF/net | TEST n/PF/net | Verdict |
|---|---:|---:|---|
| Baseline documented conf | 19 / 0.353 / -Rs7,273 | 10 / 0.643 / -Rs1,698 | Reject |
| Best hand-loop TEST candidate | 25 / 0.454 / -Rs7,531 | 11 / 0.804 / -Rs932 | Reject |
| Official tuner best TRAIN candidate | 16 / 2.477 / +Rs3,790 | 10 / 0.068 / -Rs7,787 | Reject overfit |

## Live / Backtest Mismatch

- `final_setup_conf.py` and `Train_and_Test/final_setup_conf.py` contain the E research config, but `_LIVE_SURVIVAL_DEMOTION_2026_06_29` pops it out of `FINAL_SETUP_CONF`.
- `avwap_5min_ID_v11_backtesting.py` and `eqidv2_v11_live_overlay.py` still contain an overlay path for E based on `vwap_dist_atr >= -1.25`, not the documented `vol_ratio 1.8..3.2` band.
- `eqidv2_entry_engine_1min_v5_id.py` still has an old default pre-momentum gate for E unless final-conf bootstrap overrides it.
- At the time of the prior thin audit, the source card still labeled E active/strongest; the six-week rerun corrected that stale status.

## Files / Commands

Files added for this audit:

- `Train_and_Test/setup_looping_results/run_E_VWAP_LOSE_EARLY_SHORT_loop.py`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_loop_metrics.csv`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_loop_details.json`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_baseline.md`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_experiment_log.md`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_best_config.md`
- `Train_and_Test/setup_looping_results/E_VWAP_LOSE_EARLY_SHORT_final_summary.md`

Commands run:

```powershell
python -m py_compile Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py Train_and_Test\setup_train_test.py
python Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 10 --max_mask_terms 2 --max_premom_terms 1 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
```

No live trades were placed. No production config was changed.

## Next

Next source-doc setup to process: `D_EMA20_REJECTION`.
