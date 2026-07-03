# E_VWAP_LOSE_EARLY_SHORT Experiment Log

## 2026-06-29 Six-Week Rerun

Pinned split:

| Period | Dates | Raw setup rows |
|---|---|---:|
| TRAIN | 2026-04-27..2026-06-05 | 265 |
| TEST | 2026-06-08..2026-06-12 | 60 |

Latest completed available TEST week was 2026-06-08..2026-06-12; weeks ending 2026-06-19 and 2026-06-26 were partial in the available pool.

Commands:

```powershell
python Train_and_Test\split_pool_by_setup.py --pool C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --out C:\TradingData\eqidv2\setup_pools_2026_06_29 --setups E_VWAP_LOSE_EARLY_SHORT
python -m py_compile Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_6wk_loop.py Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py Train_and_Test\setup_train_test.py
python Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_6wk_loop.py
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --train_start 2026-04-27 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 27 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --train_start 2026-04-27 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective band --min_train_trades 27 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
```

Loop decision rule: TEST was run only when TRAIN PF improved over baseline PF 0.362 and retained at least 27 TRAIN trades. Iteration changes were in-memory research variants only; no production config/code files were changed by any iteration. Machine-readable outputs:

- `E_VWAP_LOSE_EARLY_SHORT_6wk_loop_metrics.csv`
- `E_VWAP_LOSE_EARLY_SHORT_6wk_loop_details.json`

| Iter | Group | Variant | TRAIN n | TRAIN PF | TRAIN net | TEST n/PF/net | Decision |
|---:|---|---|---:|---:|---:|---|---|
| 0 | baseline | `current_conf_vol_band_min0945_0p70_1p00` | 54 | 0.362 | -20,407 | 10 / 0.643 / -1,698 | BASELINE |
| 1 | exit | `tight_scalp_0p50_0p60` | 54 | 0.212 | -20,689 | - | TRAIN reject |
| 2 | exit | `tight_scalp_0p60_0p80` | 54 | 0.267 | -22,023 | - | TRAIN reject |
| 3 | exit | `baseline_stop_smaller_target_0p70_0p80` | 54 | 0.323 | -20,386 | - | TRAIN reject |
| 4 | exit | `wider_stop_same_target_0p85_1p00` | 54 | 0.336 | -23,354 | - | TRAIN reject |
| 5 | exit | `same_stop_runner_0p70_1p20` | 54 | 0.361 | -21,423 | - | TRAIN reject |
| 6 | exit | `wider_runner_0p90_1p25` | 54 | 0.359 | -23,732 | - | TRAIN reject |
| 7 | time | `min_0950` | 51 | 0.359 | -19,326 | - | TRAIN reject |
| 8 | time | `min_1000` | 37 | 0.360 | -13,860 | - | TRAIN reject |
| 9 | time | `max_1130` | 54 | 0.362 | -20,407 | - | TRAIN reject |
| 10 | time | `max_1230` | 54 | 0.362 | -20,407 | - | TRAIN reject |
| 11 | time | `window_0950_to_1130` | 51 | 0.359 | -19,326 | - | TRAIN reject |
| 12 | volume | `drop_volume_band_raw_guarded` | 192 | 0.522 | -47,682 | 38 / 0.293 / -15,892 | TEST collapse |
| 13 | volume | `vol_band_1p8_2p8` | 42 | 0.336 | -16,910 | - | TRAIN reject |
| 14 | volume | `vol_band_2p0_3p2` | 40 | 0.363 | -14,918 | 6 / 0.528 / -1,365 | TEST collapse |
| 15 | volume | `vol_band_2p0_2p8` | 28 | 0.325 | -11,422 | - | TRAIN reject |
| 16 | volume | `vol_band_1p5_3p2` | 96 | 0.464 | -27,450 | 14 / 0.361 / -5,406 | TEST collapse |
| 17 | volume | `vol_band_1p8_4p0` | 69 | 0.376 | -25,602 | 11 / 0.804 / -932 | Reject: TRAIN failed |
| 18 | trend | `market_aligned_le_0` | 38 | 0.436 | -11,801 | 5 / 0.264 / -2,130 | TEST collapse |
| 19 | trend | `market_down_le_neg_0p05` | 32 | 0.377 | -11,317 | 3 / 0.411 / -1,092 | TEST collapse |
| 20 | trend | `market_abs_le_0p56` | 44 | 0.396 | -15,316 | 10 / 0.643 / -1,698 | TEST collapse |
| 21 | trend | `rs_lagger_le_neg_0p25` | 38 | 0.463 | -10,739 | 7 / 0.616 / -1,427 | TEST collapse |
| 22 | trend | `rs_lagger_le_neg_0p50` | 26 | 0.382 | -8,830 | - | TRAIN reject |
| 23 | vwap | `overlay_vwap_only_ge_neg1p25` | 176 | 0.532 | -42,871 | 37 / 0.306 / -14,964 | TEST collapse |
| 24 | vwap | `vol_band_plus_vwap_ge_neg1p25` | 49 | 0.404 | -16,854 | 10 / 0.643 / -1,698 | TEST collapse |
| 25 | vwap | `vol_band_plus_vwap_ge_neg1p00` | 41 | 0.421 | -13,588 | 10 / 0.643 / -1,698 | TEST collapse |
| 26 | vwap | `vol_band_plus_vwap_le_neg0p20` | 50 | 0.370 | -18,389 | 7 / 0.399 / -2,301 | TEST collapse |
| 27 | vwap | `vol_band_vwap_band_neg1p25_to_neg0p20` | 45 | 0.418 | -14,836 | 7 / 0.399 / -2,301 | TEST collapse |
| 28 | candle | `close_loc_le_0p25` | 43 | 0.285 | -19,448 | - | TRAIN reject |
| 29 | candle | `close_loc_0p08_to_0p25` | 43 | 0.285 | -19,448 | - | TRAIN reject |
| 30 | candle | `body_ge_0p65` | 42 | 0.294 | -18,571 | - | TRAIN reject |
| 31 | candle | `body_ge_0p75` | 25 | 0.302 | -10,971 | - | TRAIN reject |
| 32 | volatility | `atr_le_0p0060` | 39 | 0.169 | -23,151 | - | TRAIN reject |
| 33 | volatility | `atr_le_0p0045` | 21 | 0.166 | -12,315 | - | TRAIN reject |
| 34 | volatility | `atr_band_0p0020_0p0060` | 38 | 0.175 | -22,221 | - | TRAIN reject |
| 35 | quality | `quality_ge_60` | 54 | 0.362 | -20,407 | - | TRAIN reject |
| 36 | quality | `quality_ge_80` | 50 | 0.333 | -20,082 | - | TRAIN reject |
| 37 | quality | `quality_ge_100` | 1 | 0.000 | -129 | - | TRAIN reject |
| 38 | confirmation | `sig5_adx_ge_20` | 43 | 0.424 | -13,609 | 8 / 1.054 / 157 | Reject: TRAIN failed, TEST < 1.3 and thin |
| 39 | confirmation | `sig5_adx_ge_25` | 33 | 0.525 | -8,376 | 7 / 0.790 / -610 | TEST collapse |
| 40 | confirmation | `sig5_vol_ratio20_ge_1p56` | 38 | 0.347 | -14,676 | - | TRAIN reject |
| 41 | confirmation | `old_live_premom_gate` | 37 | 0.362 | -13,745 | - | TRAIN reject |

### Official Tuner Cross-Check, Six-Week Rerun

Max-PF objective found a TRAIN-only candidate:

- Exit: SL 0.85 / Target 0.80
- Mask: `close_loc >= 0.2709` and `quality_score >= 80.4218`
- Pre-momentum: none
- TRAIN: 25 trades, PF 1.876, net +Rs4,856, win 76%
- TEST: 11 trades, PF 0.129, net -Rs7,564, win 18.2%, 72.7% SL
- Verdict: reject; TEST PF 0.129 < 1.3, day-block p 1.0, OOS/IS PF ratio 0.07

Band objective result: `DROP_NO_EDGE`; no selected trades.

### Six-Week Keep/Reject Notes

- No hand-loop variant with enough trades reached TRAIN PF 1.0, let alone the 1.2 acceptance floor.
- The best TEST-looking hand variant, `sig5_adx_ge_20`, had TEST PF 1.054 on only 8 trades, but TRAIN PF was only 0.424.
- Removing the documented volume band increased sample size but worsened TEST to 38 trades / PF 0.293 / net -Rs15,892.
- Reusing the v11 overlay-style VWAP rule worsened TEST to 37 trades / PF 0.306 / net -Rs14,964.
- Official optimization can manufacture TRAIN PF, but the selected candidate collapsed on TEST to PF 0.129.

---

## Prior Thin 2wk/1wk Audit

Loop command:

```powershell
python Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py
```

Official tuner cross-check:

```powershell
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 10 --max_mask_terms 2 --max_premom_terms 1 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
```

Loop decision rule: TEST was run only when TRAIN PF improved over baseline PF 0.353 and retained at least 10 TRAIN trades. Iteration changes were in-memory research variants only; no production config/code files were changed by any iteration. Machine-readable outputs:

- `E_VWAP_LOSE_EARLY_SHORT_loop_metrics.csv`
- `E_VWAP_LOSE_EARLY_SHORT_loop_details.json`

| Iter | Group | Variant | TRAIN n | TRAIN PF | TRAIN net | TEST n | TEST PF | TEST net | Decision |
|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 0 | baseline | `current_conf_vol_band_min0945_0p70_1p00` | 19 | 0.353 | -7,273 | 10 | 0.643 | -1,698 | BASELINE |
| 1 | exit | `tight_scalp_0p50_0p60` | 19 | 0.288 | -5,782 | - | - | - | TRAIN reject |
| 2 | exit | `tight_scalp_0p60_0p80` | 19 | 0.384 | -5,678 | 10 | 0.171 | -5,501 | REJECT_TEST_COLLAPSE |
| 3 | exit | `baseline_stop_smaller_target_0p70_0p80` | 19 | 0.343 | -6,769 | - | - | - | TRAIN reject |
| 4 | exit | `wider_stop_same_target_0p85_1p00` | 19 | 0.304 | -9,059 | - | - | - | TRAIN reject |
| 5 | exit | `same_stop_runner_0p70_1p20` | 19 | 0.328 | -8,174 | - | - | - | TRAIN reject |
| 6 | exit | `wider_runner_0p90_1p25` | 19 | 0.240 | -11,204 | - | - | - | TRAIN reject |
| 7 | time | `min_0950` | 18 | 0.384 | -6,346 | 10 | 0.643 | -1,698 | REJECT_TEST_COLLAPSE |
| 8 | time | `min_1000` | 11 | 0.859 | -652 | 7 | 0.400 | -2,295 | REJECT_TEST_COLLAPSE |
| 9 | time | `max_1130` | 19 | 0.353 | -7,273 | - | - | - | TRAIN reject |
| 10 | time | `max_1230` | 19 | 0.353 | -7,273 | - | - | - | TRAIN reject |
| 11 | time | `window_0950_to_1130` | 18 | 0.384 | -6,346 | 10 | 0.643 | -1,698 | REJECT_TEST_COLLAPSE |
| 12 | volume | `drop_volume_band_raw_guarded` | 77 | 0.628 | -13,385 | 38 | 0.293 | -15,892 | REJECT_TEST_COLLAPSE |
| 13 | volume | `vol_band_1p8_2p8` | 13 | 0.181 | -7,568 | - | - | - | TRAIN reject |
| 14 | volume | `vol_band_2p0_3p2` | 13 | 0.483 | -3,422 | 6 | 0.528 | -1,365 | REJECT_TEST_COLLAPSE |
| 15 | volume | `vol_band_2p0_2p8` | 7 | 0.197 | -3,717 | - | - | - | TRAIN reject |
| 16 | volume | `vol_band_1p5_3p2` | 38 | 0.463 | -10,340 | 14 | 0.361 | -5,406 | REJECT_TEST_COLLAPSE |
| 17 | volume | `vol_band_1p8_4p0` | 25 | 0.454 | -7,531 | 11 | 0.804 | -932 | KEEP_CANDIDATE |
| 18 | trend | `market_aligned_le_0` | 11 | 0.347 | -4,305 | - | - | - | TRAIN reject |
| 19 | trend | `market_down_le_neg_0p05` | 9 | 0.269 | -4,148 | - | - | - | TRAIN reject |
| 20 | trend | `market_abs_le_0p56` | 16 | 0.340 | -6,200 | - | - | - | TRAIN reject |
| 21 | trend | `rs_lagger_le_neg_0p25` | 17 | 0.422 | -5,424 | 7 | 0.616 | -1,427 | REJECT_TEST_COLLAPSE |
| 22 | trend | `rs_lagger_le_neg_0p50` | 12 | 0.368 | -4,180 | 6 | 0.411 | -2,189 | REJECT_TEST_COLLAPSE |
| 23 | vwap | `overlay_vwap_only_ge_neg1p25` | 66 | 0.608 | -12,369 | 37 | 0.306 | -14,964 | REJECT_TEST_COLLAPSE |
| 24 | vwap | `vol_band_plus_vwap_ge_neg1p25` | 16 | 0.406 | -5,578 | 10 | 0.643 | -1,698 | REJECT_TEST_COLLAPSE |
| 25 | vwap | `vol_band_plus_vwap_ge_neg1p00` | 14 | 0.360 | -5,418 | 10 | 0.643 | -1,698 | REJECT_TEST_COLLAPSE |
| 26 | vwap | `vol_band_plus_vwap_le_neg0p20` | 17 | 0.422 | -5,420 | 7 | 0.399 | -2,301 | REJECT_TEST_COLLAPSE |
| 27 | vwap | `vol_band_vwap_band_neg1p25_to_neg0p20` | 14 | 0.506 | -3,725 | 7 | 0.399 | -2,301 | REJECT_TEST_COLLAPSE |
| 28 | candle | `close_loc_le_0p25` | 14 | 0.179 | -7,723 | - | - | - | TRAIN reject |
| 29 | candle | `close_loc_0p08_to_0p25` | 14 | 0.179 | -7,723 | - | - | - | TRAIN reject |
| 30 | candle | `body_ge_0p65` | 15 | 0.345 | -6,057 | - | - | - | TRAIN reject |
| 31 | candle | `body_ge_0p75` | 9 | 0.302 | -3,875 | - | - | - | TRAIN reject |
| 32 | volatility | `atr_le_0p0060` | 13 | 0.199 | -6,778 | - | - | - | TRAIN reject |
| 33 | volatility | `atr_le_0p0045` | 7 | 0.198 | -3,709 | - | - | - | TRAIN reject |
| 34 | volatility | `atr_band_0p0020_0p0060` | 13 | 0.199 | -6,778 | - | - | - | TRAIN reject |
| 35 | quality | `quality_ge_60` | 19 | 0.353 | -7,273 | - | - | - | TRAIN reject |
| 36 | quality | `quality_ge_80` | 18 | 0.384 | -6,351 | 9 | 0.799 | -767 | REJECT_TEST_COLLAPSE |
| 37 | quality | `quality_ge_100` | 1 | 0.000 | -129 | - | - | - | TRAIN reject |
| 38 | confirmation | `sig5_adx_ge_20` | 17 | 0.310 | -7,117 | - | - | - | TRAIN reject |
| 39 | confirmation | `sig5_adx_ge_25` | 14 | 0.424 | -4,348 | 7 | 0.790 | -610 | REJECT_TEST_COLLAPSE |
| 40 | confirmation | `sig5_vol_ratio20_ge_1p56` | 11 | 0.429 | -3,244 | 0 | 0.000 | 0 | REJECT_TEST_COLLAPSE |
| 41 | confirmation | `old_live_premom_gate` | 11 | 0.429 | -3,244 | 0 | 0.000 | 0 | REJECT_TEST_COLLAPSE |

## Official Tuner Cross-Check

The official tuner found a TRAIN-only candidate:

- Exit: SL 0.70 / Target 0.80
- Mask: `rs_pct <= -0.7997` and `rs_pct <= -0.8958` (effectively `rs_pct <= -0.8958`)
- TRAIN: 16 trades, PF 2.477, net +Rs3,790, min-half PF 1.81
- TEST: 10 trades, PF 0.068, net -Rs7,787, 90% SL, day-block p 1.0
- Verdict from tuner: `REJECT: test_pf 0.07<1.3; test_day_block_p 1.0>0.1; oos/is ratio 0.03<0.65`

## Keep/Reject Notes

- Exit-only changes did not fix TRAIN expectancy; the only tested exit improvement collapsed to TEST PF 0.171.
- Time guards improved TRAIN only by deleting trades; TEST stayed below PF 0.65 or worse.
- Removing the volume band increased sample size but made TEST much worse: 38 trades / PF 0.293 / net -Rs15,892.
- The overlay-style VWAP rule without the volume band also failed: TEST 37 trades / PF 0.306 / net -Rs14,964.
- The best hand-loop TEST PF was still losing: `vol_band_1p8_4p0`, TEST 11 trades / PF 0.804 / net -Rs932, with TRAIN PF only 0.454.
- Pre-entry confirmation gates either remained losing or killed TEST trade count.
- No variant met the acceptance floor of TRAIN PF meaningfully above 1.2 and TEST PF preferably >= 1.3.
