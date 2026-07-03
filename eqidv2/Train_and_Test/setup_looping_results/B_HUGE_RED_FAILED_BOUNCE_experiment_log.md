# B_HUGE_RED_FAILED_BOUNCE - Experiment Log

## Rolling 2-Week/1-Week Audit Update - 2026-06-29

Primary rolling loop:

```powershell
python Train_and_Test\setup_looping_results\run_B_HUGE_RED_FAILED_BOUNCE_loop.py
```

Official tuner cross-checks:

```powershell
python Train_and_Test\setup_train_test.py --family B --setups B_HUGE_RED_FAILED_BOUNCE --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 6 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 3 --no_fdr
python Train_and_Test\setup_train_test.py --family B --setups B_HUGE_RED_FAILED_BOUNCE --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective band --min_train_trades 6 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 3 --no_fdr
```

Machine-readable rolling outputs:

- `B_HUGE_RED_FAILED_BOUNCE_rolling_loop_metrics.csv`
- `B_HUGE_RED_FAILED_BOUNCE_rolling_loop_details.json`
- `B_HUGE_RED_FAILED_BOUNCE_rolling_slippage_check.json`

Loop rule: TEST was run only when TRAIN PF improved over baseline PF 0.647 and retained at least 6 TRAIN trades. Final acceptance still required robust TRAIN PF, TEST preferably >= 1.3, reasonable non-lucky sample, and no live-paper contradiction.

| Iter | Group | Variant | TRAIN n | TRAIN PF | TRAIN net | TEST n | TEST PF | TEST net | Decision |
|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 0 | baseline | `current_conf_premom_0p90_1p25` | 10 | 0.647 | -1,246 | 5 | 0.873 | -295 | BASELINE |
| 1 | exit | `same_gate_0p90_1p00` | 10 | 0.864 | -440 | 5 | 0.659 | -794 | REJECT_TEST_COLLAPSE |
| 2 | exit | `same_gate_0p90_1p50` | 10 | 0.718 | -997 | 5 | 0.699 | -699 | REJECT_TEST_COLLAPSE |
| 3 | exit | `same_gate_0p70_1p00` | 7 | 1.241 | 395 | 2 | inf | 1,531 | Reject tiny TEST n=2 |
| 4 | exit | `same_gate_0p70_1p25` | 7 | 1.394 | 645 | 2 | inf | 2,030 | Reject tiny TEST n=2 |
| 5 | exit | `same_gate_1p10_1p25` | 12 | 0.651 | -1,766 | 5 | 0.852 | -352 | Reject; TRAIN failed |
| 6 | exit | `same_gate_1p20_1p50` | 12 | 0.721 | -1,468 | 5 | 0.656 | -851 | REJECT_TEST_COLLAPSE |
| 7 | premom_dropout | `raw_no_premom_gate` | 219 | 0.461 | -65,455 | - | - | - | TRAIN reject |
| 8 | premom_dropout | `drop_pre3_close_pos` | 26 | 0.560 | -4,058 | - | - | - | TRAIN reject |
| 9 | premom_dropout | `drop_sig5_rsi_dir` | 85 | 0.427 | -25,403 | - | - | - | TRAIN reject |
| 10 | premom_dropout | `drop_pre5_mom_r` | 15 | 0.787 | -1,080 | 7 | 1.125 | 338 | Reject; TRAIN failed |
| 11 | premom_threshold | `pre3_close_pos_le_0p45` | 8 | 0.977 | -54 | 3 | 1.793 | 898 | Reject; TRAIN failed + TEST n=3 |
| 12 | premom_threshold | `pre3_close_pos_le_0p70` | 12 | 0.790 | -741 | 6 | 0.677 | -970 | REJECT_TEST_COLLAPSE |
| 13 | premom_threshold | `sig5_rsi_dir_le_55` | 3 | 4.533 | 1,300 | - | - | - | TRAIN too few |
| 14 | premom_threshold | `sig5_rsi_dir_le_60` | 6 | 1.055 | 108 | 3 | 18.301 | 1,919 | Reject; TRAIN PF failed + TEST n=3 |
| 15 | premom_threshold | `pre5_mom_r_le_0p10` | 4 | 2.992 | 1,086 | - | - | - | TRAIN too few |
| 16 | premom_threshold | `pre5_mom_r_le_0p00` | 4 | 2.992 | 1,086 | - | - | - | TRAIN too few |
| 17 | confirmation | `add_pre1_adx_ge_30` | 6 | 1.439 | 572 | 2 | 0.000 | -2,214 | REJECT_TEST_COLLAPSE |
| 18 | confirmation | `add_pre1_adx_ge_39` | 1 | 0.000 | -291 | - | - | - | TRAIN too few |
| 19 | confirmation | `add_sig5_adx_ge_25` | 2 | inf | 1,223 | - | - | - | TRAIN too few |
| 20 | confirmation | `add_sig5_adx_ge_35` | 0 | 0.000 | 0 | - | - | - | TRAIN too few |
| 21 | quality | `quality_le_37p6` | 1 | inf | 208 | - | - | - | TRAIN too few |
| 22 | quality | `quality_le_60` | 8 | 0.783 | -521 | 4 | 0.917 | -184 | Reject; TRAIN failed |
| 23 | quality | `quality_ge_60` | 2 | 0.360 | -725 | - | - | - | TRAIN too few |
| 24 | volume | `vol_ratio_ge_2p0` | 7 | 1.896 | 1,078 | 2 | 0.000 | -1,193 | REJECT_TEST_COLLAPSE |
| 25 | volume | `vol_ratio_ge_3p0` | 2 | inf | 860 | - | - | - | TRAIN too few |
| 26 | candle | `upper_wick_le_0p05` | 5 | 1.898 | 982 | - | - | - | TRAIN too few |
| 27 | candle | `body_ge_0p70` | 4 | 2.248 | 905 | - | - | - | TRAIN too few |
| 28 | time | `max_1130` | 1 | 0.000 | -368 | - | - | - | TRAIN too few |
| 29 | time | `max_1230` | 2 | 0.000 | -1,093 | - | - | - | TRAIN too few |
| 30 | trend | `market_aligned_le_0` | 9 | 0.588 | -1,454 | - | - | - | TRAIN reject |
| 31 | trend | `market_down_le_neg_0p05` | 7 | 0.288 | -2,513 | - | - | - | TRAIN reject |
| 32 | trend | `market_abs_le_0p56` | 5 | 0.755 | -410 | - | - | - | TRAIN too few |
| 33 | rs | `rs_lagger_le_0` | 5 | 0.343 | -1,177 | - | - | - | TRAIN too few |
| 34 | rs | `rs_lagger_le_neg_0p50` | 2 | 0.360 | -725 | - | - | - | TRAIN too few |
| 35 | overfit_check | `quality_37p6_upperwick_0_pre1adx39_1p20_1p50` | 0 | 0.000 | 0 | - | - | - | TRAIN reject |

Official tuner cross-check:

- `maxpf`: TRAIN 15 / PF 3.494 using `rs_pct>=0.985`, `vwap_dist_atr>=-4.67`, `pre5_mom_r<=0.6848`, `pre3_range_r<=0.6494`, exit 0.85/1.20. TEST collapsed to 3 / PF 0.447 / -Rs1,190.
- `band`: TRAIN 27 / PF 1.440 using `rs_pct>=0.985`, no premom, exit 1.00/0.80. TEST collapsed to 9 / PF 0.140 / -Rs6,919.

Slippage sensitivity for the current conf on this rolling split:

| Slippage | TRAIN n/PF/net | TEST n/PF/net | Read |
|---|---:|---:|---|
| 15 bps/leg | 10 / 0.647 / -Rs1,246 | 5 / 0.873 / -Rs295 | loser, too thin |
| 5 bps/leg | 10 / 1.747 / +Rs1,749 | 5 / 1.294 / +Rs525 | just below 1.3; too thin; not robust |

Rolling verdict: **reject / no config change**. Some TRAIN-passing variants are tiny and lucky; all meaningful-sample variants fail TRAIN or collapse on TEST.

---

## Prior Active-Book Experiment Log

Window: TRAIN 2026-04-13..2026-05-25, TEST 2026-05-26..2026-06-24 (unless noted).
Engine: `setup_loop_runner.py` / `setup_train_test.py` on the per-setup unified-pool slice.
Net of v6 cost + 15 bps/leg slippage; live dedupe + pipeline. Acceptance bar: TRAIN PF>1.2 (pref ≥1.4),
TEST PF≥1.3, adequate non-concentrated sample, no TRAIN→TEST collapse.

## Automated searches (whole-window, anti-overfit)
- **band objective** (`setup_train_test --objective band --min_train_trades 20`): `DROP_NO_EDGE` — no config
  reaches the PF≥1.4 band with ≥20 trades and both-half positivity on TRAIN.
- **maxpf objective** (`--objective maxpf --force_premom --max_mask_terms 2 --max_premom_terms 2`): best TRAIN
  config = mask `quality_score≤37.6 & upper_wick_pct≤0.0`, premom `pre3_close_pos≤0.939 & pre1_adx≥39.5`,
  exit 1.2/1.5 → TRAIN PF 2.31 (n=20) but **TEST bootstrap p = 0.516 → BH-FDR-dropped** (no out-of-sample edge).
  `upper_wick_pct≤0.0` is a degenerate overfit term. Verdict: overfit, not real.

## Hand iterations (one change-group each; loss-mode driven)

| # | Change group | Config | TRAIN n/PF/net | TEST n/PF/net | Keep? | Reason |
|---|---|---|---:|---:|---|---|
| 1 | baseline | conf gate, 0.9/1.25 | 28 / 0.79 / -2,098 | 16 / 0.52 / -3,422 | — | baseline; loser both periods |
| 2 | exit | gate, 0.9/1.0 | 28 / 0.75 / -2,509 | 16 / 0.54 / -3,115 | reject | tighter tgt no help |
| 3 | exit | gate, 0.9/1.5 | 28 / 0.81 / -1,908 | 16 / 0.49 / -3,576 | reject | wider tgt → more EOD |
| 4 | exit | gate, 0.7/1.0 | 26 / 0.45 / -6,695 | 9 / 1.14 / +345 | reject | TRAIN collapses; TEST is 1 lucky day (top1day 222%) |
| 5 | exit | gate, 1.2/2.0 | 31 / 0.54 / -6,770 | 18 / 0.16 / -7,323 | reject | 89% EOD on test; worst |
| 6 | time guard | gate, max_slot 11:30 | 2 / inf / +2,033 | 1 / 0.00 / -368 | reject | n=2 train — noise |
| 7 | time guard | gate, max_slot 12:30 | 7 / 1.49 / +1,018 | 3 / 0.00 / -2,225 | reject | TEST collapse (0/3) |
| 8 | quality mask | gate + quality_score≤37.6 | 13 / 1.52 / +1,436 | 4 / 1.98 / +1,106 | **reject (fragile)** | only both-positive config, but TEST n=4 with one day=92% of net; TRAIN one day >100% of net; dbp 0.33/0.28 insignificant |
| 9 | volume mask | gate + vol_ratio≥2.0 | 16 / 0.70 / -1,681 | 10 / 0.45 / -1,996 | reject | loser both |
| 10 | momentum gate | gate, pre5_mom_r≤0.10 | 20 / 0.73 / -2,070 | 5 / 4.86 / +2,103 | reject | TRAIN still loser; TEST 5 trades |
| 11 | + ADX gate | gate + pre1_adx≥39 | 8 / 0.73 / -645 | 1 / 0.00 / -291 | reject | over-tightens to n=8/1 |
| 12 | maxpf overfit | quality≤37.6 & upperwick≤0 + premom, 1.2/1.5 | 20 / 2.31 / +4,294 | 6 / 0.97 / -79 | reject | TEST coin-flip (overfit) |

## Sample-sensitivity / regime checks

To separate "thin-sample noise" from a genuine edge decay, the conf gate was re-run on a **longer fresh
train** and on the **original conf window**:

| Window | TRAIN n/PF | TEST n/PF | Read |
|---|---:|---:|---|
| Feb–May train (2026-02-01..2026-05-25) → fresh test | 48 / **0.78** | 16 / 0.52 | bigger sample, still a loser → decay is **real**, not thin-sample. Monthly: Feb −3.9k, Mar +1.0k, Apr −3.6k, May +2.2k |
| Original conf window (train 2025-11-01..2026-04-30 / test 2026-05-01..2026-06-10) | (see final_summary) | (see final_summary) | confirmatory: reproduces the historical edge, which has since decayed |

**Interpretation:** the pre-momentum gate created a genuine edge on Nov-2025..early-Jun-2026 data, but across
all of 2026-02 onward (Feb, Apr negative; only Mar/May mild positive) the edge is gone **at 15 bps/leg**.
(See the slippage section below — this conclusion flips at lower slippage.)

## Slippage sensitivity (PIVOTAL) + basis discrepancy

B_HUGE_RED is a tight 1.25%-target scalp, so per-leg slippage dominates. Re-running the conf gate at the
live-paper slippage (≈5 bps/leg) vs the tuner's realistic-fill stress (15 bps/leg):

| Window | @15 bps/leg TRAIN / TEST | @5 bps/leg TRAIN / TEST |
|---|---|---|
| FRESH (04-13..05-25 / 05-26..06-24) | 0.79 (n28) / 0.52 (n16) | **1.63 (n28, +4,317, dbp 0.14) / 1.10 (n16, +495)** |
| ORIGINAL conf (11-01..04-30 / 05-01..06-10) | 0.55 (n73) / 1.17 (n21) | **1.28 (n75) / 2.39 (n21, dbp 0.042)** |
| FRESH + quality_score≤37.6 mask | 1.52 (n13) / 1.98 (n4) | 2.95 (n13) / 3.30 (n4) |

Two independent findings:
1. **Slippage is the swing variable.** 30 bps round-trip (15/leg) turns the setup into a loser; ~10 bps
   round-trip (5/leg) makes it workable. Break-even is ≈8–10 bps/leg. Small-cap shorts rarely fill that tight,
   and the §6 live paper PF was 0.25 — so realistic fills are closer to the loser end.
2. **Basis discrepancy (backtest-vs-live mismatch).** Even at matched 5 bps, the unified-pool *readmit-raw*
   basis gives TRAIN n=75 / PF 1.28 — NOT the conf's published TRAIN n=30 / PF 2.90. The conf's strong evidence
   came from a narrower clean-pool mine that the current "readmit = faithful" pool does **not reproduce**
   (~2.5× more candidates, lower PF). The published 2.90/3.49 is not reproducible on the live-faithful pool.
3. **Recent degradation.** At 5 bps the ORIGINAL test (to 06-10) is 2.39 but the FRESH test (to 06-24, i.e.
   adding mid–late June) drops to 1.10 — the decay is concentrated in mid–late June (matches §6 live).
