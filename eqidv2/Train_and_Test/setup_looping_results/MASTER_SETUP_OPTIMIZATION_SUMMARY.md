# Master Setup Optimization Summary

Generated: 2026-06-29

Source doc: `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md`

Pool used: `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool`

Available pool coverage: 2025-06-02 through 2026-06-24. The weeks ending 2026-06-19 and 2026-06-26 are partial in this pool, so the latest completed available week is 2026-06-08..2026-06-12.

Pinned split for this loop:

| Period | Dates | A_PULLBACK rows | E_VWAP rows | B_HUGE_RED rows |
|---|---:|---:|---:|---:|
| TRAIN | 2026-05-25..2026-06-05 | 845 | 111 | 273 |
| TEST | 2026-06-08..2026-06-12 | 353 | 60 | 82 |

Additional requested six-week rerun split for `E_VWAP_LOSE_EARLY_SHORT`:

| Period | Dates | E_VWAP rows |
|---|---|---:|
| TRAIN | 2026-04-27..2026-06-05 | 265 |
| TEST | 2026-06-08..2026-06-12 | 60 |

## Setup Inventory From Source Doc

| Setup | Side | Doc/live state | Current filters | Gates | Guards | Exit | Notes/issues |
|---|---|---|---|---|---|---|---|
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | SHORT | User re-promoted 2026-06-29 watchlist | quality_score >= 123.7606 | sig5_adx_calc >= 21.4683 | none | 1.20/1.50 | Last-month available replay PF 3.491 on 30 trades; monitor live-paper before sizing |
| E_VWAP_LOSE_EARLY_SHORT | SHORT | Parked 2026-06-29 | vol_ratio 1.8..3.2 | none | min 09:45 | 0.70/1.00 | Live paper loser; parked after 2026-06-29 survival audit |
| D_EMA20_REJECTION | SHORT | Parked 2026-06-29 | none | pre10_mom_r <= 0.156614, pre5_mom_r >= 0.12493, sig5_adx_calc >= 20 | none | 0.75/1.30 | Overlay uses dropped body/ranker mask and no premom |
| B_HUGE_RED_FAILED_BOUNCE | SHORT | Survival active | none | pre3_close_pos <= 0.581797, sig5_rsi_dir <= 64.104659, pre5_mom_r <= 0.284145 | none | 0.90/1.25 | Corrected-VWAP mined short |
| C_OR_BREAKDOWN | SHORT | Survival active | none | sig5_adx_calc >= 39.670518, pre1_adx <= 21.368044 | none | 0.90/2.00 | Corrected-VWAP mined short |
| A_MOD_BREAK_C1_LOW | SHORT | Survival active | vol_ratio >= 1.955814 | pre5_mom_r >= 0.425861, pre3_range_r <= 0.202087 | none | 1.10/1.00 | Corrected-VWAP mined short |
| G_LOWER_LOW_BREAK | SHORT | Survival active | vol_ratio >= 4.129044, quality_score >= 76.444124 | sig5_rsi_dir >= 68.747209 | none | 0.80/0.80 | Corrected-VWAP mined short; selective; user exit override 2026-06-29 from 1.10/1.00 |
| B_AVWAP_RECLAIM_REVERSAL | LONG | Parked 2026-06-29 | vwap_dist_atr <= 1.0 | none | none | 0.70/1.50 | Overlay still uses inverted vwap_dist_atr >= 0.60 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | LONG | Parked 2026-06-29 | regime != BULL | none | none | 1.00/1.50 | Test was thin 5/5 winners; user SL override 2026-06-29 from 0.70 |
| G_HIGHER_HIGH_BREAK | LONG | Parked 2026-06-29 | none | pre2_mom_r >= 0.55, sig5_adx_calc >= 26 | none | 0.90/2.50 | Sample-thin; latest replay failed |
| L_DOUBLE_BOTTOM_VWAP | LONG | Parked 2026-06-29 | none | pre_entry_momentum_score >= 79, sig5_adx_calc >= 28 | none | 0.90/1.50 | RAW-pool caveat; live research layer blocks L family |
| L_PRESSURE_BURST_VWAP | LONG | Parked 2026-06-29 | quality_score <= 25 | pre1_adx >= 44 | none | 0.70/1.25 | Weak user override; failed robustness checks |
| P_PDH_BREAK_RETEST_LONG | LONG | Demoted 2026-06-22 | body_pct <= 0.749993 | pre_entry_momentum_score >= 75.07, pre3_range_r >= 0.499787 | none | 0.50/0.60 | Live paper PF 0.25; over-fires |
| L_RS_LEADER_VWAP_HOLD | LONG | Demoted 2026-06-22 | quality_score >= 97.121, vol_ratio >= 2.1643, vwap_dist_atr <= 1.4934, signal_minute <= 660 | none | none | 0.50/1.25 | Live paper PF 0.15 |
| V_RECLAIM_PULLBACK_LONG | LONG | Demoted 2026-06-22 | rs_pct >= 0.372426 | pre_entry_momentum_score <= 58.013, sig5_adx_calc >= 33.933 | none | 0.50/0.80 | Live paper 0/3 |
| E_ORB_RETEST_HOLD_LONG | LONG | Demoted 2026-06-22 | vol_ratio >= 2.4238, quality_score >= 86.575, signal_minute >= 605 | sig5_adx_calc >= 42.416 | none | 0.90/1.25 | Live paper PF 0.01 |

Rejected research-watch setups listed in the source doc: D_AVWAP_LOSE_REVERSAL, E_ORB_RETEST_HOLD_SHORT, T_TREND_DAY_EMA_STAIR_SHORT, S_UPTHRUST_TRAP_FADE, E_ORB_BREAKOUT_SHORT, E_ORB_BREAKOUT_LONG, E_VWAP_BAND_FADE, L_BB_SQUEEZE_LONG, L_TREND_PULLBACK, S_BB_SQUEEZE_SHORT, S_MACD_HIST_FLIP, T_TREND_DAY_EMA_STAIR_LONG, MR_CONTROLLED_VWAP_EXTREME_FADE_LONG, MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT.

## Tested Setups

| Setup | Baseline TRAIN n/PF | Baseline TEST n/PF | Best research candidate TRAIN n/PF | Best research candidate TEST n/PF | Status | Files changed |
|---|---:|---:|---:|---:|---|---|
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 236 / 0.771 | 101 / 0.402 | 37 / 2.210 (`quality_score >= 123.7606` + `sig5_adx_calc >= 21.4683`) | 8 / 3.195 | USER_REPROMOTED_WATCHLIST | `final_setup_conf.py`, `Train_and_Test/final_setup_conf.py` |
| E_VWAP_LOSE_EARLY_SHORT | 19 / 0.353 | 10 / 0.643 | 16 / 2.477 (official tuner `rs_pct<=-0.8958`, SL 0.70/Tgt 0.80) | 10 / 0.068 | REJECTED / keep parked | No production config change; added E loop/docs under `Train_and_Test/setup_looping_results/` |
| E_VWAP_LOSE_EARLY_SHORT *(6wk rerun)* | 54 / 0.362 | 10 / 0.643 | 25 / 1.876 (official maxpf `close_loc>=0.2709` + `quality_score>=80.4218`, SL 0.85/Tgt 0.80) | 11 / 0.129 | REJECTED / keep parked | No production config change; added 6wk runner/metrics/docs; source card status corrected |
| B_HUGE_RED_FAILED_BOUNCE *(rolling 2wk/1wk rerun)* | 10 / 0.647 | 5 / 0.873 | 15 / 3.494 (official maxpf overfit) | 3 / 0.447 | REJECTED / no config change | No production config change; added B rolling runner/metrics/slippage check |
| B_HUGE_RED_FAILED_BOUNCE *(active; split-faithful track, see below)* | 28 / 0.79 @15bps · 1.63 @5bps | 16 / 0.52 @15bps · 1.10 @5bps | quality≤37.6: 13 / 2.95 @5bps | 4 / 3.30 @5bps (n=4 too thin) | REJECT for sizing / keep unsized | No config change; slippage-fragile + basis-discrepancy + recent decay |
| C_OR_BREAKDOWN *(active; split-faithful track, see below)* | 83 / 0.64 @15bps · 0.94 @5bps | 53 / 0.62 @15bps · 1.12 @5bps | rs_pct≤-4.92: 121 / 0.64 @15bps · 1.00 @5bps | 76 / 0.93 @15bps · 1.33 @5bps | REJECT for sizing / keep unsized | No config change; loser at realistic cost; rs-weakness lead (WATCH) |
| A_MOD_BREAK_C1_LOW *(active; split-faithful track, see below)* | 72 / 0.62 @15bps · 1.23 @5bps | 23 / 0.57 @15bps · 1.06 @5bps | vol-band: 48 / 0.76 @15bps · 1.48 @5bps | 18 / 0.53 @15bps · 0.96 @5bps | REJECT for sizing / keep unsized | No config change; best-of-4 at paper cost but loser @15bps; TEST never ≥1.3 |
| G_LOWER_LOW_BREAK *(active; split-faithful track, see below)* | 6 / 3.42 @15bps · 10.27 @5bps (n=6 noise) | 6 / 0.75 @15bps · 1.27 @5bps | i03 vol≥3&q≥50: 31 / 1.66 @15bps · 3.07 @5bps | 25 / 0.93 @15bps · 1.40 @5bps | REJECT for sizing / **strongest WATCH lead** | No config change; conf mask too selective (n=6); loosened i03 passes bar @5bps, fails @15bps; climax-bar fills favourable |
| L_DOUBLE_BOTTOM_VWAP *(parked LONG; split-faithful track)* | 38 / 0.88 @15bps · 1.30 @5bps | 29 / 0.29 @15bps · 0.48 @5bps | best TEST exit 1.1/1.5: 29 / 0.90 @15bps · 1.28 @5bps | 19 / 0.74 @15bps · 0.92 @5bps | REJECT / keep parked | No config change; universal TEST collapse (60–79% SL); regime failure for long reversals |
| L_PRESSURE_BURST_VWAP *(parked LONG; split-faithful track)* | 138 / 0.51 @15bps · 0.84 @5bps | 67 / 0.39 @15bps · 0.79 @5bps | best @5bps exit 0.9/1.5: 138 / 0.54 @15bps · 0.95 @5bps | 67 / 0.46 @15bps · 0.79 @5bps | REJECT / keep parked (no lead) | No config change; structural loser at every config/slippage on large n; gate≈ungated |

Top setups for paper/live-watch from this loop: A_PULLBACK_C2_THEN_BREAK_C2_LOW with `quality_score >= 123.7606` and `sig5_adx_calc >= 21.4683`. E_VWAP_LOSE_EARLY_SHORT is rejected and should remain parked after both the thin audit and the requested six-week rerun. B_HUGE_RED_FAILED_BOUNCE failed the rolling audit for sizing; at 5 bps it is only watchlist-thin (TEST 5/PF 1.294), while at 15 bps it is a loser. Survival/watchlist book now includes A_PULLBACK_C2_THEN_BREAK_C2_LOW plus B_HUGE_RED_FAILED_BOUNCE, C_OR_BREAKDOWN, A_MOD_BREAK_C1_LOW, G_LOWER_LOW_BREAK, but B should remain unsized unless user explicitly keeps it active.

## Commands Used

```powershell
python Train_and_Test\setup_train_test.py --family A --setups A_PULLBACK_C2_THEN_BREAK_C2_LOW --pool_dir C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 40 --max_mask_terms 2 --max_premom_terms 1 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 10 --no_fdr
python Train_and_Test\setup_looping_results\run_A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop.py
python -m py_compile Train_and_Test\setup_looping_results\run_A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop.py Train_and_Test\setup_train_test.py Train_and_Test\train_test_conf.py final_setup_conf.py eqidv2_final_conf_live_bootstrap.py eqidv2_v11_live_overlay.py
python -m py_compile Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py Train_and_Test\setup_train_test.py
python Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 10 --max_mask_terms 2 --max_premom_terms 1 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
python Train_and_Test\split_pool_by_setup.py --pool C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --out C:\TradingData\eqidv2\setup_pools_2026_06_29 --setups E_VWAP_LOSE_EARLY_SHORT
python -m py_compile Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_6wk_loop.py Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_loop.py Train_and_Test\setup_train_test.py
python Train_and_Test\setup_looping_results\run_E_VWAP_LOSE_EARLY_SHORT_6wk_loop.py
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --train_start 2026-04-27 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 27 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
python Train_and_Test\setup_train_test.py --family E --setups E_VWAP_LOSE_EARLY_SHORT --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --train_start 2026-04-27 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective band --min_train_trades 27 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 5 --no_fdr
python Train_and_Test\split_pool_by_setup.py --pool C:\TradingData\eqidv2\outputs_ID_v11_unified_pool --out C:\TradingData\eqidv2\setup_pools_2026_06_29 --setups B_HUGE_RED_FAILED_BOUNCE
python -m py_compile Train_and_Test\setup_looping_results\run_B_HUGE_RED_FAILED_BOUNCE_loop.py Train_and_Test\setup_train_test.py
python Train_and_Test\setup_looping_results\run_B_HUGE_RED_FAILED_BOUNCE_loop.py
python Train_and_Test\setup_train_test.py --family B --setups B_HUGE_RED_FAILED_BOUNCE --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective maxpf --min_train_trades 6 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 3 --no_fdr
python Train_and_Test\setup_train_test.py --family B --setups B_HUGE_RED_FAILED_BOUNCE --pool_dir C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --train_start 2026-05-25 --train_end 2026-06-05 --test_start 2026-06-08 --test_end 2026-06-12 --objective band --min_train_trades 6 --max_mask_terms 2 --max_premom_terms 2 --fine_quantiles --force_premom --train_pf_min 1.20 --train_pf_max 2.00 --min_test_trades 3 --no_fdr
python -m py_compile Train_and_Test\setup_looping_results\check_B_HUGE_RED_FAILED_BOUNCE_slippage.py Train_and_Test\setup_looping_results\run_B_HUGE_RED_FAILED_BOUNCE_loop.py Train_and_Test\setup_train_test.py
python Train_and_Test\setup_looping_results\check_B_HUGE_RED_FAILED_BOUNCE_slippage.py
```

## Live/Backtest Mismatch Found

A_PULLBACK_C2_THEN_BREAK_C2_LOW:

- Conf/research config: raw detection only, no `mask_terms`, no pre-momentum gate, exit 1.20/1.50.
- v11 live overlay path: admits this setup through `ab_filtered_relaxed_mask` using `market_abs_ret_pct <= AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT` and then an A/B quality-top-slot gate; source doc notes exit differs.
- User override on 2026-06-29 removed this setup from `_LIVE_SURVIVAL_DEMOTION_2026_06_29` and set the conf gate to `quality_score >= 123.7606` plus `sig5_adx_calc >= 21.4683`.

E_VWAP_LOSE_EARLY_SHORT:

- Source doc previously labeled it active/strongest; this rerun corrected the E card to rejected/parked. Current `final_setup_conf.py` demotes it through `_LIVE_SURVIVAL_DEMOTION_2026_06_29`.
- Conf/research config: `vol_ratio >= 1.8`, `vol_ratio <= 3.2`, no pre-momentum, guard `min_slot=09:45`, exit 0.70/1.00.
- v11 backtest/live overlay path: `vwap_dist_atr >= -1.25` via `MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR`, with no volume-band edge.
- Entry engine default still contains the old E pre-momentum gate (`sig5_vol_ratio20 >= 1.5643`, `pre3_body_sum_r <= 0.797498`) unless final-conf bootstrap overrides it.
- Audit result: keep parked. Thin split baseline failed (TRAIN 19/PF 0.353, TEST 10/PF 0.643) and the official TRAIN-optimized candidate collapsed on TEST (10/PF 0.068). Requested six-week rerun also failed: baseline TRAIN 54/PF 0.362 and TEST 10/PF 0.643; hand variants never reached TRAIN PF 1.0 with enough trades; official max-PF overfit TRAIN 25/PF 1.876 collapsed to TEST 11/PF 0.129.

B_HUGE_RED_FAILED_BOUNCE:

- Backtest/live logic check: no known overlay mismatch under final-conf mode; B is a final-conf/bootstrap readmit setup, and the bootstrap installs the conf pre-momentum terms plus exit levels.
- Current conf: no mask, pre-momentum `pre3_close_pos <= 0.581797`, `sig5_rsi_dir <= 64.104659`, `pre5_mom_r <= 0.284145`, exit 0.90/1.25.
- Rolling split baseline at 15 bps failed: TRAIN 10/PF 0.647 and TEST 5/PF 0.873.
- Slippage check: same conf at 5 bps was TRAIN 10/PF 1.747 and TEST 5/PF 1.294, still below the preferred 1.3 bar and too thin to accept.
- Official tuner overfit check: maxpf TRAIN 15/PF 3.494 collapsed to TEST 3/PF 0.447; band TRAIN 27/PF 1.440 collapsed to TEST 9/PF 0.140.
- Audit result: no config change; reject for sizing/robust acceptance.

## Next Setup To Process

`D_EMA20_REJECTION` is next in the source doc order.

---

# Active-book audit (split-faithful track)

> **Note:** two audit tracks ran concurrently on 2026-06-29 (parallel workers). The track above processed the
> doc top-down using a thin split (TRAIN 05-25..06-05 / TEST 06-08..06-12). This track audits the **active book
> first** (the 4 mined shorts that are the only members of `FINAL_SETUP_CONF` after the 2026-06-29 demotion) on a
> split more faithful to the stated policy: **TRAIN = 2026-04-13..2026-05-25 (6 full weeks), TEST =
> 2026-05-26..2026-06-24 (latest ~4 weeks of available data).** Tooling: `split_pool_by_setup.py` +
> `setup_loop_runner.py` (single-load, reuses the tuner pipeline). Per-setup detail files:
> `<SETUP>_baseline.md`, `_experiment_log.md`, `_best_config.md`, `_final_summary.md`.

### Cross-cutting finding — slippage is decisive for tight-target scalps
The tuner default is **15 bps/leg** (30 bps round-trip) for realistic small-cap fills; the live *paper* model is
**~5 bps entry**. For the 1.0–1.25%-target shorts this gap flips the verdict. Every setup here is reported at
**both 5 and 15 bps/leg**.

### Setup 1 — B_HUGE_RED_FAILED_BOUNCE (SHORT, active): **REJECT for sizing**
- Backtest==live verified (bootstrap-only faithful port; no overlay contradiction).
- Conf gate net of cost: @15 bps TRAIN 0.79 / TEST 0.52 (loser); @5 bps TRAIN 1.63 / TEST 1.10 (TEST < 1.3 bar
  and one day = 362% of TEST net). Longer Feb–May train @15 bps still 0.78 (decay is real, not thin-sample).
- maxpf search: TRAIN 2.31 but TEST p=0.52 (FDR-dropped, overfit). band search: DROP_NO_EDGE.
- **Basis discrepancy (live/backtest mismatch):** unified-pool *readmit-raw* gives original-window TRAIN n=75/PF
  1.28 — NOT the conf's published n=30/PF 2.90. The published 2.90/3.49 is **not reproducible** on the
  "live-faithful" pool. Decay concentrated in mid–late June.
- **Action:** no config change (changes require `--approve` + sign-off). Recommend parking/demoting it like the
  others — it fails its own re-promotion trigger. Flagged for review.

### Setup 2 — C_OR_BREAKDOWN (SHORT, active): **REJECT for sizing**
- Backtest==live verified (bootstrap-only faithful port).
- Conf gate (0.9/2.0): @15 bps TRAIN 0.64 / TEST 0.62 (loser); @5 bps TRAIN 0.94 / TEST 1.12 (one day = 185% net).
- **Loser at realistic 15 bps/leg across every config tried** (12 hand iterations + maxpf@15bps test p 0.69 +
  band@5bps test p 0.12). The wide 2% target hits only ~10%; 53% EOD.
- **Research lead (WATCH, not traded):** replacing the ADX gate with a **deep RS-weakness mask `rs_pct ≤ -4.92`**
  gives a well-distributed TEST 1.33 (n=76, dbp 0.20) @5 bps — but TRAIN only breakeven (1.00) and a loser at
  realistic cost. Needs more data + realistic-cost profitability before trusting.
- **Tooling caveat:** tuner search PF 1.49 (n=169) overstates the deployable book PF 1.00 (n=121 after dedupe +
  overlay) — confirm leads in the full-pipeline runner.
- **Action:** no config change. Recommend parking/demoting (fails re-promotion trigger at realistic cost).

### Setup 3 — A_MOD_BREAK_C1_LOW (SHORT, active): **REJECT for sizing**
- Backtest==live verified (conf path; overlay uses a different gate but is suppressed under the conf flag).
- Conf gate (1.10/1.00, asymmetric high-win scalp): @15 bps TRAIN 0.62 / TEST 0.57 (loser); @5 bps TRAIN **1.23**
  (well-distributed) / TEST 1.06 (one day = 243% net).
- **Best of the four shorts at paper cost** (cleanest TRAIN) — but a loser at realistic 15 bps/leg across all 12
  iterations, and **TEST never clears 1.3** at any slippage. TRAIN-boosting variants (vol band 1.48, mom-tight
  1.44) all **degrade TEST** → overfit. maxpf @15 bps capstone: TRAIN 1.43 but TEST p=0.974 (FDR-dropped).
- The pre-momentum gate is load-bearing (mask-only collapses to 0.29/0.53).
- **Action:** no config change. Recommend parking/demoting; first to re-check if the book is revisited at low fills.

### Active-book audit queue (this track)
1. B_HUGE_RED_FAILED_BOUNCE — **done → REJECT for sizing**
2. C_OR_BREAKDOWN — **done → REJECT for sizing** (rs-weakness lead flagged WATCH)
3. A_MOD_BREAK_C1_LOW — **done → REJECT for sizing** (best-of-4 at paper cost)
4. G_LOWER_LOW_BREAK — **done → REJECT for sizing** (strongest WATCH lead: loosened i03)
(then the parked longs L_DOUBLE_BOTTOM_VWAP / L_PRESSURE_BURST_VWAP, then native parked setups.)

### Setup 4 — G_LOWER_LOW_BREAK (SHORT, active): **REJECT for sizing — strongest WATCH lead**
- Conf mask (vol≥4.13 & q≥76) is **too selective**: only n=6/6 on the fresh window → uncertifiable.
- **Loosening to `vol_ratio≥3 & quality_score≥50` (keep rsi_dir≥68.7, exit 1.1/1.0)** gives the one config in the
  whole audit that PASSES the bar at paper cost: TRAIN 3.07 / TEST 1.40 (n=31/25, well-distributed, train dbp 0.021).
- At realistic 15 bps/leg it slips to TEST 0.93 (loser); paper-cost TEST significance is weak (dbp 0.28).
- **But** vol≥3 = volume-climax bars (most liquid) → real fills plausibly beat the 15 bps stress; the verdict hinges
  on measured fills. The rsi_dir gate is load-bearing; wide targets collapse (quick-exhaustion fade).
- **Action:** no config change. **Forward paper-trade i03 and measure climax-bar slippage**; re-promote only if
  fills ≤ ~8 bps/leg and the edge holds.

### Active-book audit COMPLETE (all 4 currently-tradeable shorts)
All four active mined shorts are **REJECT for sizing**: losers at realistic (15 bps/leg) cost on the fresh window,
with published conf PFs (2.5–9.1) **not reproducible** on the current unified-pool readmit basis, and TRAIN-boosting
filters that consistently degrade TEST (overfit). This is consistent with the §6 live paper collapse (PF 0.25) — the
survival book's "4 corrected-VWAP mined shorts" thesis is **not holding up on fresh, realistically-costed data**, a
systemic finding rather than per-setup flukes.

**Two leads worth a forward paper-watch (NOT sizing):**
1. **G_LOWER_LOW_BREAK i03** (`vol≥3 & q≥50`) — strongest; passes bar at paper cost; climax-bar fills favourable.
2. **C_OR_BREAKDOWN `rs_pct≤-4.92`** (deep relative weakness instead of the ADX gate) — well-distributed TEST 1.33
   @5 bps, breakeven TRAIN.

**Cross-cutting recommendations:**
- The whole short book's deployability hinges on **execution slippage**. Instrument the live executor to record
  realised per-leg fill slippage on these setups; the backtest verdict flips entirely between 5 and 15 bps/leg.
- Investigate the **readmit-basis discrepancy**: the unified pool's "readmit = live-faithful" rows are ~2.5× the
  clean-pool mine the conf was tuned on and reproduce much lower PFs — the live basis for these setups is ambiguous.
- This track made **no `final_setup_conf.py` changes** (read-only on the config to avoid colliding with the parallel
  worker, and because none would pass `--approve`). Recommend the four active shorts be parked/demoted unless and
  until the leads above clear a realistic-cost re-validation + a live-paper holdout.

### Setup 5 — L_DOUBLE_BOTTOM_VWAP (parked LONG, readmit): **REJECT / keep parked**
- Conf gate: @15 bps TRAIN 0.88 / TEST **0.29**; @5 bps TRAIN 1.30 / TEST **0.48**. TEST collapse driven by a
  **60–79% SL rate** — double-bottom-reclaim LONGS walled by stops in the late-May/June test regime.
- All 12 iterations × 2 slippages collapse on TEST (best TEST 0.92 at the widest 1.1% SL). TRAIN-positive variants
  (alt G-gate 1.60, rs-strong 1.55 @5 bps) **all** collapse on TEST → overfit/adverse-regime divergence.
- Not a cost story (losses are SL hits). Doc's RAW-pool 2.55/3.57 not reproduced; live research layer still blocks L*.
- **Action:** no config change; keep parked.

### Setup 6 — L_PRESSURE_BURST_VWAP (parked LONG, readmit): **REJECT / keep parked (no lead)**
- Conf gate (quality≤25 + pre1_adx≥44, 0.7/1.25): @15 bps TRAIN 0.51 / TEST 0.39; @5 bps 0.84 / 0.79 — loser both.
- **Clearest reject of the audit:** loser at every gate/exit/slippage on a LARGE sample (n=138/67). The gate barely
  beats the ungated firehose (0.51 vs 0.32) → almost no edge. Loosening/flipping all worse; pre1_adx non-monotonic.
- Vindicates the doc's USER_APPROVED_OVERRIDE_WEAK flag. **Action:** no config change; keep parked; no WATCH lead.

### ALL LOOP-FAITHFUL SETUPS COMPLETE (6 of 6: 4 active shorts + 2 parked longs)
Every setup for which the fast pool harness is **live-faithful** (the readmit-basis setups) has now been audited at
~20–26 evaluations each, at both 5 and 15 bps/leg. **Result: 6/6 REJECT for sizing.** Two paper-forward WATCH leads
only: G_LOWER_LOW_BREAK i03 (`vol≥3 & q≥50`) and C_OR_BREAKDOWN `rs_pct≤-4.92`. The entire current
`FINAL_SETUP_CONF` (the 4 active mined shorts) is **not holding up on fresh, realistically-costed data** — recommend
parking/demoting all four pending realistic-cost re-validation + a live-paper holdout.

### Remaining (native parked setups — NOT loop-faithful on this pool)
D_EMA20_REJECTION, B_AVWAP_RECLAIM_REVERSAL, B_HUGE_C1_CLOSE_RECLAIM_BREAK, G_HIGHER_HIGH_BREAK (A_PULLBACK +
E_VWAP_LOSE_EARLY_SHORT already done by the other track). These are **native** setups: in the unified pool they are a
raw firehose (live filters through v8/research first), so the fast harness is NOT live-representative for them. A
faithful audit needs the **v11 conf backtest** (`--selected_strategy_profile final_setup_conf`), which is too slow to
loop in-process — recommend running it per setup rather than via the firehose pool. Per-setup pools are split if a
pessimistic-firehose read is wanted.
