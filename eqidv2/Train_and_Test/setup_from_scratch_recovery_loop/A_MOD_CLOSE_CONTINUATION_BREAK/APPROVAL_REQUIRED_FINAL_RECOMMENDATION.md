# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION (recovery loop)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Approval recommendation: **NO**

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES** (nothing to promote)

## Why NO — the from-scratch evidence chain

1. Production pool = collapse residue (96.8% BEAR): two campaigns, 1,673 iterations -> REJECT (documented in setup_pf_1_4_full_loop/).
2. Redesigned uncollapsed pool (42,757 signals, all regimes, morning window restored): baseline TRAIN PF 0.223 / TEST 0.173; EVERY regime slice negative (BULL 0.22, BEAR 0.30, NEUTRAL 0.28, TREND 0.30, all with n >= 448).
3. MFE/MAE on 4,000 1-min paths: median MFE +0.37% vs MAE -1.05%; close-to-EOD median -0.47%. ALL 49 exit brackets physically infeasible — perfect-exit hit-rate ceiling ~= half the win rate needed for PF 1.3.
4. Winner/loser separation: fresh-break, first-break, pullback-then-break, regime, hour — none separates winners from losers (PF 0.19-0.41 everywhere).
5. Redesign packs, single-term sweeps, TPE combinations and the rescue loop (this campaign's ITERATION_LOG) confirmed no stable in-band pocket exists.

## Closest confirmations (full TRAIN)

- SL0.85/T2.5 [x_ema200_dist_atr<=6.126011;x_prev_pullback>=0.5;wick_skew_pct>=0.0] g={'min_slot': '11:00', 'max_slot': '11:30', 'top_n': 1}: REJECT: TRAIN PF outside [1.30,1.80]
  - TRAIN n=43 PF=1.048 net=Rs1,172 win%=41.9 avgW=Rs1,420 avgL=Rs-976 SL/TGT/EOD=21/9/13 tpd=1.43 domT/D/S=0.089/4.268/2.34 dbp=0.4687
  - TEST  (not run)
- SL0.85/T2.5 [x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_adx_slope3<=-1.734748] g={'min_slot': '10:00', 'max_slot': '11:30', 'top_n': 1}: REJECT: TRAIN PF outside [1.30,1.80]
  - TRAIN n=52 PF=1.013 net=Rs385 win%=34.6 avgW=Rs1,729 avgL=Rs-904 SL/TGT/EOD=26/11/15 tpd=2.0 domT/D/S=0.073/17.445/5.886 dbp=0.5157
  - TEST  (not run)
- SL0.85/T2.5 [x_first_break_of_day>=0.5;x_prev_pullback>=0.5;wick_skew_pct>=0.0] g={'min_slot': '11:00', 'max_slot': '11:30', 'top_n': 1}: REJECT: TRAIN PF outside [1.30,1.80]
  - TRAIN n=63 PF=0.921 net=Rs-2,952 win%=38.1 avgW=Rs1,442 avgL=Rs-963 SL/TGT/EOD=33/12/18 tpd=1.57 domT/D/S=0.065/9.99/9.99 dbp=0.6163
  - TEST  (not run)
- SL0.85/T2.5 [x_first_break_of_day>=0.5;x_macd_hist_delta_atr<=0.033329;x_prev_pullback>=0.5] g={'min_slot': '10:00', 'max_slot': '11:30', 'top_n': 1}: REJECT: TRAIN PF outside [1.30,1.80]
  - TRAIN n=57 PF=0.905 net=Rs-3,572 win%=33.3 avgW=Rs1,782 avgL=Rs-985 SL/TGT/EOD=33/14/10 tpd=1.73 domT/D/S=0.067/9.99/9.99 dbp=0.6501
  - TEST  (not run)

## Conclusion on edge

The card's trigger — buying the close-near-high break of the prior 5-min bar after strength — is a systematic LOCAL-EXTREME purchase. Its forward 1-min distribution is negatively skewed in every regime, every hour, and every structural variant; costs are ~27% of the median favorable excursion. **The setup has no real edge at 5-min next-tick granularity in this universe.** Recommend permanently retiring it (keep GATE_BLOCKED / never promote) and spending future iteration budget on setups whose baseline is at least cost-line-adjacent.

## Rerun commands

```
cd <repo root>
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\scan_redesigned_pool.py
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\enrich_pool_features.py --no-premom --pool Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\pools\pool_redesigned --out Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\pools\pool_enriched
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\eval_baseline_recovery.py
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\mfe_mae_study.py
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\winner_loser_study.py
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\campaign_recovery.py --trials 3000 --time_budget_min 45 --seed 17
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\render_recovery_reports.py
```

## Remaining risks / caveats

- Redesigned scan is research-side; live deployment would need a flag-gated detector (S9/DOC5D pattern) and a fresh sign-off run.
- 15 bps/leg slippage assumed; small-caps may be worse.
- TEST excludes 2026-07-02 (truncated 1-min data); 2026-06-26 has no 5-min data.