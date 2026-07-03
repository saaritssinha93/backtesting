# A_MOD_BREAK_C1_LOW (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Approval recommendation: **NO**

## No promotion proposed

- No config cleared the robust TRAIN+TEST gate on the recreated Mar-Jul pool. The existing conf entry stays as-is; nothing is edited.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun commands

```
# pool recreation (fresh-scan segment already on disk; see pools/_fresh_scan.log)
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\recreate_pool.py
# baseline + sweeps + search + finalists + rescue
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\run_full_loop.py --trials 500 --time_budget_min 60 --seed 7
# reports
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\write_reports.py
```

## Remaining risks

- TEST = 20 June/July sessions; June was a poor month for many of this book's setups — a single-month OOS is still one market regime.
- RAW-pool basis: live fires this setup through the conf gate, but v8/research-layer differences remain a live-parity risk (watch live/paper before sizing).
- Domination caps used: trade<=35% gross, day<=40% net, symbol<=40% net.
- No trailing/break-even exits: resolver supports fixed SL/TGT + EOD only.

## PHASE 2 addendum (2026-07-03) — recommendation remains **NO**

- Combined campaign: ~2,841 iterations (phase 1: 767 base-feature; phase 2: 846 feature scans + 1200 TPE trials + confirmations).
- The full indicator space (RSI/ADX/EMA/MACD/BB/Stoch/CCI/MFI/OBV/vol-z/VWAP-context/day-context/C1-geometry/momentum/streak/pressure/compression) contains NO slice where A_MOD_BREAK_C1_LOW is net-profitable at 15 bps/leg + statutory costs on Mar-Jun 2026.
- Every TRAIN-band config found is day-concentrated noise that loses 70-95% of risked capital OOS. Promoting any of them would be exactly the fake-overfit failure mode this campaign was designed to prevent.
- Suggested user decision (NOT executed): demote/disable the live conf entry for this setup — its production config lost on both TRAIN (PF 0.54) and TEST (PF 0.34) on the recreated pool.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Phase-2 rerun commands

```
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\enrich_features.py
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\run_enriched_loop.py --trials 1200 --time_budget_min 60 --seed 11
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\append_phase2_reports.py
```
