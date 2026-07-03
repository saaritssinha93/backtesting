# B_HUGE_FAILED_BOUNCE (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Approval recommendation: **NO**

## No promotion proposed

- No config cleared the robust TRAIN+TEST gate on the recreated Mar-Jul pool. The existing conf entry stays as-is; nothing is edited.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun commands

```
# pool recreation (fresh-scan segment already on disk; see pools/_fresh_scan.log)
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\scripts\recreate_pool.py
# baseline + sweeps + search + finalists + rescue
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\scripts\run_full_loop.py --trials 500 --time_budget_min 60 --seed 7
# reports
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\scripts\write_reports.py
```

## Remaining risks

- TEST = 22 June/July sessions; June was a poor month for many of this book's setups — a single-month OOS is still one market regime.
- RAW-pool basis: live fires this setup through the conf gate, but v8/research-layer differences remain a live-parity risk (watch live/paper before sizing).
- Domination caps used: trade<=35% gross, day<=40% net, symbol<=40% net.
- No trailing/break-even exits: resolver supports fixed SL/TGT + EOD only.