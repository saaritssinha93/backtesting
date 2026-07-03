# A_MOD_BREAK_C1_LOW (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION (recovery)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Approval recommendation: **NO**

## No promotion proposed

- Neither the original detection, nor 6 redesigns of it, nor exits derived from measured 1-min MFE/MAE produce a config passing the robust TRAIN+TEST gate on Mar-Jun 2026.
- Combined with the prior campaign (~2,841 iterations over 47 features), the totality of evidence says the A_MOD_BREAK_C1_LOW intent has NO tradeable edge at 15 bps/leg + statutory costs in this period: the median trade's best-case 1-min excursion (0.472%) is barely above the ~0.30% cost stack and adverse excursion (0.823%) is nearly twice as large.
- Standing suggestion (user decision, NOT executed): demote/disable the live conf entry.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun commands

```
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\recreate_pool.py
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\mfe_mae_study.py
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\redesign_scan.py
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\run_recovery_loop.py --trials_per_variant 150 --minutes_per_variant 8 --seed 21
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\write_recovery_reports.py
```

## Remaining risks / caveats

- Redesigned pools are research detections; live use would need a flag-gated detector wired into the production scanner (S9/DOC5D pattern) plus parity checks.
- Tractability caps (deepest-per-ticker-day + seeded sample) are documented; full-universe reruns are possible but change nothing qualitatively (sampling is unbiased).
- One-month TEST (June) is a single regime; a July re-run is the cheapest next validation.