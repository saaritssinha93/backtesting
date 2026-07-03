# APPROVAL_REQUIRED — FINAL RECOMMENDATION — B_AVWAP_RECLAIM_REVERSAL (LONG)

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.** This file is a recommendation only. Nothing here has been written to `final_setup_conf.py` or `Train_and_Test/final_setup_conf.py`.

## Verdict: **NO — do not promote**

No config reached full-TRAIN PF in [1.3,1.7] **and** TEST PF > 1.4 with stable, meaningful trade counts. The best in-band TRAIN attempts either failed OOS on TEST or were sample/dominance-fragile (see CANDIDATE_CONFIGS.md / ITERATION_LOG.md).

## If you still want to iterate
- The binding constraint and the closest near-miss are recorded in ITERATION_LOG.md.
- No promotion target file should be edited.

## Commands
```
# baseline replay:
py -3.12 Train_and_Test/setup_loop_runner.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --configs <baseline.json> --train_start 2026-05-18 --train_end <day-before-test> --test_start 2026-06-20 --test_end <latest> --slippage_bps 5.0

# full loop rerun:
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_RECLAIM_REVERSAL/scripts/pf_band_search.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --train_start 2026-05-18 --test_start 2026-06-20 --trials 500 --time_budget_min 25.0 --seed 7 --slippage_bps 5.0
```