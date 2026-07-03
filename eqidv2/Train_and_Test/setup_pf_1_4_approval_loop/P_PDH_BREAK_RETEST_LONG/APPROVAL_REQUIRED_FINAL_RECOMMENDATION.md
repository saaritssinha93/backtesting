# P_PDH_BREAK_RETEST_LONG — FINAL RECOMMENDATION

## Recommendation: **NO** — do not promote. Keep `enabled=False` (demoted).

There is **no candidate** that meets the bar (TRAIN PF ∈ [1.30,1.70], TEST PF > 1.40, meaningful & non-dominated). The setup is **clearly dead** on the available data and agrees with its live-paper failure.

## Best "candidate"
**None.** The best near-miss (`wick_skew≥−0.053 & close_loc≥0.993`, SL/Tgt 1.00/1.50) has TRAIN PF **0.96** (a loser) and a TEST PF of 1.37 driven entirely by **one day** (top-day net share 2.0×). It is not tradeable and is **not** proposed.

## Proposed config block
**None proposed.** No block should be added to or changed in the conf.

## File that would need approval before any edit
- `final_setup_conf.py` (root) and its mirror `Train_and_Test/final_setup_conf.py`.
- **No edit is requested.** P_PDH_BREAK_RETEST_LONG remains in the demotion block (`enabled=False`, `_LIVE_SURVIVAL_DEMOTION_2026_06_29` / RESEARCH_WATCH). Leave it there.

> ⚠️ **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES** — and in this case there is nothing to move: no candidate passed. Promotion is **not** recommended.

## Re-run commands
Baseline + full search (rebuild the pool slice once, then run):
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/build_pool.py
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/optimize_ppdh.py --trials 400 --time_budget_min 16 --test_n 9 --train_n 27
```
Diagnostics / focused structural sweep:
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/analyze.py
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/iterate2.py
```

## Remaining risks / caveats
1. **Window mismatch:** the requested TEST (2026-06-20+) does not exist in any P_PDH pool (demoted ⇒ not scanned; the unified pool ends 2026-05-29). Used nearest-available TRAIN 2026-04-01..05-15 / TEST 2026-05-18..05-29. The conclusion is reinforced — not weakened — by the live June paper (PF 0.25), which is the actual June out-of-sample.
2. **Small TEST window (9 sessions):** any positive-TEST config is one-day fragile (dominance > 1). If P_PDH is ever to be reconsidered, regenerate a fresh **live-gated** June+ pool (re-enable the scanner for P_PDH) and require: test PF ≥ 1.30, test_n ≥ 20, day_block_p ≤ 0.10, no day/symbol share > 0.45, then a live-paper holdout PF ≥ 1.20 — the standard re-promotion trigger.
3. **Cost sensitivity:** results use 15 bps/leg. At 5 bps paper the picture improves only marginally (the edge problem is directional, not just cost) — it does not change the verdict.
4. No live trades, no live execution, no conf edits were made during this work.
