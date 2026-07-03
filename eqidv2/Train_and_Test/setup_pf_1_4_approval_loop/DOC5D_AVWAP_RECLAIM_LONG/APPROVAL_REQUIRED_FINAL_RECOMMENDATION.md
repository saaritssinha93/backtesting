# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01. Research-only._

## Approval recommendation: **NO**

Two full tracks were run against the goal (TRAIN PF ∈ [1.30,1.70] AND TEST PF > 1.40, meaningful
trades, no single trade/day/symbol dominating, simple & robust):

1. **Original DOC5D pool @ 15 bps/leg** — 5 Optuna studies, 2,500 trials. Best full-TRAIN config
   PF **0.767** (n=26), TEST **0.00**. **REJECT.**
2. **REINVENTED detector @ 5 bps/leg** (per user directive) — redesigned "confirmed VWAP reclaim +
   momentum + leadership + regime" detector, 4 graded variants (vA–vD) over the full ~1,287-name
   universe, 7+ Optuna studies. Base win-rate lifted 21% → ~40–47%, but PF stays a loser.
   **REJECT.**

## The wall (why NO)

- No config satisfies **both** gates at once. The best-found near-misses each fail the *opposite* gate:

| Near-miss (@5 bps) | TRAIN n / PF | TEST n / PF | Fails |
|---|---:|---:|---|
| vA + `sig5_adx_calc≤20.87`, SL1.0/T2.5, min_slot11:00, top_n3 | 95 / 0.99 | 30 / **1.45** | TRAIN is a **net loser** |
| vB + `vwap_dist_atr≥1.028`, SL0.7/T2.5, min_slot10:00, top_n2 | 29 / **1.33** | 9 / 0.54 | TEST **collapses** (winner = 83% of TEST gross) |

- TRAIN (2026-05-18…06-19) and TEST (2026-06-20…06-30) are **anti-correlated** for this long
  archetype — a regime shift, not a tunable edge. Not a cost artifact: the reinvented gate is already
  at **5 bps**.
- Accepting the vA-s23 near-miss because its *TEST* prints 1.45 would be TEST-fitting a config whose
  TRAIN is a loser — explicitly disallowed by the anti-overfit rule and the "simple/robust,
  not-one-day-dominated" acceptance criteria.

## No promotion proposed

- **No edit to `final_setup_conf.py` (or `Train_and_Test/final_setup_conf.py`) is recommended.**
- The two near-miss configs are recorded under `candidates/*_NEAR_MISS_*.json` **for the record only**,
  each labelled `NOT_PASSING — DO NOT PROMOTE`.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## If the user still wants to force one live (against this recommendation)

The least-bad, most-defensible option is the vB-s23 config (TRAIN in-band 1.33, simple 1-mask), but it
is an OOS loser and must first pass a fresh live-gated rolling re-test (TEST PF ≥ 1.30, ≥ 20 trades,
day-block p ≤ 0.10) before any sizing — which on current data it does **not**.

## Rerun commands

```
# 15-bps original-pool track (baseline + search)
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5D_AVWAP_RECLAIM_LONG\scripts\doc5d_baseline_and_split.py
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test\doc5_long_setups\pool --train_start 2026-05-18 --test_start 2026-06-20 --trials 500 --seed 7 --test_pf_min 1.40

# reinvented detector @5bps (scan once, then loop per variant)
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5D_AVWAP_RECLAIM_LONG\scripts\reinvent_doc5d_scan.py --start 2026-05-01 --end 2026-06-30 --out Train_and_Test\setup_pf_1_4_approval_loop\DOC5D_AVWAP_RECLAIM_LONG\pool_reinvent
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5D_AVWAP_RECLAIM_LONG\scripts\screen_variants.py
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test\setup_pf_1_4_approval_loop\DOC5D_AVWAP_RECLAIM_LONG\pool_vB --train_start 2026-05-18 --test_start 2026-06-20 --trials 450 --seed 23 --test_pf_min 1.40 --search_slippage_bps 5 --max_mask_terms 1 --max_pm_terms 1
```

## Remaining risks / caveats

- OOS sample is thin (TEST = 5–6 sessions); but the failure is consistent across variants and seeds,
  so it is a signal, not just low power.
- RS is the repo `rs_pct` proxy (not the doc's cross-sectional percentile) and breadth is a
  NIFTYBEES-VWAP regime proxy; a true `rs_rank`+`breadth` two-pass scan is the one untested lever,
  but with every variant net-negative OOS the prior is poor.
- These are screening-only research pools; any live use would require a v11 conf-backtest cross-check.
