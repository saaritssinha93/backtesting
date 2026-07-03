# DOC5C reinvention — RESULT (5 bps/leg)

_Generated 2026-07-01. Research-only. NO `final_setup_conf.py` edits, NO live trades._

**Goal:** TRAIN PF ∈ [1.30,1.70] (not higher — overshoot = overfit) AND **TEST PF > 1.40**,
meaningful trades, no single trade/day/symbol dominates, simple/structural/tradeable logic,
**evaluated at 5 bps/leg**.

**Verdict: NOT ACHIEVED — approval recommendation NO.** The reinvention made real, honest
progress (a genuine in-sample edge the original chase never had) but the edge does **not**
survive the mandated June-22→30 holdout. Details below.

---

## 1. Why reinvent, and how

Original `DOC5C_ORB_GAP_GO_LONG` buys the ORB gap-breakout **continuation** → on a 5-min
next-open fill you enter ~5 min into an extended move that reverts (raw TRAIN PF 0.20 / TEST 0.14;
see `PARAMETER_SWEEP_SUMMARY.md`). Reinvention kept the **controlled-gap DNA** but changed the
**trigger so the fill lands at value, not extended**. Three new detectors
(`scripts/scan_doc5c_reinvent.py` → `reinvent_pool/`, 861 candidates over the F&O universe):

| # | new setup | idea |
|---|---|---|
| R1 | **DOC5C_GAP_RETEST_HOLD_LONG** | gap-up that broke the ORH earlier, pulls back to **retest** the ORH and **holds** above it (buy the hold, not the break) |
| R2 | DOC5C_GAP_RECLAIM_LONG | gap-up that faded under VWAP (shakeout) then **reclaims** VWAP on an up-bar |
| R3 | DOC5C_GAP_PULLBACK_HOLD_LONG | gap-up in an EMA-stacked uptrend that pulls back to VWAP/EMA20 and turns up |

New structural columns emitted for masking: `gap_pct`, `orh_dist_atr`, **`retest_depth_atr`**
(how deep the pullback tagged below the ORH, in ATR), `vwap_slope_atr`, `adx`, `rsi`,
`ema20_slope_3bar`, `stock_ret_pct`.

**Split (mandated, printed by the tools):** FIT 2026-05-19..06-04 (8), VAL 2026-06-09..06-19 (8),
TRAIN 2026-05-19..06-19 (16), **TEST 2026-06-22..06-30 (6)**. All at **5 bps/leg**, statutory NSE
cost, next-1-min-open fill, via `setup_train_test.eval_family` (single-source pipeline).

## 2. Screen @5bps — RETEST_HOLD is the strongest of the three

| variant | raw TRAIN PF | raw TEST PF | best-exit TRAIN | best-exit TEST |
|---|---:|---:|---:|---:|
| **DOC5C_GAP_RETEST_HOLD_LONG** | **0.432** | 0.238 | 0.788 (1.2/0.6) | **0.332** |
| DOC5C_GAP_RECLAIM_LONG | 0.613 | 0.261 | 0.898 (1.5/0.8) | 0.206 |
| DOC5C_GAP_PULLBACK_HOLD_LONG | 0.479 | 0.128 | — | — |

All three raw-improve on the original (0.20) — the enter-at-value thesis is correct — but every
one's raw/best-exit **TEST** is a deep loser. RETEST_HOLD carries the most trades and the best
TEST, so it got the full staged search.

## 3. The genuine edge: `retest_depth_atr`

Full staged sweep on RETEST_HOLD (`scripts/reinvent_sweep_5bps.py --mode full`) surfaced the
first **stable FIT/VAL pocket** in the entire DOC5C effort:

| filter (ref exit 1.2/0.6) | FIT n/PF | VAL n/PF |
|---|---|---|
| **retest_depth_atr ≥ 0.5** | 16 / **1.348** | 23 / **1.130** |
| retest_depth_atr ≥ 0.25 | 34 / 1.228 | 36 / 1.030 |
| vwap_dist_atr ≤ 2.5 | 44 / 1.017 | 47 / 1.096 |

Structurally sensible: a **deeper** retest (price pulled ≥0.5 ATR below the ORH before holding)
is a *real* test of the breakout level, not a shallow wobble. On full TRAIN this edge is strong
and clean — win 70–81%, target-fill 55–70%, **day-block p ≈ 0.00**, non-dominated (day-share
0.2–0.4) — but at the scalpy 1.2/0.6 exit it overshoots to **PF 2.3–5.2 (> 1.70 = overfit)**.

## 4. Dial into the anti-overfit band, then read TEST once

`scripts/confirm_retest_edge_5bps.py` keeps the FIT/VAL-validated edge and sweeps ONLY the exit
(+ one simple structural companion) to bring **full-TRAIN PF into [1.30,1.70]** (selection on
TRAIN band membership + FIT/VAL positivity — never on TEST), then scores TEST once. **6 configs
landed in the band. All 6 fail TEST:**

| config | TRAIN n/PF | FIT/VAL PF | TEST n/PF | TEST net | pass? |
|---|---|---|---|---:|---|
| retest≥0.5 & vwap_dist≤2.5, exit **1.0/1.0** | 33 / 1.488 | 1.96 / 1.31 | 12 / **0.808** | −915 | ✗ |
| retest≥0.5 & vwap_dist≤2.5, exit 1.2/0.8 | 33 / 1.503 | 1.91 / 1.34 | 12 / 0.646 | −1,820 | ✗ |
| retest≥0.4 & vwap_dist≤2.5, exit 1.5/1.0 | 41 / 1.581 | 1.45 / 1.67 | 14 / 0.608 | −2,483 | ✗ |
| retest≥0.4 & vwap_dist≤2.5, exit 1.0/1.0 | 41 / 1.317 | 1.28 / 1.34 | 14 / 0.593 | −2,647 | ✗ |
| retest≥0.4, exit 1.2/0.6 | 48 / 1.362 | 2.05 / 1.04 | 18 / 0.512 | −3,705 | ✗ |
| retest≥0.5, exit 1.5/1.0 | 39 / 1.502 | 1.34 / 1.61 | 16 / 0.481 | −4,335 | ✗ |

**Best OOS = PF 0.808** (net −Rs 915) — far short of 1.40, still net-negative.

## 5. Diagnosis & recommendation

- The edge is **real in-sample but regime-bound**: it works FIT (May 19–Jun 4) AND VAL
  (Jun 9–19), so it is not a single-window fluke — yet the **true holdout (Jun 22–30)** is a
  losing window for controlled-gap longs (raw TEST 0.24 across the whole variant). Late-June
  simply did not pay gap-up continuation/hold longs.
- 5 bps vs 15 bps barely moved the result — the failure is **directional (the OOS window has no
  gap-long edge), not cost.** Tightening to hit TEST > 1.40 would require fitting to the 12–18
  TEST trades, which the protocol forbids.
- **Approval: NO.** Do not promote any reinvented variant. `final_setup_conf.py` untouched.

**Re-validation trigger (if revisited later):** the `retest_depth_atr ≥ 0.4–0.5` +
`vwap_dist_atr ≤ 2.5` edge on `DOC5C_GAP_RETEST_HOLD_LONG` should be re-scored on a FRESH forward
holdout (new sessions after 2026-06-30). Promote only if that forward window shows TEST PF ≥ 1.40,
n ≥ 15, day-block p ≤ 0.10, non-dominated — i.e. the in-sample edge must reappear out of sample
at least once before sizing.

## 5b. Wide-split re-test (TRAIN Apr 1–May 29 / TEST Jun 2–30, 5 bps) — 0 PASS

Re-run on the user's larger split (`scripts/search_aprmay_jun_5bps.py`): TRAIN = Apr+May
(31 sessions; FIT = April 15 sess, VAL = May 16 sess), TEST = all of June (16 sessions).

| variant | raw TRAIN PF (n) | raw TEST PF (n) | FIT/VAL-stable terms | in-band TRAIN cfgs | PASS |
|---|---:|---:|---:|---:|---:|
| DOC5C_GAP_RETEST_HOLD_LONG | 0.648 (204) | 0.433 (85) | **0** | 0 | 0 |
| DOC5C_GAP_RECLAIM_LONG | 0.569 (147) | 0.625 (56) | **0** | 0 | 0 |
| DOC5C_GAP_PULLBACK_HOLD_LONG | 0.594 (243) | 0.386 (91) | — | — | 0 |

**Decisive:** with FIT = April and VAL = May, **no knob — including `retest_depth_atr` — holds
PF ≥ 1.0 in both halves**. The `retest_depth` edge that reached the band on the narrow May–June
split **does not exist in April** → it was **period-specific, not structural**. No robust in-band
TRAIN book can be built, so TEST > 1.40 is unreachable (and full-June TEST is a raw loser, 0.39–0.63).
Exit grids top out at min(FIT,VAL) PF ≈ 0.73 (RETEST) / 0.61 (RECLAIM). **0 pass candidates.**

**Combined conclusion (both splits):** the narrow split produced an in-band TRAIN config that
failed TEST (best OOS 0.81); the wide split can't even build an in-band TRAIN config because the
edge doesn't generalize to April. DOC5C — original and all three reinvented variants — has **no
long edge that satisfies TRAIN [1.30,1.70] + TEST > 1.40 at 5 bps** on either split without
curve-fitting to the holdout. **Approval: NO.**

## 6. Reproduce

```
# 1. build the reinvented pool (861 candidates)
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5C_ORB_GAP_GO_LONG\scripts\scan_doc5c_reinvent.py --start 2026-04-01 --end 2026-06-30
# 2. screen the three variants @5bps
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5C_ORB_GAP_GO_LONG\scripts\reinvent_sweep_5bps.py --mode screen
# 3. full staged search on the strongest variant
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5C_ORB_GAP_GO_LONG\scripts\reinvent_sweep_5bps.py --mode full --setup DOC5C_GAP_RETEST_HOLD_LONG
# 4. dial the edge into the TRAIN band + read TEST once
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5C_ORB_GAP_GO_LONG\scripts\confirm_retest_edge_5bps.py
```

> **DO NOT MOVE ANYTHING TO FINAL CONFIG UNTIL USER APPROVES.**
