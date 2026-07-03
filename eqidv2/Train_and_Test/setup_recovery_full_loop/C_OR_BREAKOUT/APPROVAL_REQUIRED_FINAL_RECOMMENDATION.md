# C_OR_BREAKOUT (LONG) — APPROVAL_REQUIRED_FINAL_RECOMMENDATION

_Generated 2026-07-03. Research-only campaign under `Train_and_Test/setup_recovery_full_loop/C_OR_BREAKOUT/`.
No live trades. final_setup_conf.py (root + mirror) untouched._

## Approval recommendation: **NO — REJECT (structural; never reached the TRAIN band)**

## 1. Why the previous approach failed

C_OR_BREAKOUT was never promoted — it sat in the pre-pooled group that went **0/12** in the
2026-07-01 PF-band campaign. Root cause visible in this recreation: it is a breakout-chase
hose (36-52 raw trades/day pool-wide) whose signals fire ≥11:00 IST on a detector that
re-fires every bar above the OR-high; costs at 5-min next-open fills eat whatever
continuation exists (same E_ORB_BREAKOUT churn/cost-sink signature already documented in
RESEARCH_WATCH).

## 2-4. Pool recreation, sessions, baseline

- Pool recreated successfully: 8,414 raw rows, 75 sessions **2026-03-04..2026-07-02**
  (same source stack as C_OR_BREAKDOWN; see POOL_RECREATION_REPORT.md).
- TRAIN 52 sessions (03-04..05-29), FIT 31 / VAL 21, TEST 23 (06-01..07-02).
- Baselines (raw detector): 0.90/2.00 → TRAIN PF **0.358** (−Rs874k); 0.90/1.25 → TRAIN PF
  0.345 (−Rs921k); 0.70/1.00 → TRAIN PF 0.294 (−Rs1.15M). TEST PF 0.27-0.31 at all three.

## 5-6. Best candidate found

**None.** 2,363 logged iterations (13 versions, 1,490 one-knob sweeps, 400 Optuna TPE
combos, 230-combo rescue round). No configuration ever reached TRAIN PF 1.30, so **zero
TEST evaluations were earned** (anti-overfit protocol).

| best of | config | FIT | VAL | TRAIN |
|---|---|---|---|---|
| sweeps | V4_rs_leader + vol_ratio≤1.65 | 34/0.60 | 28/0.59 | — |
| Optuna | fresh-break + bigbar≥1.38 + pre3_range≤1.10, 1.5/2.0, ≥12:30 | 39/0.79 | 19/0.83 | — |
| round 2 | broad-gate + volcap_q25 + rs_q75, 1.1/2.5, top-2 | — | — | 21/1.17/+Rs2.0k |

## 7-12. Final logic / values

Not applicable — no candidate proposed. Setup stays unpromoted (status quo).

## 13. Domination check

Not reached — no config cleared the PF/n gates that precede it.

## 14-16. Artifacts / approval

- `candidates/` empty; evidence in `iteration_log.csv` (2,363 rows) + the stage reports.
- Approval: **NO.**

## 17. Why the setup has no real edge

1. Late, redundant detection (scan starts ~11:00; OR formed by 09:45; re-fires all day).
2. Breakout-chase economics: the failure study shows the same immediate-MAE signature as
   its short twin; VAL (Apr-May chop) is consistently WORSE than FIT across every version —
   the "edge" degrades monotonically toward the present.
3. The best block (big signal bar + quiet pre-bars + first fire) is a momentum-ignition
   archetype the repo has already tested to death (FAST_MOMENTUM_LONG: structurally
   edgeless; DOC5B/DOC5C: dead) — this pool reproduces those verdicts.

## 18. Rerun commands

```powershell
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\build_pool_generic.py --setup C_OR_BREAKOUT --out Train_and_Test\setup_recovery_full_loop\C_OR_BREAKOUT\pools\pool_full
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\precompute.py --setup C_OR_BREAKOUT
py -3.12 Train_and_Test\setup_recovery_full_loop\C_OR_BREAKOUT\scripts\run_baseline.py
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\failure_study.py --setup C_OR_BREAKOUT --sl 0.90 --tgt 1.25
py -3.12 Train_and_Test\setup_recovery_full_loop\C_OR_BREAKOUT\scripts\full_loop.py
py -3.12 Train_and_Test\setup_recovery_full_loop\C_OR_BREAKOUT\scripts\rescue_round2.py
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\write_reports.py --setup C_OR_BREAKOUT
```

## 19. Closest robust candidate

`broad ADX gate (sig5_adx≥30 & pre1_adx≤25) + vol_ratio≤q25 + rs_pct≥q75, SL 1.10 / Tgt 2.50,
top-2 per slot` — TRAIN 21 trades / PF 1.17 / +Rs2,026 over three months. Below the band, a
fifth of the minimum trade count, and economically a rounding error. The base population
(PF 0.29-0.42 at every exit) offers nothing for filters to rescue: the setup as detected has
no real edge at realistic costs.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES** (nothing to move — verdict REJECT).
