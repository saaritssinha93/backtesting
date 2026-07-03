# C_OR_BREAKDOWN (SHORT) — APPROVAL_REQUIRED_FINAL_RECOMMENDATION

_Generated 2026-07-03. Research-only campaign under `Train_and_Test/setup_recovery_full_loop/C_OR_BREAKDOWN/`.
No live trades. final_setup_conf.py (root + mirror) untouched._

## Approval recommendation: **NO — REJECT (structural, high confidence)**

## 1. Why the previous approach failed

The 2026-06-13 promotion (train PF 2.78 / test 5.26, n=29/19) was measured on a **sampled**
pool (1300tr/1300te) with a 6-decimal 2-term ADX knife-edge. On the honest full-pool basis
(this campaign: every raw detection 03-02..07-02, full pipeline, 15 bps + statutory costs)
the same config is a plain loser: **TRAIN PF 0.633 (−Rs43.6k, 213 trades), TEST PF 0.278
(−Rs27.0k, 66 trades, 0/66 target fills in June)**. The edge never existed at production scale —
it was sampling + threshold overfit, already flagged as the conf16 "train hero → test loser".

## 2-4. Pool recreation, sessions, baseline

- Pool recreated successfully: 12,219 raw rows, 73 sessions **2026-03-02..2026-07-02**
  (requested 03-01/05-30 + 06-01/07-02; weekend/holiday adjusted; 05-28 + 06-26 unrecoverable
  store holes; 06-11 + 07-01 genuine detector silence). See POOL_RECREATION_REPORT.md.
- TRAIN = 52 sessions (03-02..05-29), FIT = 31, VAL = 21, TEST = 20 (06-01..07-02).
- Baseline (raw detector, 0.90/2.00): TRAIN n=1,721 PF **0.282** net **−Rs843k**;
  TEST n=671 PF **0.297** net **−Rs298k**. 33 trades/day of pure churn; 921 SL vs 95 targets.

## 5-6. Best candidate found

**None passed.** 1,805 logged iterations (13 redesign versions × one-knob sweeps ×
400-trial Optuna TPE combos × 230-combo rescue round). Best results:

| config (FIT/VAL-stable) | TRAIN | TEST | fate |
|---|---|---|---|
| conf-gate + sig_range≤q25 + body≥q50 + pre3_range≤q25, 0.9/1.5 | 24 tr / PF 2.07 / +Rs6.9k, doms OK, dbp 0.06 | **10 tr / PF 0.007 / −Rs6.4k (diagnostic)** | reject: n<35, then TEST collapse |
| conf-gate + body≥q50 + volcap + pre3_range≤q50, 1.1/2.0 | 35 tr / PF 1.45 / +Rs6.0k | not scored | reject: day_dom 0.58 |
| conf-gate + body≥q50 + fresh≤3 + rsi_dir≤q75, 0.9/2.0 | 52 tr / PF 1.32 / +Rs6.4k | not scored | reject: day_dom 0.55+ |

TEST PF never crossed 1.40 — no config even earned a protocol TEST evaluation; the one
diagnostic evaluation of the closest near-miss collapsed to PF 0.007.

## 7-12. Final logic / values

Not applicable — no candidate is proposed. The recommended config of record remains
**"do not trade C_OR_BREAKDOWN"**; the existing FINAL_SETUP_CONF entry is a
**demote-candidate** (its gate re-tested here at TRAIN 0.63 / TEST 0.28), but per campaign
rules no conf edit was made — that is a separate user decision.

## 13. Domination check

The only in-band-PF TRAIN pockets were (a) tiny-n (≤25) 5-term stacks or (b) books where a
single day carried 55-60% of net (cap 40%). Nothing passed all robustness gates together.

## 14-15. Artifacts

- Candidate configs: **none** (`candidates/` empty). Iteration evidence: `iteration_log.csv`
  (1,805 rows), ITERATION_LOG.md, PARAMETER_SWEEP_SUMMARY.md, REDESIGNED_SETUP_IDEAS.md.
- No file awaits approval.

## 16. Approval: **NO**

## 17. Remaining risks / why the setup has no real edge

1. **The detection is structurally late and redundant**: the catalog scan starts ~11:00 IST
   (OR formed 09:15-09:45) and re-fires every bar below the OR low — by entry time the move
   is hours old; median signal sits 4.3 ATR below VWAP with close_loc 0.13 (selling exhaustion,
   not initiation). MAE/MFE shows losers are immediately underwater (median MAE 1.74%) while
   the 0.90 stop is run over 10× more often than the 2.00 target fills.
2. Every pre-entry feature quartile of the raw book is PF ≤ 0.5; the only "edges" found were
   intersections so narrow they are indistinguishable from noise (and the best one proved it
   by collapsing 2.07 → 0.007 out-of-sample).
3. This is the 10th+ setup on this raw basis to fail the same band (A_MOD×3, B-family×5,
   P_PDH, DOC5×...) — the raw-candidate hose + costs at 5-min next-open fills has no
   recoverable short-continuation edge in this regime window.

## 18. Rerun commands

```powershell
# pool (master + reused gapfills + fresh 07-02 gen)
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\build_pool_generic.py --setup C_OR_BREAKDOWN --out Train_and_Test\setup_recovery_full_loop\C_OR_BREAKDOWN\pools\pool_full
# precompute derived matrices (incremental)
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\precompute.py --setup C_OR_BREAKDOWN
# stages
py -3.12 Train_and_Test\setup_recovery_full_loop\C_OR_BREAKDOWN\scripts\run_baseline.py
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\failure_study.py --setup C_OR_BREAKDOWN --sl 0.90 --tgt 2.00
py -3.12 Train_and_Test\setup_recovery_full_loop\C_OR_BREAKDOWN\scripts\full_loop.py
py -3.12 Train_and_Test\setup_recovery_full_loop\C_OR_BREAKDOWN\scripts\rescue_round2.py
py -3.12 Train_and_Test\setup_recovery_full_loop\_shared\write_reports.py --setup C_OR_BREAKDOWN
```

## 19. Closest robust candidate

`conf-gate (sig5_adx≥39.67 & pre1_adx≤21.37) + signal_range_pct≤0.271 + body_pct≥0.722 +
pre3_range_r≤0.190, SL 0.90 / Tgt 1.50` — TRAIN 24/PF 2.07/+Rs6.9k with clean doms and
day-block p 0.06, i.e. the best the setup can look — and it still scored TEST 10/PF 0.007.
A 5-term stack yielding 0.46 trades/day and −Rs6.4k OOS is not a tradeable edge; it is the
overfit ceiling of a dead base population.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES** (nothing to move — verdict REJECT).
