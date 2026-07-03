# Optuna code-loop LEADERBOARD

Engine: `Train_and_Test/optuna_setup_loop.py` (Optuna TPE over the repo's `setup_train_test.eval_family` pipeline).
Window: TRAIN 2026-04-13..2026-05-25, TEST 2026-05-26..2026-06-24. Net of v6 cost. 300 trials/setup. Objective =
`min(trPF, tePF) − 0.5·|gap|` judged at **15 bps/leg** (deployable); 5 bps/leg = paper ceiling. Gate: PF≥1.30,
n_train≥20, n_test≥8, day&trade dominance≤0.40, trades/day≤6, test day_block_p≤0.10 — BOTH windows.

> Kept separate from `LEADERBOARD.md` (which the parallel manual-audit track maintains in a different schema) to
> avoid mutual clobbering. Per-setup artifacts live in `Train_and_Test/results/<setup>/`.

| Setup | Side | Faithfulness | Verdict | @15bps TRAIN PF/n | @15bps TEST PF/n | @5bps TRAIN/TEST PF | Notes |
|---|---|---|---|---:|---:|---|---|
| E_VWAP_LOSE_EARLY_SHORT | SHORT | native → screening-only | **NOT SELECTED** | 0.749 / 22 | 0.724 / 24 | 1.22 / 0.98 | loser @15bps; firehose screen |
| D_EMA20_REJECTION | SHORT | native → screening-only | **WATCH→FORCE-PROMOTED (user)** | 1.416 / 20 | 1.695 / 9 | 2.41 / 2.31 | PFs>1.3 but fails robustness (day/trade dom>0.40, p=0.20, n=9); force-promoted at user direction 2026-06-29 |
| B_HUGE_RED_FAILED_BOUNCE | SHORT | **readmit → live-faithful** | **NOT SELECTED** (authoritative) | 0.575 / 57 | 0.644 / 45 | 0.82 / 1.08 | balanced loser; triangulates with manual + rolling audits |
| C_OR_BREAKDOWN | SHORT | **readmit → live-faithful** | **NOT SELECTED** (authoritative) | 0.843 / 42 | 0.843 / 18 | 1.18 / 1.30 | 5bps TEST touches 1.30 but day_dom 0.73, p 0.28, degenerate upper_wick term |
| B_AVWAP_RECLAIM_REVERSAL | LONG | native → screening-only (firehose) | **NOT SELECTED** | 0.419 / 652 | 0.418 / 390 | 0.60 / 0.62 | deep firehose loser; firehose (652) ≫ conf gated basis (27) |
| B_AVWAP_RECLAIM_REVERSAL | LONG | **gated clean pool, strict** | **INSUFFICIENT_SAMPLE** | 0.237 / 11 | 0.00 / 4 | 0.30 / 0.00 | live-faithful basis: only 11 train / 4 test in window; the few that fired LOST. Conf's 1.45/1.20 was the OLDER window — edge absent now. (results_cleanpool/) |

## Cross-cutting read
- **Live-faithful (readmit) verdicts so far: B_HUGE_RED and C_OR both NOT SELECTED** at realistic cost — consistent
  with the manual split-faithful audits and the live-paper collapse.
- **Native (screening-only) setups** (E_VWAP, D_EMA20, B_AVWAP) are pessimistic firehose reads; the v11 conf
  backtest is the live-faithful arbiter for them. D_EMA20 was force-promoted by user direction despite failing.
- The `min(PF)−λ·|gap|` objective returns the most *balanced* config; when nothing is profitable it surfaces a
  balanced loser, so read the "best config" as "best achievable balance," not a real edge.
- No `final_setup_conf.py` change from the loop except the user-directed D_EMA20 force-promote. No live trades.

## FIT/VAL → TRAIN/TEST protocol run (`optuna_fitval_loop.py`, session-based windows)
Inner-optimize on FIT+VAL only (score = min(FIT_PF,VAL_PF) − 0.5·|gap|), confirm best on full TRAIN and TEST once.
Gate: PF≥1.30, train_n≥15, test_n≥5, gross-profit/day/symbol dominance≤0.40, trades/day≤6. Optuna TPE (seed 7).

| Setup | Basis | Windows | Verdict | Baseline TRAIN/TEST PF | Best TRAIN PF/n | Best TEST PF/n | Best @5bps TR/TE |
|---|---|---|---|---|---|---|---|
| B_AVWAP_RECLAIM_REVERSAL | native firehose (screening-only) | FIT 05-29..06-04 / VAL 06-05..06-11 / TEST 06-12..06-24 | **NOT SELECTED** | 0.28 / 0.46 | 0.564 / 81 | 0.722 / 25 | 0.71 / 0.93 |
| B_AVWAP_RECLAIM_REVERSAL *(STRICT: tpd≤3, 3 mask terms)* | native firehose (screening-only) | same | **NOT SELECTED** | 0.28 / 0.46 | 0.554 / 18 (2.25/day) | 0.365 / 12 (3.0/day) | 0.72 / 0.50 | (results_strict/) |

**Strict re-run finding (user: "too many selections"):** capping selectivity to ≤3 trades/day cut TRAIN from ~9/day to
2.25/day and TEST to 3.0/day — but TEST PF got *worse* (0.72→0.37). So over-selection was a symptom, not the cause:
B_AVWAP is a genuine loser on the fresh window at every selectivity level. Trade-dominance is clean (0.11–0.25) →
not a concentration artifact. Keep parked.

- Best FIT/VAL config (SL/Tgt 1.1/1.0; mask `quality_score≥89.68`; premom `pre3_close_pos≥0.786 & sig5_vol_ratio20≤4.42`;
  guard min 10:30/max 14:00; daily_loss 4000) lifts TRAIN PF 0.28→0.56 vs the card baseline but stays a **loser at
  realistic cost on TEST** (0.72). Trade-dominance is fine (0.03–0.09) — the failure is pure PF, not concentration.
- Triangulation for B_AVWAP across all attempts: 2-window firehose **NOT SELECTED**, gated-clean strict
  **INSUFFICIENT_SAMPLE**, FIT/VAL firehose **NOT SELECTED**. The conf's published 1.45/1.20 was the OLDER window;
  the edge is absent on fresh data under every protocol. Keep parked.
