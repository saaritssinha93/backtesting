# DOC5B_MOMO_BREAKOUT_LONG (LONG) — CANDIDATE_CONFIGS

_Generated 2026-07-01._

**No candidate cleared the robust gate** (TRAIN PF >= 1.30, TEST PF >= 1.40, TEST day-block p <= 0.10, target-fill, neighborhood, dropout, meaningful trades, and concentration checks).

Verdict: **REJECT**

Reject reasons: TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6); TRAIN PF above preferred band (>1.70)
