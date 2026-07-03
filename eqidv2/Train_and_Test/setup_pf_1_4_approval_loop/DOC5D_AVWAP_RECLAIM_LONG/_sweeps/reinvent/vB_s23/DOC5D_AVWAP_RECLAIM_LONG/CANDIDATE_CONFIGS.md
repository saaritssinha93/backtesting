# DOC5D_AVWAP_RECLAIM_LONG (LONG) — CANDIDATE_CONFIGS

_Generated 2026-07-01._

**No candidate cleared the robust gate** (TRAIN PF >= 1.30, TEST PF >= 1.40, TEST day-block p <= 0.10, target-fill, neighborhood, dropout, meaningful trades, and concentration checks).

Verdict: **REJECT**

Reject reasons: TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)
