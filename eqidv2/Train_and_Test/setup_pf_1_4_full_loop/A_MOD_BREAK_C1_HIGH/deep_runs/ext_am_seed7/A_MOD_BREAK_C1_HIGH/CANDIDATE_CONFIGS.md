# A_MOD_BREAK_C1_HIGH (LONG) — CANDIDATE_CONFIGS

_Generated 2026-07-03._

**No candidate cleared the robust gate** (TRAIN PF >= 1.30, TEST PF >= 1.40, TEST day-block p <= 0.10, target-fill, neighborhood, dropout, meaningful trades, and concentration checks).

Verdict: **REJECT**

Reject reasons: TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)
