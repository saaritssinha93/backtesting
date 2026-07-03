# P_PDH_BREAK_RETEST_LONG — CANDIDATE_CONFIGS

**Acceptance criteria:** TRAIN PF ∈ [1.30, 1.70] **AND** TEST PF > 1.40 (plus train_n ≥ ~25, test_n ≥ 15, no day/symbol/trade net share > 0.45).

## Result: **0 candidates.**

No configuration out of ~617 evaluated (197 hand + 400 Optuna + 120 confirmed + 20 structural) satisfies the acceptance criteria. The `candidates/` folder therefore contains **no** passing candidate JSONs.

For transparency, the closest near-misses (all **rejected**) and why:

| Config | TRAIN n/PF | TEST n/PF | Why rejected |
|---|---|---|---|
| `lower_wick_pct≥0.007 & vol_ratio≥7.12`, SL/Tgt 1.00/1.50 | 21 / 2.02 | 3 / 0.84 | TRAIN PF > 1.70 (overfit); test_n=3; TEST collapses |
| `wick_skew≥−0.053 & close_loc≥0.993`, SL/Tgt 1.00/1.50 | 25 / 0.96 | 11 / 1.37 | TRAIN PF < 1.30; one test day = all profit (domday 2.0) |
| baseline gate + exit 0.70/2.50 | 20 / 1.28 | 7 / 0.83 | TRAIN PF < 1.30 (and < band); train_n < 25; TEST < 1.40 |
| morning-only (≤11:30) baseline gate, 0.70/2.00 | 10 / 1.32 | 3 / 6.00 | train_n=10; TEST is a single day (n=3) |
| `quality_score ≥ median` + baseline gate, 0.70/2.00 | 9 / 4.22 | 6 / 1.03 | train_n=9 (overfit); one test day dominates |

None is tradeable: each is either below the TRAIN band, above it (overfit), critically under-sampled, or carried by a single day/symbol.

**No candidate is promoted. No candidate file is recommended for `final_setup_conf.py`.**

See `ITERATION_LOG.md` for the full sweep and `APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md` for the final recommendation.
