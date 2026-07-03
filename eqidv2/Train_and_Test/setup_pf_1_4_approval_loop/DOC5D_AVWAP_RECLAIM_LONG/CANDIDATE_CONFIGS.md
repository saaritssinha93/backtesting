# DOC5D_AVWAP_RECLAIM_LONG — CANDIDATE_CONFIGS

_Generated 2026-07-01. Research-only._

## No candidate cleared the gate (TRAIN PF ∈ [1.30,1.70] AND TEST PF > 1.40, meaningful & non-concentrated & robust).

Two tracks were run:
1. **Original pool @ 15 bps** — 5 Optuna studies (2,500 trials). Best full-TRAIN config PF 0.767
   (n=26), TEST 0.00. REJECT. (BASELINE_RESULT.md, PARAMETER_SWEEP_SUMMARY.md, ITERATION_LOG.md)
2. **Reinvented detector @ 5 bps** — redesigned confirmed-reclaim detector, 4 graded variants over
   the full ~1,287-name universe, 7+ Optuna studies. REJECT. (REINVENTION_RESULTS.md)

### Best-found NEAR-MISSES (each fails the *opposite* gate — recorded, NOT passing, NOT for promotion)

| Candidate | Detector | TRAIN n/PF @5bps | TEST n/PF @5bps | Fails |
|---|---|---:|---:|---|
| `candidates/..._NEAR_MISS_vA_s23.json` | vA + `sig5_adx_calc≤20.87`, SL1.0/T2.5, min_slot 11:00, top_n3 | 95 / 0.99 | 30 / **1.45** | **TRAIN loser** (needs ≥1.30) |
| `candidates/..._NEAR_MISS_vB_s23.json` | vB + `vwap_dist_atr≥1.028`, SL0.7/T2.5, min_slot 10:00, top_n2 | 29 / **1.33** | 9 / 0.54 | **TEST collapse** (needs >1.40) |

Neither is tradeable: the first passes only OOS (TRAIN is a net loser), the second passes only TRAIN
(OOS collapses, and the winning trade is 83% of TEST gross). No config satisfies both gates at once —
TRAIN (05-18…06-19) and TEST (06-20…06-30) are anti-correlated for this long archetype (regime shift).

**Verdict: REJECT. See APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md.**
