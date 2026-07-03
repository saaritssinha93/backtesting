# DOC5C_ORB_GAP_GO_LONG (LONG) — CANDIDATE_CONFIGS

_Generated 2026-07-01._

**No candidate cleared the robust gate** (TRAIN PF >= 1.30, TEST PF >= 1.40, TEST day-block p <= 0.10, target-fill, neighborhood, dropout, meaningful trades, and concentration checks).

Verdict: **REJECT**

Reject reasons: TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 10.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

---

## Gap-knob sweep confirmation (`scripts/gap_knob_sweep.py`)

The custom staged sweep — which additionally searches the gap-and-go structural columns
`gap_pct`, `orh_dist_atr`, `vwap_slope_atr` — **also produced ZERO band-eligible candidates**.
Best full-TRAIN PF found anywhere = **0.683** (mask `vwap_slope_atr≥0.5 & vol_ratio≥2.5`,
pre-mom `pre3_range_r≥0.3`, exit 1.20/2.00; n=15, net −Rs 3,188). Every one of the top-12
FIT/VAL combinations confirmed as a net-negative loser on full TRAIN.

**`candidates/` is intentionally empty** — no config satisfies TRAIN PF ∈ [1.30,1.70] with
TEST PF > 1.40, so writing a candidate JSON would misrepresent the result.
