# DOC5B_MOMO_BREAKOUT_LONG (LONG) — CANDIDATE_CONFIGS

_Generated 2026-07-01._

**No candidate cleared the robust gate** (TRAIN PF >= 1.30, TEST PF >= 1.40, TEST day-block p <= 0.10, target-fill, neighborhood, dropout, meaningful trades, and concentration checks).

Verdict: **REJECT**

Reject reasons: TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day

## Expanded Search Audit

- 1x1 search: 200 FIT/VAL trials, best TRAIN PF 0.488 / TEST PF 0.211.
- 2x2 search: 600 FIT/VAL trials, best TRAIN PF 0.763 / TEST PF 0.662.
- Full TRAIN rescore across both logs: 453 unique configs.
- Meaningful train-band configs found: 0.
- Best meaningful TRAIN rescore (`n >= 20`): PF 0.952 over 32 trades, still negative.
- Thin train-band pockets existed only at 7-11 TRAIN trades; not approved.

## RS/Breadth V2 Audit

- Rebuilt detector pool with real cross-sectional `rs_rank` and breadth features.
- V2 pool rows: 353.
- V2 search: 800 FIT/VAL trials, up to 2 mask and 2 pre-momentum terms.
- Best selected v2 config: TRAIN PF 0.653 / TEST PF 0.329.
- V2 rescore: 736 unique tried configs; zero meaningful train-band configs with `n >= 20`.
- Exploratory near-miss: TRAIN PF 1.636 over 18 trades, but TEST PF 0.000 over 5 trades.

No candidate is acceptable for approval.

## Retest V3 Audit

- Rebuilt detector as first controlled retest/hold after a strong breakout.
- V3 pool rows: 74.
- V3 search: 700 FIT/VAL trials, up to 2 mask and 2 pre-momentum terms.
- Best selected v3 config: TRAIN PF 1.816 over 12 trades, TEST PF 0.000 over 4 trades.
- Strict rescore: 444 unique tried configs; zero meaningful train-band configs with `n >= 20`.
- Exploratory near-miss: TRAIN PF 1.303 over 12 trades, but TEST PF 0.000 over 4 trades.

Still no candidate is acceptable for approval.
