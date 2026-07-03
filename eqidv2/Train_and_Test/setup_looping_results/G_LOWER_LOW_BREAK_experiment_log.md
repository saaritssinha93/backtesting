# G_LOWER_LOW_BREAK — Experiment Log

Window: TRAIN 2026-04-13..2026-05-25, TEST 2026-05-26..2026-06-24. Net of v6 cost; reported at 15 bps/leg
(realistic) and 5 bps/leg (paper). Bar: TRAIN PF>1.2, TEST PF≥1.3, non-concentrated, robust at realistic cost.

## Baseline (conf gate: vol_ratio≥4.13 & quality≥76.4 + premom sig5_rsi_dir≥68.7, exit 1.10/1.00)
- **n=6 train / n=6 test** — uncertifiable. @15 bps TRAIN 3.42 / TEST 0.75; @5 bps TRAIN 10.27 / TEST 1.27.
  The mask is too selective for the fresh 10-week window. Key iteration: **loosen vol/quality to gain sample.**

## Hand iterations (12 configs × both slippages)

| # | Change group | @5 bps TRAIN/TEST (n) | @15 bps TRAIN/TEST (n) | Keep? | Reason |
|---|---|---|---|---|---|
| 1 | baseline vol≥4.13 & q≥76 | 10.27 / 1.27 (6/6) | 3.42 / 0.75 (6/6) | reject | n=6 noise |
| 2 | vol≥3.0 & q≥76 | 5.12 / 1.27 (11/6) | 2.58 / 0.75 (11/6) | reject | test still n=6 |
| 3 | **vol≥3.0 & q≥50** | **3.07 / 1.40 (31/25)** dbp 0.02/0.28 | **1.66 / 0.93 (31/25)** | **WATCH (best lead)** | clears bar @5 bps, well-distributed; @15 bps TEST 0.93 loser |
| 4 | exit 0.9/1.0 | 10.27 / 1.48 (6/6) | 1.30 / 0.83 (6/6) | reject | n=6 |
| 5 | exit 1.1/1.5 | 10.72 / 0.38 (6/6) | 3.59 / 0.16 (6/6) | reject | wide tgt → TEST collapse |
| 6 | exit 1.1/2.0 | 10.72 / 0.38 (6/6) | 3.59 / 0.16 (6/6) | reject | same |
| 7 | drop rsi gate (mask only) | 1.22 / 1.22 (25/25) | 0.60 / 0.72 (25/25) | reject | loser @15 bps; @5 bps one-day-ish (top1day 2.0) |
| 8 | + rs_pct≤-2.0 | inf / 0.00 (2/1) | inf / 0.00 (2/1) | reject | sample destroyed |
| 9 | vol≥3 + rs_pct≤-3.0 | 7.10 / — (5/0) | 3.27 / — (5/0) | reject | no test trades |
| 10 | vol≥4.13 only | 1.50 / 0.91 (81/76) | 0.78 / 0.51 (81/76) | reject | TEST <1 both |
| 11 | vol≥3 & q≥50, 0.9/1.5 | 1.48 / 0.99 (105/87) | 0.77 / 0.54 (105/87) | reject | wide tgt worse |
| 12 | vol≥5.0 | 2.56 / 0.71 (14/15) | 1.34 / 0.50 (14/15) | reject | TEST loser |

**Read:** loosening to **vol≥3 & quality≥50 (i03)** is the one change that produces a tradeable, well-distributed
config that PASSES the bar at paper cost (TRAIN 3.07 / TEST 1.40, n=31/25, top1day 0.46/0.90, train dbp 0.021).
But at realistic 15 bps/leg the TEST drops to 0.93 (loser), and the paper-cost TEST significance is weak
(dbp 0.28). Dropping the rsi_dir gate (i07) kills the edge → the gate is load-bearing. Wide targets (i05/i06/i11)
collapse TEST → this is a quick-exhaustion fade, not a runner.

## Verdict
**REJECT for sizing**, but **i03 (vol≥3 & q≥50, rsi_dir≥68.7, exit 1.1/1.0) is the strongest WATCH / paper-forward
lead in the entire active-book audit.** Rationale to keep watching: (a) it passes the full bar at paper cost with a
healthy, well-distributed sample; (b) vol≥3 selects **volume-climax bars that are genuinely liquid**, so real fills
plausibly sit closer to the 5 bps than the 15 bps assumption — the realistic-cost verdict hinges on measured fills.
Recommend forward paper-trading i03 and measuring actual climax-bar slippage before any sizing decision. No config change made.
