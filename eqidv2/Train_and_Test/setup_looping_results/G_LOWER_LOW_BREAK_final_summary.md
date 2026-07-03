# G_LOWER_LOW_BREAK — Final Summary

**Verdict: REJECT for sizing / keep unsized — BUT this is the book's single strongest WATCH / paper-forward lead.**

## What was done (≈26 evaluations)
- Verified backtest == live (bootstrap-only faithful port). ✔
- Baseline at 5 + 15 bps (found the conf mask is too selective: only n=6/6 on the fresh window — uncertifiable).
- 12 hand iterations × both slippages, centred on loosening the vol/quality mask to gain a tradeable sample,
  plus exit grid, gate drop-out, and rs-weakness; loss analysis.

## Key numbers

| Config | 5 bps TRAIN/TEST (n) | 15 bps TRAIN/TEST (n) |
|---|---|---|
| conf gate (vol≥4.13 & q≥76) | 10.27 / 1.27 (6/6) | 3.42 / 0.75 (6/6) — **n=6, noise** |
| **i03: vol≥3 & q≥50 (best lead)** | **3.07 / 1.40 (31/25)** | 1.66 / 0.93 (31/25) |

Conf published claim: TRAIN 2.25 / TEST 9.12 (n=9) — the test 9.12 was always a tiny-sample artifact.

## Why REJECT for sizing
- Conf gate is uncertifiable on the fresh window (n=6/6).
- The best loosened config (i03) **passes the full bar at paper cost** (TRAIN 3.07 / TEST 1.40, well-distributed,
  train dbp 0.021) but **fails at realistic 15 bps/leg** (TEST 0.93) and its paper-cost TEST significance is weak
  (dbp 0.28, n=25). Not yet a sizing candidate.

## Why it's nonetheless the strongest lead
- It is the **only config across all four active shorts** that clears TRAIN≫1.2 AND TEST≥1.3 with a healthy,
  well-distributed, non-one-day sample (at paper cost).
- The slippage assumption is favourable for this setup specifically: **vol_ratio≥3 = volume-climax bars = the most
  liquid moments**, so real fills plausibly beat the 15 bps generic small-cap stress. The realistic verdict hinges
  on measured fills, not on the conservative default.
- The `sig5_rsi_dir≥68.7` pre-entry gate is load-bearing (dropping it → loser); wide targets collapse (it's a
  quick-exhaustion fade, not a runner) — so the mechanism is coherent.

## Action
- **No config change.** Keep unsized.
- **Recommend:** forward paper-trade i03 (`vol_ratio≥3 & quality_score≥50`, rsi_dir≥68.7, exit 1.1/1.0) and
  **measure actual climax-bar slippage**. Re-promote only if fills ≤ ~8 bps/leg and the TEST edge holds on more data.
