# L_PRESSURE_BURST_VWAP — Experiment Log

Window: TRAIN 2026-04-13..2026-05-25, TEST 2026-05-26..2026-06-24. Net of v6 cost; 5 + 15 bps/leg.
Bar: TRAIN PF>1.2, TEST PF≥1.3, robust at realistic cost. This is the doc's WEAK/USER_APPROVED_OVERRIDE setup.

## Baseline (conf gate: quality_score≤25 + premom pre1_adx≥44, exit 0.70/1.25)
- @15 bps: TRAIN 0.51 (n138) / TEST 0.39 (n67) — heavy loser, LARGE sample.
- @5 bps : TRAIN 0.84 (n138) / TEST 0.79 (n67) — still loser at paper cost.
- Ungated raw: TRAIN 0.32 (n1781) — the gate barely lifts it (0.51), the underlying setup is a structural loser.

## Hand iterations (12 configs × both slippages) — ALL losers

| # | Change group | @5 bps TRAIN/TEST | @15 bps TRAIN/TEST | Note |
|---|---|---|---|---|
| 1 | baseline 0.7/1.25 | 0.84 / 0.79 | 0.51 / 0.39 | least-bad gate |
| 2 | exit 0.7/1.0 | 0.77 / 0.79 | 0.48 / 0.43 | loser |
| 3 | exit 0.9/1.25 | 0.90 / 0.79 | 0.54 / 0.42 | loser |
| 4 | exit 0.9/1.5 | **0.95 / 0.79** | 0.54 / 0.46 | best @5 bps; still both <1 |
| 5 | exit 0.7/1.5 | 0.90 / 0.76 | 0.51 / 0.43 | loser |
| 6 | drop quality mask | 0.61 / 0.55 (n970) | 0.37 / 0.32 | much worse |
| 7 | pre1_adx≥40 | 0.63 / 0.69 | 0.38 / 0.37 | worse |
| 8 | pre1_adx≥48 | 0.86 / 0.61 | 0.51 / 0.31 | loser (non-monotonic adx confirmed) |
| 9 | drop adx gate | 0.58 / 0.54 | 0.35 / 0.28 | worse |
| 10 | quality≥60 (flip) | 0.59 / 0.49 | 0.35 / 0.28 | worse |
| 11 | + vol_ratio≥2 | 0.79 / 0.89 | 0.48 / 0.43 | loser |
| 12 | + rs_pct≥0.5 | 0.41 / 0.63 (n17/9) | 0.28 / 0.31 | worse |

## Verdict
**REJECT / keep parked — the clearest reject of the audit.** Every gate/exit/slippage combination is a loser, on a
LARGE sample (n=138 train / 67 test), so this is a robust structural loser, not small-sample noise. The conf gate
(quality≤25 + pre1_adx≥44) barely beats the ungated firehose (0.51 vs 0.32 train) — it adds almost no edge.
Loosening makes it worse; flipping quality high makes it worse; the non-monotonic pre1_adx is confirmed (≥40 worse,
≥48 worse). This matches the doc's USER_APPROVED_OVERRIDE_WEAK / failed-anti-overfit assessment. No config change.
**Recommendation:** this one should not merely stay parked — it has no defensible edge and should not be
re-promoted under any condition without a fundamentally different thesis.
