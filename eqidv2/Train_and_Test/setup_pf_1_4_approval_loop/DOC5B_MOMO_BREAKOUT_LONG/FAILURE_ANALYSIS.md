# DOC5B_MOMO_BREAKOUT_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01._

- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample.

## Classified failure reasons

- TRAIN PF too low (<1.30)
- TRAIN target-fill rate below 12.0%
- TRAIN concentrated (one trade/day/symbol dominates)
- TRAIN too many trades/day
- neighborhood robustness failed
- term-dropout robustness failed
- TEST PF below 1.40
- TEST day-block p above 0.10
- TEST concentrated (one trade/day/symbol dominates)
- TEST too many trades/day

## Robust gate diagnostics

- neighborhood pass: False
- term-dropout pass: False
- TRAIN target-fill rate: 0.0% (min 12.0%)
- TEST PF/day-block p: 0.211 / 0.9864 (gate 1.4 / 0.1)

## Expanded Search Findings

- A broader 600-trial search with up to 2 mask terms and 2 pre-momentum terms improved the best TRAIN PF only to 0.763 and TEST PF to 0.662.
- Rescoring 453 unique tried configs on full TRAIN found zero configs with both meaningful trades (`n >= 20`) and TRAIN PF in the controlled 1.30-1.70 band.
- The only positive TRAIN pockets were too thin: 7, 8, or 11 trades. Once the TRAIN trade count threshold reached 12, best PF dropped below 1.0.
- This points to weak DOC5B detector edge, not merely a bad SL/target/filter combination.

## RS/Breadth Detector Repair

- Added `scan_doc5b_rs_breadth_v2.py` to build a more faithful pool with real cross-sectional `rs_rank`, breadth above VWAP, breadth positive-return, and breakout-strength features.
- The v2 detector produced a smaller, cleaner pool (353 rows), but the best selected config still failed: TRAIN PF 0.653, TEST PF 0.329.
- V2 rescore found no meaningful TRAIN-band config at `n >= 20`.
- The only 15+ trade train-band near-miss collapsed on TEST: TRAIN PF 1.636 over 18 trades, TEST PF 0.000 over 5 trades.
- Conclusion: the issue persists after correcting RS/breadth; DOC5B momentum breakout remains an unstable breakout-chase pattern in this sample.

## Retest V3 Findings

- Reframed DOC5B from breakout-chase to controlled breakout retest/hold.
- The best selected config found the expected train-side shape (`retest_depth_atr >= 0.35`) but only on 12 TRAIN trades and with PF 1.816, above the preferred anti-overfit band.
- TEST failed completely: PF 0.000 over 4 trades.
- Rescoring all 444 unique v3 tried configs found no meaningful (`n >= 20`) TRAIN-band candidates.
- The retest version helps explain the structure, but not enough to make DOC5B tradeable on this sample.
