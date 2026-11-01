# Ten-setup G3 and G7 V4 lock audit

This consolidated package covers the ten unique requested setup/direction pairs in G3 and G7 V4 two-loss-lock. Evidence ends on 2026-10-27. The 2026-10-28 session is incomplete and is excluded.

All batch manifest hashes and baseline parity checks passed. Final daywise truth comes from the accepted portfolios that already contain the 09:26 SHORT wick>=0.20 change. No active strategy or config file was changed.

## Final decision

No tested rule is promoted. Three G3 variant rows passed the development gates across two setups. The frozen tie-break selected one rule for each setup; both selected rules failed the later chronology. They remain visible as diagnostics and the final locked result stays at accepted baseline for every setup and strategy.

| Setup | Strategy | Development choice | Later result | Final locked |
|---|---|---|---:|---|
| 0931_LONG | G3 | none | no development qualifier | baseline |
| 0931_LONG | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 0931_SHORT | G3 | none | no development qualifier | baseline |
| 0931_SHORT | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1001_LONG | G3 | none | no development qualifier | baseline |
| 1001_LONG | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1051_LONG | G3 | move_magnitude_le_0p75 | 4/6 wins, 66.67% | baseline |
| 1051_LONG | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1111_SHORT | G3 | none | no development qualifier | baseline |
| 1111_SHORT | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1146_SHORT | G3 | extension_le_3p5 | 5/8 wins, 62.50% | baseline |
| 1146_SHORT | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1226_SHORT | G3 | none | no development qualifier | baseline |
| 1226_SHORT | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1231_SHORT | G3 | none | no development qualifier | baseline |
| 1231_SHORT | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1301_SHORT | G3 | none | no development qualifier | baseline |
| 1301_SHORT | G7_V4_LOCK2 | none | no development qualifier | baseline |
| 1321_SHORT | G3 | none | no development qualifier | baseline |
| 1321_SHORT | G7_V4_LOCK2 | none | no development qualifier | baseline |

Development rows that passed all five gates were:

- 1051_LONG G3 `move_magnitude_le_0p75`: 10/13 (76.92%), setup net change Rs 2,900.00; selected by tie-break.
- 1146_SHORT G3 `extension_le_3p5`: 7/9 (77.78%), setup net change Rs 7,970.60; selected by tie-break.
- 1146_SHORT G3 `body_le_0p90`: 7/9 (77.78%), setup net change Rs 817.32; eligible, not selected.

The two tie-break selections were rejected after development:

- 1051_LONG G3 `move_magnitude_le_0p75`: later 4/6 (66.67%), all history 14/19 (73.68%). It failed the later >75% requirement.
- 1146_SHORT G3 `extension_le_3p5`: later 5/8 (62.50%), all history 12/17 (70.59%). It failed the later >75% requirement.

## Accepted baseline at 5 bps

| Setup | Strategy | Overall W-L | Overall win rate | Overall net | October W-L | October win rate | October net |
|---|---|---:|---:|---:|---:|---:|---:|
| 0931_LONG | G3 | 2-2 | 50.00% | Rs 11,781.95 | 0-0 | n/a | Rs 0.00 |
| 0931_LONG | G7_V4_LOCK2 | 2-2 | 50.00% | Rs 2,624.58 | 0-0 | n/a | Rs 0.00 |
| 0931_SHORT | G3 | 10-4 | 71.43% | Rs 30,779.75 | 1-3 | 25.00% | -Rs 14,521.47 |
| 0931_SHORT | G7_V4_LOCK2 | 7-3 | 70.00% | Rs 747.84 | 2-1 | 66.67% | -Rs 1,021.77 |
| 1001_LONG | G3 | 5-4 | 55.56% | Rs 22,510.81 | 1-1 | 50.00% | -Rs 5,223.26 |
| 1001_LONG | G7_V4_LOCK2 | 4-2 | 66.67% | Rs 16,713.61 | 0-1 | 0.00% | -Rs 5,246.96 |
| 1051_LONG | G3 | 15-6 | 71.43% | Rs 30,530.24 | 3-2 | 60.00% | Rs 367.33 |
| 1051_LONG | G7_V4_LOCK2 | 11-4 | 73.33% | Rs 18,944.32 | 2-2 | 50.00% | -Rs 6,371.06 |
| 1111_SHORT | G3 | 6-3 | 66.67% | Rs 22,466.27 | 0-2 | 0.00% | -Rs 3,135.08 |
| 1111_SHORT | G7_V4_LOCK2 | 4-2 | 66.67% | Rs 23,090.90 | 0-1 | 0.00% | -Rs 27.93 |
| 1146_SHORT | G3 | 13-6 | 68.42% | Rs 58,897.99 | 4-2 | 66.67% | Rs 15,166.92 |
| 1146_SHORT | G7_V4_LOCK2 | 9-5 | 64.29% | Rs 19,482.81 | 2-2 | 50.00% | Rs 5,084.07 |
| 1226_SHORT | G3 | 7-4 | 63.64% | Rs 30,288.03 | 1-3 | 25.00% | -Rs 5,919.82 |
| 1226_SHORT | G7_V4_LOCK2 | 5-2 | 71.43% | Rs 15,102.14 | 1-1 | 50.00% | Rs 408.43 |
| 1231_SHORT | G3 | 9-4 | 69.23% | Rs 17,170.28 | 2-3 | 40.00% | -Rs 5,523.24 |
| 1231_SHORT | G7_V4_LOCK2 | 6-3 | 66.67% | Rs 8,842.24 | 2-3 | 40.00% | -Rs 5,354.85 |
| 1301_SHORT | G3 | 18-7 | 72.00% | Rs 21,689.06 | 7-3 | 70.00% | Rs 4,130.13 |
| 1301_SHORT | G7_V4_LOCK2 | 10-4 | 71.43% | Rs 8,575.21 | 4-1 | 80.00% | Rs 2,421.02 |
| 1321_SHORT | G3 | 14-8 | 63.64% | Rs 16,292.64 | 1-6 | 14.29% | -Rs 17,811.50 |
| 1321_SHORT | G7_V4_LOCK2 | 10-6 | 62.50% | Rs 10,308.01 | 1-5 | 16.67% | -Rs 14,543.60 |

## Full-portfolio cost stress

| Strategy | Cost | Overall W-L | Overall win rate | Overall net | October net |
|---|---:|---:|---:|---:|---:|
| G3 | 5 bps | 394-152 | 72.16% | Rs 1,224,672.05 | Rs 114,547.91 |
| G3 | 9 bps | 384-162 | 70.33% | Rs 1,115,472.05 | Rs 84,147.91 |
| G3 | 15 bps | 371-175 | 67.95% | Rs 951,672.05 | Rs 38,547.91 |
| G7_V4_LOCK2 | 5 bps | 320-77 | 80.60% | Rs 859,638.79 | Rs 131,028.36 |
| G7_V4_LOCK2 | 9 bps | 316-81 | 79.60% | Rs 780,478.01 | Rs 109,104.20 |
| G7_V4_LOCK2 | 15 bps | 225-137 | 62.15% | Rs 618,956.30 | Rs 70,043.05 |

The 5, 9, and 15 bps rows rerun cost-dependent G7 loss-lock chronology. Higher costs materially lower both win rate and PnL.

## Evidence and limits

- 108 declared batch files were hash-verified.
- 30 batch baseline-parity checks passed, plus accepted-ledger normalization for all 20 setup/strategy pairs.
- `october_setup_daywise.csv` has all 320 setup/strategy/session rows, including zero-trade sessions. `october_requested_setups_aggregate_daywise.csv` and `october_full_portfolio_daywise.csv` provide both aggregate views.
- Setup-level point win rates are descriptive. No setup-level Wilson 95% lower bound validates a >75% claim, and this repeatedly inspected 59-session history is not an independent holdout.

See `promotion_decisions.csv`, `development_gate_summary.csv`, `final_locked_results.csv`, `all_normalized_variant_results.csv`, and `artifact_manifest.json` for the complete audit.
