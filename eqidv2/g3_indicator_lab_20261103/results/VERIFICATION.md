# Independent numerical verification

Status: **PASS**. No strategy, frozen input, replay output, or report was modified by this audit.

Verified 63 represented sessions (2026-07-29 through 2026-11-02), 24 primary variants and four fill-stress scenarios.

The combined books contain **13,783 selected-order rows** (12,809 rule/stress; 974 models), not unique market trades. There are 12,077 executed rows across mutually exclusive experiments and 2,512 distinct underlying candidate IDs. These counts must not be interpreted as one portfolio.

Checks completed:

- Fresh original-control selection and saved-ledger parity passed on all 63 days.
- All six frozen computational hashes, every raw/minute input hash, partition configuration hash, and all four consumed supplemental-slot hashes match.
- 4,788 day/variant/cost rows form the exact grid: 24 primary variants at 5/9/15 bps plus four stress variants at 5 bps. 47,880 daily numeric comparisons passed.
- No duplicate (variant, sid) or (variant, day, setup, stock) order keys; merged ledgers match their day artifacts.
- Independently recomputed 720 summary rows (11,520 metric checks), daily comparisons, and all deltas.
- G3, every experiment, and the separate G7 reference share complete 63-day calendars; no missing references were silently filled with zero.
- Model training precedes evaluation in all 9 frozen folds; 43 evaluation sessions. Prior-data thresholds, finite ready scores, control fallback, same-pool comparison, and overall predictive diagnostics reconcile.

## Apparent improvements and concentration

| Variant | Delta at 5 bps | Delta at 9 bps | Delta at 15 bps | October delta | Delta without best day | Delta without best trade contribution |
|---|---:|---:|---:|---:|---:|---:|
| or_market_strict | 7,815.72 | 1,215.72 | -8,684.28 | 13,308.47 | -4,534.28 | -6,234.28 |
| all51_slots | 240,681.95 | 201,081.95 | 141,681.95 | -49,697.79 | 205,103.74 | 216,181.95 |

Amounts are rupees relative to control. Strict OR's small aggregate improvement disappears at 15 bps and after removal of its largest positive day/trade contribution. All51 has broader historical gains and survives these sensitivity checks, but deteriorates in October and on November 2. Neither result establishes a production improvement.

## Limits

- This validates internal consistency and reproduction, not future profitability or data truth at the exchange.
- All history through October 27 reused policy-development outcomes; the four later sessions were previously inspected. No untouched strategy holdout exists.
- Opening OI/universe evidence is inherited; October 28 and November 2 use restricted cohorts. Represented sessions are not every exchange date.
- Intrabar order flow, executable spreads, market impact, and timestamp-complete historical news are unobserved. Stop-first OHLC and conservative timing remain assumptions.
- The within-day drawdown is measured at minute closes, not tick extrema; the daily drawdown is a different series.
- Models are trained chronologically, but their base slot policy was selected retrospectively. Predictions are ranking scores, not validated live win probabilities.
- Largest-contribution removal is attribution sensitivity only, not a rerun with changed capital availability.
- Comparing 28 scenarios and parameter neighborhoods on reused history creates selection bias; bootstrap intervals do not remove it.

Exact hashes, counts, per-day parity, month/cost changes, and largest contributions are in [VERIFICATION.json](VERIFICATION.json).
