# Round2 confirmation-filter causal replay: 1226_SHORT, 1231_SHORT, 1301_SHORT, 1321_SHORT

The frozen round2 grid was applied before native ranking and quota. Replacements were simulated from one-minute paths. All existing G3/G7 gates were preserved, including dated Nifty permission, configured extension/liquidity guards, the V4 low-efficiency veto, +0.6/+0.1 protection, and causal two-loss lock at each stress cost. This round adds 160 comparisons to 220 prior comparisons, cumulative 380.

All history has already been inspected. Later periods are retrospective chronological diagnostics, never an independent holdout. A point win rate above 75% is descriptive when the Wilson lower bound remains below 75%. A development choice is not a promotion.

## Development choices

- 1226_SHORT G3: NO PREDECLARED VARIANT PASSED; retain baseline
- 1226_SHORT G7_V4_LOCK2: NO PREDECLARED VARIANT PASSED; retain baseline
- 1231_SHORT G3: NO PREDECLARED VARIANT PASSED; retain baseline
- 1231_SHORT G7_V4_LOCK2: NO PREDECLARED VARIANT PASSED; retain baseline
- 1301_SHORT G3: NO PREDECLARED VARIANT PASSED; retain baseline
- 1301_SHORT G7_V4_LOCK2: NO PREDECLARED VARIANT PASSED; retain baseline
- 1321_SHORT G3: NO PREDECLARED VARIANT PASSED; retain baseline
- 1321_SHORT G7_V4_LOCK2: NO PREDECLARED VARIANT PASSED; retain baseline

## Shared-rule check

- 1226_SHORT: no same predeclared rule passed the development gates in both strategies
- 1231_SHORT: no same predeclared rule passed the development gates in both strategies
- 1301_SHORT: no same predeclared rule passed the development gates in both strategies
- 1321_SHORT: no same predeclared rule passed the development gates in both strategies

See `development_selection.csv`, `selected_variant_results.csv`, and `variant_setup_metrics.csv` for every result.
