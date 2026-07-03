# D_EMA20_REJECTION Winner / Loser Study

Baseline and serious candidates were checked for win rate, exit mix, day/symbol spread, and dominance.

## Baseline

- TEST: n=20 PF=0.3719 net=-4792.93
- Win rate: 30.0%; exits target/SL/EOD: 5.0 / 35.0 / 60.0%
- Days/symbols: 9 / 20; max trades/day: 4
- Dominance: top trade gross share 0.4061, top day net share None, top symbol net share None

## Selected/Closest Candidate

TEST was not run for this candidate because full TRAIN was outside the PF band.

Interpretation: a candidate is rejected when profit is dominated by a single trade, day, or symbol even if headline PF is high.
