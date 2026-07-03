# D_AVWAP_LOSE_REVERSAL Winner / Loser Study

Baseline and serious candidates were checked for win rate, exit mix, day/symbol spread, and dominance.

## Baseline

- TEST: n=542 PF=0.7522 net=-57725.0
- Win rate: 44.83%; exits target/SL/EOD: 13.84 / 30.81 / 55.35%
- Days/symbols: 21 / 406; max trades/day: 38
- Dominance: top trade gross share 0.0078, top day net share None, top symbol net share None

## Selected/Closest Candidate

TEST was not run for this candidate because full TRAIN was outside the PF band.

Interpretation: a candidate is rejected when profit is dominated by a single trade, day, or symbol even if headline PF is high.
