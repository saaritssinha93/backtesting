# D_AVWAP_LOSE_REVERSAL Winner / Loser Study

Baseline and serious candidates were checked for win rate, exit mix, day/symbol spread, and dominance.

## Baseline

- TEST: n=550 PF=0.9186 net=-17755.33
- Win rate: 47.64%; exits target/SL/EOD: 15.27 / 30.91 / 53.82%
- Days/symbols: 21 / 412; max trades/day: 38
- Dominance: top trade gross share 0.0071, top day net share None, top symbol net share None

## Selected/Closest Candidate

TEST was not run for this candidate because full TRAIN was outside the PF band.

Interpretation: a candidate is rejected when profit is dominated by a single trade, day, or symbol even if headline PF is high.
