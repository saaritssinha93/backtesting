# D_EMA20_REJECTION Winner / Loser Study

Baseline and serious candidates were checked for win rate, exit mix, day/symbol spread, and dominance.

## Baseline

- TEST: n=20 PF=0.585 net=-2430.37
- Win rate: 40.0%; exits target/SL/EOD: 5.0 / 30.0 / 65.0%
- Days/symbols: 9 / 20; max trades/day: 4
- Dominance: top trade gross share 0.3509, top day net share None, top symbol net share None

## Selected/Closest Candidate

- TEST: n=9 PF=1.0312 net=138.88
- Win rate: 44.44%; exits target/SL/EOD: 0.0 / 44.44 / 55.56%
- Days/symbols: 6 / 9; max trades/day: 2
- Dominance: top trade gross share 0.4208, top day net share 27.4304, top symbol net share 13.909

Interpretation: a candidate is rejected when profit is dominated by a single trade, day, or symbol even if headline PF is high.
