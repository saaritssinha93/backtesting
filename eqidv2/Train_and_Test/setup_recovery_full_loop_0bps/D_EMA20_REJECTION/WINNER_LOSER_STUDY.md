# D_EMA20_REJECTION Winner / Loser Study

Baseline and serious candidates were checked for win rate, exit mix, day/symbol spread, and dominance.

## Baseline

- TEST: n=20 PF=0.585 net=-2430.37
- Win rate: 40.0%; exits target/SL/EOD: 5.0 / 30.0 / 65.0%
- Days/symbols: 9 / 20; max trades/day: 4
- Dominance: top trade gross share 0.3509, top day net share None, top symbol net share None

## Selected/Closest Candidate

- TEST: n=43 PF=0.7389 net=-4705.65
- Win rate: 46.51%; exits target/SL/EOD: 2.33 / 27.91 / 69.77%
- Days/symbols: 18 / 42; max trades/day: 5
- Dominance: top trade gross share 0.1809, top day net share None, top symbol net share None

Interpretation: a candidate is rejected when profit is dominated by a single trade, day, or symbol even if headline PF is high.
