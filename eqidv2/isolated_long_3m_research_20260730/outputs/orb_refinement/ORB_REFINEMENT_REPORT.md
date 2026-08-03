# Opening-range breakout refinement

## Decision: REJECT

Changing practical ORB parameters and exits did not produce a stable,
post-cost edge. TEST was not used to select the rule.

## Search design

- 3,000 causal signal configurations
- 35,280 development signal/exit combinations
- Two chronological 20-session development folds
- Separate 10-session validation fold
- OR15, OR20 and OR30 definitions
- Targets: 0.25%–0.85%
- Stops: 0.35%–0.95%
- Holds: 10–60 minutes
- Varied morning window, breakout extension, market regime, liquidity, ATR,
  AVWAP extension, candle range/wick, ADX, RSI, relative volume, close
  location, confirmation score and per-slot ranking cap

## Robustness result

- Candidates profitable across both development halves and validation: **0**
- Candidates with PF above 1.05 and positive expectancy in all three selection
  folds: **0**
- Best minimum PF across the three selection folds: **0.780**

The least-bad frozen configuration was:

- Opening range: 30 minutes
- Entry window: 09:50–10:30
- Market regime: all
- Breakout extension: 0.00%–0.25%
- Minimum traded value: INR 1 million
- ATR%: 0.35%–0.75%
- Maximum candle range: 0.80 ATR
- Close location: at least 0.75
- Relative volume: at least 0.90
- Confirmation score: at least 6
- Top 10 candidates per signal slot
- Target: 0.45%
- Stop: 0.65%
- Hold: 60 minutes

| Fold | Trades | PF | Expectancy | Net P&L |
|---|---:|---:|---:|---:|
| Development A | 49 | 1.002 | INR 0.36 | INR 17.58 |
| Development B | 57 | 0.780 | INR -52.48 | INR -2,991.55 |
| Validation | 30 | 0.792 | INR -53.73 | INR -1,611.77 |
| TEST confirmation | 14 | 0.292 | INR -253.18 | INR -3,544.44 |
| TEST, 150% slippage | 14 | 0.278 | INR -270.83 | INR -3,791.64 |

## Interpretation

The ordinary opening-range breakout is not repairable through parameter or
bracket tuning on this sample. Its apparent edge in isolated periods reverses
chronologically and deteriorates further out of sample.

Do not wire or trade this ORB version. A future ORB experiment would need a
different event definition, such as break–retest–hold or failed-break reclaim,
and genuinely new out-of-sample sessions. Continuing to tune the known TEST
segment would create an overfit strategy.
