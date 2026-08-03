# AVWAP pullback refinement

## Decision: REJECT

The broad AVWAP pullback was not rescued by changing its practical parameter
values. The search did not use TEST for selection.

## Search design

- 3,000 signal configurations
- 21,600 development signal/exit combinations
- Two chronological 20-session development folds
- Separate 10-session validation fold
- TEST evaluated once after the least-bad rule was frozen
- Targets: 0.35%–0.85%
- Stops: 0.45%–0.95%
- Holds: 20–60 minutes
- Varied time window, market regime, liquidity, ATR range, AVWAP extension,
  candle range/wick, pullback type, EMA/AVWAP tolerance, ADX range, RSI range,
  relative volume, confirmation score, and per-slot ranking cap

## Robustness result

- Candidates profitable across both development halves and validation: **0**
- Candidates with PF above 1.05 and positive expectancy in all three selection
  folds: **0**
- Best minimum PF across the three selection folds: **0.970**

The least-bad frozen configuration was:

- Window: 13:00–14:30
- Regime: not bearish
- Pullback: AVWAP touch within 0.25% or EMA9 reclaim
- Minimum traded value: INR 2.5 million
- ATR%: 0.25%–0.55%
- Maximum AVWAP extension: 0.40%
- Relative volume: at least 0.90
- Confirmation score: at least 5
- Target: 0.75%
- Stop: 0.95%
- Hold: 60 minutes

| Fold | Trades | PF | Expectancy | Net P&L |
|---|---:|---:|---:|---:|
| Development A | 216 | 0.970 | INR -7.31 | INR -1,578.45 |
| Development B | 69 | 1.080 | INR 18.89 | INR 1,303.65 |
| Validation | 15 | 0.992 | INR -1.84 | INR -27.60 |
| TEST confirmation | 40 | 0.642 | INR -99.28 | INR -3,970.99 |
| TEST, 150% slippage | 40 | 0.592 | INR -119.04 | INR -4,761.39 |

## Interpretation

This is not a target/stop tuning problem. The entry family lacks a stable
post-cost edge in this period: improved performance in one chronological fold
does not persist into the others. Continuing to tune against TEST until PF
becomes positive would manufacture an overfit result.

Do not wire or trade this AVWAP pullback version. A future attempt should use a
materially different event definition—such as a fresh AVWAP reclaim followed
by a confirmed retest—plus genuinely new out-of-sample sessions.
