# Final three-month LONG strategy result

## Outcome

One configuration is **profitable across TRAIN, VALIDATION, and TEST**, but it
is classified as a **forward-paper candidate**, not a production-ready
strategy, because the final test has only eight trades across three days.

The broad AVWAP pullback and opening-range breakout families have no usable
edge in this period. Their best frozen TEST profit factors were 0.590 and 0.466
respectively. They should not be deployed or optimized further on this sample.

## Profitable research candidate

**Late-session BB Compression Breakout**

- Completed 5-minute signals between 14:00 and 14:29
- Strict BB10 compression profile, non-bearish market regime
- Confirmation score of at least 6/8
- Entry above signal high during the next three 1-minute candles
- Target: **0.75%**
- Stop-loss: **0.70%**
- Time exit: **60 minutes**, capped at 15:15
- Full costs plus 5 bps adverse entry/non-target-exit slippage

| Split | Sessions | Trades | PF | Expectancy/trade | Net P&L |
|---|---:|---:|---:|---:|---:|
| TRAIN | 40 | 38 | 1.262 | INR 67.48 | INR 2,564.10 |
| VALIDATION | 10 | 12 | 1.263 | INR 44.35 | INR 532.19 |
| TEST | 10 | 8 | 3.163 | INR 226.58 | INR 1,812.63 |
| TEST with 150% exit slippage | 10 | 8 | 2.833 | INR 214.35 | INR 1,714.80 |

All nine neighboring target/stop combinations around the selection were
profitable in each split. This supports a local parameter region, but does not
solve the sample-size problem.

## Why it is not live-ready

- Only 8 untouched TEST trades and 58 trades across all 60 sessions.
- TEST trades occurred on only 3 days.
- The top TEST day supplied 37.7% of positive profit.
- The top TEST ticker supplied 25.1% of positive profit.
- The refinement search examined many TRAIN/VALIDATION rules, increasing the
  chance that a small-sample winner is selection noise.

The setup was subsequently wired into V11 and the V7 paper/live chains at the
user's direction. The correct next action remains forward paper collection
without changing the rules. Require at least 100–300 new trades, positive
post-cost expectancy, PF above 1.4, and materially lower day/ticker
concentration before production sizing.

## Other frozen family results

| Family | Best target | Best stop | TEST trades | TEST PF | TEST expectancy |
|---|---:|---:|---:|---:|---:|
| AVWAP pullback | 0.85% | 0.80% | 839 | 0.590 | INR -119.08 |
| Opening-range breakout | 0.85% | 0.90% | 661 | 0.466 | INR -199.12 |
| Broad compression finalist | 0.75% | 0.80% | 47 | 0.488 | INR -134.11 |

The profitable result comes specifically from the independently selected
late-session compression refinement, not from combining losing families.

## Repository safety

The original research task wrote only inside
`isolated_long_3m_research_20260730`. A later user-directed task wired the
frozen candidate into the existing V7/V11 configuration, scanner, entry, and
paper-execution paths. Unrelated concurrent workspace changes were neither
modified nor reverted.
